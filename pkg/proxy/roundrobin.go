/*
Copyright 2014 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package proxy

import (
	"errors"
	"net"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	"github.com/golang/glog"
)

var (
	ErrMissingServiceEntry = errors.New("missing service entry")
	ErrMissingEndpoints    = errors.New("missing endpoints")
)

type sessionAffinityDetail struct {
	clientIPAddress string
	clientProtocol  api.Protocol //not yet used
	sessionCookie   string       //not yet used
	endpoint        string
	lastUsedDTTM    time.Time
}

type serviceDetail struct {
	name                string
	sessionAffinityType api.AffinityType
	sessionAffinityMap  map[string]sessionAffinityDetail
}

// LoadBalancerRR is a round-robin load balancer.
type LoadBalancerRR struct {
	lock          sync.RWMutex
	endpointsMap  map[string][]string
	rrIndex       map[string]int
	serviceDtlMap map[string]serviceDetail
}

func newServiceDetail(service string, sessionAffinityType api.AffinityType) *serviceDetail {
	return &serviceDetail{
		name:                service,
		sessionAffinityType: sessionAffinityType,
		sessionAffinityMap:  make(map[string]sessionAffinityDetail),
	}
}

// NewLoadBalancerRR returns a new LoadBalancerRR.
func NewLoadBalancerRR() *LoadBalancerRR {
	return &LoadBalancerRR{
		endpointsMap:  make(map[string][]string),
		rrIndex:       make(map[string]int),
		serviceDtlMap: make(map[string]serviceDetail),
	}
}

func (lb *LoadBalancerRR) NewService(service string, sessionAffinityType api.AffinityType) error {
	if _, exists := lb.serviceDtlMap[service]; !exists {
		lb.serviceDtlMap[service] = *newServiceDetail(service, sessionAffinityType)
		glog.V(4).Infof("NewService.  Service does not exist.  So I created it: %+v", lb.serviceDtlMap[service])
	}
	return nil
}

// NextEndpoint returns a service endpoint.
// The service endpoint is chosen using the round-robin algorithm.
func (lb *LoadBalancerRR) NextEndpoint(service string, srcAddr net.Addr) (string, error) {
	var ipaddr string
	glog.V(4).Infof("NextEndpoint.  service: %s.  srcAddr: %+v. Endpoints: %+v", service, srcAddr, lb.endpointsMap)

	lb.lock.RLock()
	serviceDtls, exists := lb.serviceDtlMap[service]
	endpoints, _ := lb.endpointsMap[service]
	index := lb.rrIndex[service]

	lb.lock.RUnlock()
	if !exists {
		return "", ErrMissingServiceEntry
	}
	if len(endpoints) == 0 {
		return "", ErrMissingEndpoints
	}
	if serviceDtls.sessionAffinityType != api.AffinityTypeNone {
		if _, _, err := net.SplitHostPort(srcAddr.String()); err == nil {
			ipaddr, _, _ = net.SplitHostPort(srcAddr.String())
		}
		sessionAffinity, exists := serviceDtls.sessionAffinityMap[ipaddr]
		glog.V(4).Infof("NextEndpoint.  Key: %s. sessionAffinity: %+v\n", ipaddr, sessionAffinity)
		if exists && time.Now().Sub(sessionAffinity.lastUsedDTTM).Minutes() < 30 {
			endpoint := sessionAffinity.endpoint
			sessionAffinity.lastUsedDTTM = time.Now()
			lb.serviceDtlMap[service].sessionAffinityMap[ipaddr] = sessionAffinity
			glog.V(4).Infof("NextEndpoint.  Key: %s. sessionAffinity: %+v", ipaddr, sessionAffinity)
			return endpoint, nil
		}
	}
	endpoint := endpoints[index]
	lb.lock.Lock()
	lb.rrIndex[service] = (index + 1) % len(endpoints)

	if serviceDtls.sessionAffinityType != api.AffinityTypeNone {
		affinity, _ := lb.serviceDtlMap[service].sessionAffinityMap[ipaddr]
		affinity.lastUsedDTTM = time.Now()
		affinity.endpoint = endpoint
		affinity.clientIPAddress = ipaddr

		lb.serviceDtlMap[service].sessionAffinityMap[ipaddr] = affinity
		glog.V(4).Infof("NextEndpoint. New Affinity key %s: %+v", ipaddr, lb.serviceDtlMap[service].sessionAffinityMap[ipaddr])
	}

	lb.lock.Unlock()
	glog.V(4).Infof("NextEndpoint 16. Service Detail Map for %s: %+v", service, lb.serviceDtlMap[service])
	return endpoint, nil
}

func isValidEndpoint(spec string) bool {
	_, port, err := net.SplitHostPort(spec)
	if err != nil {
		return false
	}
	value, err := strconv.Atoi(port)
	if err != nil {
		return false
	}
	return value > 0
}

func filterValidEndpoints(endpoints []string) []string {
	var result []string
	for _, spec := range endpoints {
		if isValidEndpoint(spec) {
			result = append(result, spec)
		}
	}
	return result
}

func removeSessionAffinityByEndpoint(lb *LoadBalancerRR, service string, endpoint string) {
	for _, affinityDetail := range lb.serviceDtlMap[service].sessionAffinityMap {
		if affinityDetail.endpoint == endpoint {
			glog.V(4).Infof("Removing client: %s from sessionAffinityMap for service: %s", affinityDetail.endpoint, service)
			delete(lb.serviceDtlMap[service].sessionAffinityMap, affinityDetail.clientIPAddress)
		}
	}
}

func updateServiceDetailMap(lb *LoadBalancerRR, service string, validEndpoints []string) {
	m := map[string]int{}
	for _, s1Val := range validEndpoints {
		m[s1Val] = 1
	}
	for _, s2Val := range lb.endpointsMap[service] {
		m[s2Val] = m[s2Val] + 1
	}
	for mKey, mVal := range m {
		if mVal == 1 {
			glog.V(3).Infof("Delete endpoint %s for service: %s", mKey, service)
			removeSessionAffinityByEndpoint(lb, service, mKey)
			delete(lb.serviceDtlMap[service].sessionAffinityMap, mKey)
		}
	}
}

// OnUpdate manages the registered service endpoints.
// Registered endpoints are updated if found in the update set or
// unregistered if missing from the update set.
func (lb *LoadBalancerRR) OnUpdate(endpoints []api.Endpoints) {
	registeredEndpoints := make(map[string]bool)
	lb.lock.Lock()
	defer lb.lock.Unlock()
	// Update endpoints for services.
	for _, endpoint := range endpoints {
		existingEndpoints, exists := lb.endpointsMap[endpoint.Name]
		validEndpoints := filterValidEndpoints(endpoint.Endpoints)
		if !exists || !reflect.DeepEqual(existingEndpoints, validEndpoints) {
			glog.V(3).Infof("LoadBalancerRR: Setting endpoints for %s to %+v", endpoint.Name, endpoint.Endpoints)
			updateServiceDetailMap(lb, endpoint.Name, validEndpoints)
			lb.NewService(endpoint.Name, api.AffinityTypeNone) //This is just a catch all in case any existing flows did not create the new service before getting to this point.
			lb.endpointsMap[endpoint.Name] = validEndpoints

			// Reset the round-robin index.
			lb.rrIndex[endpoint.Name] = 0
		}
		registeredEndpoints[endpoint.Name] = true
	}
	// Remove endpoints missing from the update.
	for k, v := range lb.endpointsMap {
		if _, exists := registeredEndpoints[k]; !exists {
			glog.V(3).Infof("LoadBalancerRR: Removing endpoints for %s -> %+v", k, v)
			delete(lb.endpointsMap, k)
			delete(lb.serviceDtlMap, k)
		}
	}
}

func (lb *LoadBalancerRR) CleanupStaleStickySessions(service string, stickyMaxAgeMinutes int) {
	for key, affinityDetail := range lb.serviceDtlMap[service].sessionAffinityMap {
		if int(time.Now().Sub(affinityDetail.lastUsedDTTM).Minutes()) >= stickyMaxAgeMinutes {
			glog.V(4).Infof("Removing client: %s from sessionAffinityMap for service: %s.  Last used is greater than %d minutes....", affinityDetail.clientIPAddress, service, stickyMaxAgeMinutes)
			delete(lb.serviceDtlMap[service].sessionAffinityMap, key)
		}
	}
}
