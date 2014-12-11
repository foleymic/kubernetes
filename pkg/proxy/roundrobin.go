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
	"strings"

	"github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	"github.com/golang/glog"
)

var (
	ErrMissingServiceEntry = errors.New("missing service entry")
	ErrMissingEndpoints    = errors.New("missing endpoints")
)

type sessionAffinityDetail struct{
	ipAddress		string
	sessionCookie	string
	endpoint		string
	lastUsedDTTM	time.Time
}

type serviceDetail struct {
	name						string
	maintainSessionAffinity		bool
	sessionAffinityType			api.AffinityType
	//endpoints					[]string
	sessionAffinityMap			map[string]sessionAffinityDetail
	//reverseSessionAffinityMap	map[string]sessionAffinityDetail
}

// LoadBalancerRR is a round-robin load balancer.
type LoadBalancerRR struct {
	lock         		sync.RWMutex
	endpointsMap 		map[string][]string
	rrIndex      		map[string]int
	serviceDtlMap		map[string]serviceDetail

}


func newServiceDetail(service string, maintainSessionAffinity bool, sessionAffinityType api.AffinityType) *serviceDetail {
	return &serviceDetail{
		name:						service,
		maintainSessionAffinity:	maintainSessionAffinity,
		sessionAffinityType:		sessionAffinityType,
		sessionAffinityMap:			make(map[string]sessionAffinityDetail),
	}
}

// NewLoadBalancerRR returns a new LoadBalancerRR.
func NewLoadBalancerRR() *LoadBalancerRR {
	return &LoadBalancerRR{
		endpointsMap: 		make(map[string][]string),
		rrIndex:      		make(map[string]int),
		serviceDtlMap:		make(map[string]serviceDetail),
	}
}


func getAffinityKey(service string, key net.Addr) string{
	if key!=nil {
		var ipaddr string = strings.Split(key.String(),":")[0]
		return service + ":" + ipaddr
	}else {
		return ""
	}
}

func (lb *LoadBalancerRR) NewService(service string, maintainSessionAffinity bool, sessionAffinityType api.AffinityType) error {
	glog.Infof("NewService 1.  service: %s.  maintainSessionAffinity: %v sessionAffinityType: %+v", service, maintainSessionAffinity, sessionAffinityType)

	if _, exists := lb.serviceDtlMap[service]; !exists {
		glog.Infof("NewService 2.  Service does not exist.  Let's create it.")
		//MJF - temp hack until I figure out the api versions issue.
		if service == "nginx" || service =="tpeservice"{
			maintainSessionAffinity=true
		}
		lb.serviceDtlMap[service] = *newServiceDetail(service, maintainSessionAffinity, sessionAffinityType)
		glog.Infof("NewService 3.  Service map: %+v", lb.serviceDtlMap[service])
	}

	glog.Infof("NewService 4.  Service map: %+v", lb.serviceDtlMap[service])

	return nil
}

// NextEndpoint returns a service endpoint.
// The service endpoint is chosen using the round-robin algorithm.
func (lb *LoadBalancerRR) NextEndpoint(service string, srcAddr net.Addr) (string, error) {
	glog.Infof("********************************************")
	glog.Infof("NextEndpoint 2.  service: %s.  srcAddr: %s ", service, srcAddr)
	glog.Infof("NextEndpoint 3.  Endpoints: %+v", lb.endpointsMap)

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
	var ipaddr string

	if serviceDtls.maintainSessionAffinity{
		if srcAddr != nil {
			ipaddr = strings.Split(srcAddr.String(), ":")[0]
		}
		sessionAffinity, affinityKeyExists := serviceDtls.sessionAffinityMap[ipaddr]
		glog.Infof("NextEndpoint 7.  Key: %s. sessionAffinity: %+v\n", ipaddr, sessionAffinity)
		if affinityKeyExists && time.Now().Sub(sessionAffinity.lastUsedDTTM).Minutes() < 30{
			endpoint := sessionAffinity.endpoint
			sessionAffinity.lastUsedDTTM = time.Now()
			glog.Infof("NextEndpoint 8.  Key: %s. sessionAffinity: %+v", ipaddr, sessionAffinity)
			glog.Infof("********************************************")
			return endpoint, nil
		}
	}


	endpoint := endpoints[index]
	lb.lock.Lock()
	lb.rrIndex[service] = (index+1)%len(endpoints)

	if serviceDtls.maintainSessionAffinity{
		affinity,_ := lb.serviceDtlMap[service].sessionAffinityMap[ipaddr]
		affinity.lastUsedDTTM=time.Now()
		affinity.endpoint = endpoint
		affinity.ipAddress = ipaddr

		lb.serviceDtlMap[service].sessionAffinityMap[ipaddr] =affinity
		glog.Infof("NextEndpoint 15. New Affinity key %s: %+v", ipaddr, lb.serviceDtlMap[service].sessionAffinityMap[ipaddr])
	}

	lb.lock.Unlock()
	glog.Infof("NextEndpoint 16. Service Detail Map for %s: %+v", service, lb.serviceDtlMap[service])
	glog.Infof("********************************************")
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

func removeSessionAffinityByEndpoint(lb *LoadBalancerRR, service string, endpoint string){
	for _, seshAffinDtl := range lb.serviceDtlMap[service].sessionAffinityMap {
		if seshAffinDtl.endpoint == endpoint{
			delete(lb.serviceDtlMap[service].sessionAffinityMap, seshAffinDtl.ipAddress)
		}
	}
}

func updateServiceDetailMap(lb *LoadBalancerRR, service string, validEndpoints []string){
	m :=map [string]int{}
	for _, s1Val := range validEndpoints {
		m[s1Val] = 1
	}
	for _, s2Val := range lb.endpointsMap[service] {
		m[s2Val] = m[s2Val] + 1
	}
	glog.Infof("------------------------")
	glog.Infof("--------m: %+v", m)
	for mKey, mVal := range m{
		if mVal==1{
			glog.Infof("Delete endpoint %s for service: %s", mKey, service)
			removeSessionAffinityByEndpoint(lb, service, mKey)
			delete(lb.serviceDtlMap[service].sessionAffinityMap, mKey)
		}
	}
	glog.Infof("---updateServiceDetailMap--- serviceDetailMap: %+v", lb.serviceDtlMap[service])
	glog.Infof("------------------------")
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
			glog.Infof("Existing Endpoints: %+v", existingEndpoints)
			glog.Infof("valid Endpoints: %+v", validEndpoints)

			glog.Infof("---BEFORE--- serviceDetailMap: %+v", lb.serviceDtlMap[endpoint.Name])
			updateServiceDetailMap(lb, endpoint.Name, validEndpoints)

			lb.NewService(endpoint.Name, false, "")   //this is wrong.............
			glog.Infof("LoadBalancerRR: Setting endpoints for %s to %+v", endpoint.Name, endpoint.Endpoints)
			lb.endpointsMap[endpoint.Name] = validEndpoints
			glog.Infof("---AFTER--- serviceDetailMap: %+v", lb.serviceDtlMap[endpoint.Name])

			// Reset the round-robin index.
			lb.rrIndex[endpoint.Name] = 0
		}
		registeredEndpoints[endpoint.Name] = true
	}
	// Remove endpoints missing from the update.
	for k, v := range lb.endpointsMap {
		if _, exists := registeredEndpoints[k]; !exists {
			glog.Infof("LoadBalancerRR: Removing endpoints for %s -> %+v", k, v)
			delete(lb.endpointsMap, k)
			delete(lb.serviceDtlMap, k)
			glog.Infof("Remaining services: %+v", lb.serviceDtlMap)
		}
	}
}
