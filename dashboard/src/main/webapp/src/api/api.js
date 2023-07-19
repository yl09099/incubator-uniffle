import http from "@/utils/http";

// Create a Coordinator information interface
export function getCoordinatorServerInfo(params){
    return http.get('/coordinator/info', params,{})
}

// Create a coordinator configuration file interface
export function getCoordinatorConf(params){
    return http.get('/coordinator/conf', params,{})
}

// Create an interface for the total number of nodes
export function getShufflegetStatusTotal(params){
    return http.get('/server/nodestatustotal', params,{})
}

// Create an interface for activeNodes
export function getShuffleActiveNodes(params){
    return http.get('/server/nodes/active', params,{})
}

// Create an interface for lostNodes
export function getShuffleLostList(params){
    return http.get('/server/nodes/lost', params,{})
}

// Create an interface for unhealthyNodes
export function getShuffleUnhealthyList(params){
    return http.get('/server/nodes/unhealthy', params,{})
}

// Create an interface for decommissioningNodes
export function getShuffleDecommissioningList(params){
    return http.get('/server/nodes/decommissioning', params,{})
}

// Create an interface for decommissionedNodes
export function getShuffleDecommissionedList(params){
    return http.get('/server/nodes/decommissioned', params,{})
}

// Create an interface for excludeNodes
export function getShuffleExcludeNodes(params){
    return http.get('/server/nodes/excluded', params,{})
}

// Total number of interfaces for new App
export function getAppTotal(params){
    return http.get('/app/total', params,{})
}

// Create an interface for the app basic information list
export function getApplicationInfoList(params){
    return http.get('/app/appinfos', params,{})
}

// Create an interface for the number of apps for a user
export function getTotalForUser(params){
    return http.get('/app/usertotal', params,{})
}
