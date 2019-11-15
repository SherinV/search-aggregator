/*
IBM Confidential
OCO Source Materials
(C) Copyright IBM Corporation 2019 All Rights Reserved
The source code for this program is not published or otherwise divested of its trade secrets, irrespective of what has been deposited with the U.S. Copyright Office.
*/

package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.ibm.com/IBMPrivateCloud/search-aggregator/pkg/config"

	db "github.ibm.com/IBMPrivateCloud/search-aggregator/pkg/dbconnector"
)

// SyncEvent - Object sent by the collector with the resources to change.
type SyncEvent struct {
	ClearAll bool `json:"clearAll,omitempty"`

	AddResources    []*db.Resource
	UpdateResources []*db.Resource
	DeleteResources []DeleteResourceEvent

	AddEdges    []db.Edge
	DeleteEdges []db.Edge
	RequestId   int
}

// DeleteResourceEvent - Contains the information needed to delete an existing resource.
type DeleteResourceEvent struct {
	UID string `json:"uid,omitempty"`
}

// SyncResponse - Response to a SyncEvent
type SyncResponse struct {
	TotalAdded        int
	TotalUpdated      int
	TotalDeleted      int
	TotalResources    int
	TotalEdgesAdded   int
	TotalEdgesDeleted int
	TotalEdges        int
	AddErrors         []SyncError
	UpdateErrors      []SyncError
	DeleteErrors      []SyncError
	AddEdgeErrors     []SyncError
	DeleteEdgeErrors  []SyncError
	Version           string
	RequestId         int
}

// SyncError is used to respond with errors.
type SyncError struct {
	ResourceUID string
	Message     string // Often comes out of a golang error using .Error()
}

// SyncResources - Process Add, Update, and Delete events.
func SyncResources(w http.ResponseWriter, r *http.Request) {
	metrics := InitSyncMetrics()
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	clusterName := params["id"]
	glog.V(2).Info("SyncResources() for cluster: ", clusterName)
	subscriptionUpdated := false                // flag to decide the time when last suscription was changed
	subscriptionUIDMap := make(map[string]bool) // map to hold exisiting subscription uids
	policyUIDMap := make(map[string]bool)       // map to hold exisiting VA/MA Policies
	policyUpdated := false
	response := SyncResponse{Version: config.AGGREGATOR_API_VERSION}

	// Function that sends the current response and the given status code.
	// If you want to bail out early, make sure to call return right after.
	respond := func(status int) {
		statusMessage := fmt.Sprintf(
			"Responding to cluster %s with requestId %d, status %d, stats: {Added: %d, Updated: %d, Deleted: %d, Edges Added: %d, Edges Deleted: %d, Total Resources: %d, Total Edges: %d}",
			clusterName,
			response.RequestId,
			status,
			response.TotalAdded,
			response.TotalUpdated,
			response.TotalDeleted,
			response.TotalEdgesAdded,
			response.TotalEdgesDeleted,
			response.TotalResources,
			response.TotalEdges,
		)
		if status == http.StatusOK {
			glog.Infof(statusMessage)
		} else {
			glog.Errorf(statusMessage)
		}
		w.WriteHeader(status)
		encodeError := json.NewEncoder(w).Encode(response)
		if encodeError != nil {
			glog.Error("Error responding to SyncEvent:", encodeError, response)
		}
	}

	var syncEvent SyncEvent
	err := json.NewDecoder(r.Body).Decode(&syncEvent)
	if err != nil {
		glog.Error("Error decoding body of syncEvent: ", err)
		respond(http.StatusBadRequest)
		return
	}
	response.RequestId = syncEvent.RequestId
	glog.V(3).Infof("Processing Request with { request: %d, add: %d, update: %d, delete: %d edge add: %d edge delete: %d }", syncEvent.RequestId, len(syncEvent.AddResources), len(syncEvent.UpdateResources), len(syncEvent.DeleteResources), len(syncEvent.AddEdges), len(syncEvent.DeleteEdges))

	err = db.ValidateClusterName(clusterName)
	if err != nil {
		glog.Warning("Invalid Cluster Name: ", clusterName)
		respond(http.StatusBadRequest)
		return
	}

	// Validate that we have a Cluster CRD so we can build edges on create
	if !assertClusterNode(clusterName) {
		glog.Warningf("Warning, couldn't to find a Cluster resource with name: %s. This means that the sync request came from a remote cluster that hasn’t joined MCM. Rejecting the incoming sync request.", clusterName)
		respond(http.StatusBadRequest)
		return
	}

	// add cluster fields
	for i := range syncEvent.AddResources {
		syncEvent.AddResources[i].Properties["cluster"] = clusterName
	}
	for i := range syncEvent.UpdateResources {
		syncEvent.UpdateResources[i].Properties["cluster"] = clusterName
	}

	// let us store the Current Subscription Uids in a map [String] -> boolean
	uidresults, uiderr := getUIDsForSubscriptions()
	if uiderr == nil {
		if len(uidresults.Results) > 1 {
			for _, uid := range uidresults.Results[1:] {
				subscriptionUIDMap[uid[0]] = true
			}
		}

	} else {
		glog.Warningf("Error Fetching Subscriptions %s", uiderr)
	}

	// let us store the Current Policies Uids in a map [String] -> boolean
	uidresults, uiderr = getUIDsForPolicies()
	if uiderr == nil {
		if len(uidresults.Results) > 1 {
			for _, uid := range uidresults.Results[1:] {
				policyUIDMap[uid[0]] = true
			}
		}

	} else {
		glog.Warningf("Error Fetching Policies %s", uiderr)
	}

	// This usually indicates that something has gone wrong, basically that the collector detected we are out of sync and wants us to resync.
	if syncEvent.ClearAll {
		stats, err := resyncCluster(clusterName, syncEvent.AddResources, syncEvent.AddEdges)
		if err != nil {
			glog.Warning("Error on resyncCluster. ", err)
		} else {
			response.TotalAdded = stats.TotalAdded
			response.TotalUpdated = stats.TotalUpdated
			response.TotalDeleted = stats.TotalDeleted
			response.TotalEdgesAdded = stats.TotalEdgesAdded
			response.TotalEdgesDeleted = stats.TotalEdgesDeleted
			response.AddErrors = stats.AddErrors
			response.UpdateErrors = stats.UpdateErrors
			response.DeleteErrors = stats.DeleteErrors
			response.AddEdgeErrors = stats.AddEdgeErrors
			response.DeleteEdgeErrors = stats.DeleteEdgeErrors
		}

	} else {
		// INSERT Resources

		metrics.NodeSyncStart = time.Now()
		insertResponse := db.ChunkedInsert(syncEvent.AddResources, clusterName)
		response.TotalAdded = insertResponse.SuccessfulResources // could be 0
		if insertResponse.ConnectionError != nil {
			respond(http.StatusServiceUnavailable)
			return
		} else if len(insertResponse.ResourceErrors) != 0 {
			response.AddErrors = processSyncErrors(insertResponse.ResourceErrors, "inserted")
			respond(http.StatusBadRequest)
			return
		}

		// UPDATE Resources

		updateResponse := db.ChunkedUpdate(syncEvent.UpdateResources)
		response.TotalUpdated = updateResponse.SuccessfulResources // could be 0
		if updateResponse.ConnectionError != nil {
			respond(http.StatusServiceUnavailable)
			return
		} else if len(updateResponse.ResourceErrors) != 0 {
			response.UpdateErrors = processSyncErrors(updateResponse.ResourceErrors, "updated")
			respond(http.StatusBadRequest)
			return
		}

		// DELETE Resources

		// reformat to []string
		deleteUIDS := make([]string, 0, len(syncEvent.DeleteResources))
		for _, de := range syncEvent.DeleteResources {
			deleteUIDS = append(deleteUIDS, de.UID)
		}

		deleteResponse := db.ChunkedDelete(deleteUIDS)
		response.TotalDeleted = deleteResponse.SuccessfulResources // could be 0
		if deleteResponse.ConnectionError != nil {
			respond(http.StatusServiceUnavailable)
			return
		} else if len(deleteResponse.ResourceErrors) != 0 {
			response.DeleteErrors = processSyncErrors(deleteResponse.ResourceErrors, "deleted")
			respond(http.StatusBadRequest)
			return
		}
		metrics.NodeSyncEnd = time.Now()

		// Insert Edges
		metrics.EdgeSyncStart = time.Now()
		insertEdgeResponse := db.ChunkedInsertEdge(syncEvent.AddEdges)
		response.TotalEdgesAdded = insertEdgeResponse.SuccessfulResources // could be 0
		if insertEdgeResponse.ConnectionError != nil {
			respond(http.StatusServiceUnavailable)
			return
		} else if len(insertEdgeResponse.ResourceErrors) != 0 {
			response.AddEdgeErrors = processSyncErrors(insertEdgeResponse.ResourceErrors, "inserted by edge")
			respond(http.StatusBadRequest)
			return
		}

		// Delete Edges
		deleteEdgeResponse := db.ChunkedDeleteEdge(syncEvent.DeleteEdges)
		response.TotalEdgesDeleted = deleteEdgeResponse.SuccessfulResources // could be 0
		if deleteEdgeResponse.ConnectionError != nil {
			respond(http.StatusServiceUnavailable)
			return
		} else if len(deleteEdgeResponse.ResourceErrors) != 0 {
			response.DeleteEdgeErrors = processSyncErrors(deleteEdgeResponse.ResourceErrors, "removed by edge")
			respond(http.StatusBadRequest)
			return
		}

		metrics.EdgeSyncEnd = time.Now()
		metrics.SyncEnd = time.Now()
		metrics.LogPerformanceMetrics(clusterName, syncEvent)
	}

	glog.V(2).Infof("Done updating resources for cluster %s, preparing response", clusterName)
	response.TotalResources = computeNodeCount(clusterName) // This goes out to the DB through a work order, so it can take a second
	response.TotalEdges = computeIntraEdges(clusterName)

	respond(http.StatusOK)

	// update the timestamp if we made any changes Kind = Subscription OR Kind = Policy
	// An Edge which connect from/to a Node (Kind = Subscription) or (Kind = Policy or VulnerabilityPolicy or MutationPolicy)

	// if any Node with kind Subscription Added then subscriptionUpdated
	// if any Node with kind Policy/VulnerabilityPolicy / MutationPolicy Added then policyUpdated
	for i := range syncEvent.AddResources {
		if (!subscriptionUpdated) && (syncEvent.AddResources[i].Properties["kind"] == "Subscription" || syncEvent.AddResources[i].Properties["kind"] == "Application") {
			glog.V(3).Infof("Will trigger Intercluster - Added  Node %s ", syncEvent.AddResources[i].Properties["name"])
			subscriptionUpdated = true
		} else if (!policyUpdated) && (syncEvent.AddResources[i].Properties["kind"] == "Policy" || syncEvent.AddResources[i].Properties["kind"] == "MutationPolicy" || syncEvent.AddResources[i].Properties["kind"] == "VulnerabilityPolicy") {
			glog.V(3).Infof("Will trigger Policy Intercluster - Added  Node %s ", syncEvent.AddResources[i].Properties["name"])
			policyUpdated = true
		}
		if subscriptionUpdated && policyUpdated {
			break
		}
	}
	if !subscriptionUpdated || !policyUpdated {
		for i := range syncEvent.UpdateResources {
			if (!subscriptionUpdated) && (syncEvent.UpdateResources[i].Properties["kind"] == "Subscription" || syncEvent.UpdateResources[i].Properties["kind"] == "Application") {
				glog.V(3).Infof("Will trigger Intercluster - Updated  Node %s ", syncEvent.UpdateResources[i].Properties["name"])
				subscriptionUpdated = true
			} else if (!policyUpdated) && (syncEvent.UpdateResources[i].Properties["kind"] == "Policy" || syncEvent.UpdateResources[i].Properties["kind"] == "MutationPolicy" || syncEvent.UpdateResources[i].Properties["kind"] == "VulnerabilityPolicy") {
				glog.V(3).Infof("Will trigger Policy Intercluster - Updated  Node %s ", syncEvent.UpdateResources[i].Properties["name"])
				policyUpdated = true
			}
			if subscriptionUpdated && policyUpdated {
				break
			}
		}
	}
	if !subscriptionUpdated || !policyUpdated {
		for i := range syncEvent.AddEdges {
			if (!subscriptionUpdated) && (subscriptionUIDMap[syncEvent.AddEdges[i].SourceUID] || subscriptionUIDMap[syncEvent.AddEdges[i].DestUID]) {
				glog.V(3).Infof("Will trigger Intercluster Added Edge %s -> %s ", syncEvent.AddEdges[i].SourceUID, syncEvent.AddEdges[i].DestUID)
				subscriptionUpdated = true
			} else if (!policyUpdated) && (policyUIDMap[syncEvent.AddEdges[i].SourceUID] || policyUIDMap[syncEvent.AddEdges[i].DestUID]) {
				glog.V(3).Infof("Will trigger Policy Intercluster Added Edge %s -> %s ", syncEvent.AddEdges[i].SourceUID, syncEvent.AddEdges[i].DestUID)
				policyUpdated = true
			}
			if subscriptionUpdated && policyUpdated {
				break
			}
		}
	}
	if !subscriptionUpdated || !policyUpdated {
		for i := range syncEvent.DeleteEdges {
			if (!subscriptionUpdated) && (subscriptionUIDMap[syncEvent.DeleteEdges[i].SourceUID] || subscriptionUIDMap[syncEvent.DeleteEdges[i].DestUID]) {
				glog.V(3).Infof("Will trigger Intercluster Deleted Edge %s -> %s ", syncEvent.DeleteEdges[i].SourceUID, syncEvent.DeleteEdges[i].DestUID)
				subscriptionUpdated = true
			} else if (!policyUpdated) && (policyUIDMap[syncEvent.DeleteEdges[i].SourceUID] || policyUIDMap[syncEvent.DeleteEdges[i].DestUID]) {
				glog.V(3).Infof("Will trigger Policy Intercluster Deleted Edge %s -> %s ", syncEvent.DeleteEdges[i].SourceUID, syncEvent.DeleteEdges[i].DestUID)
				policyUpdated = true
			}
			if subscriptionUpdated && policyUpdated {
				break
			}
		}
	}

	if policyUpdated {
		PolicyLastUpdated = time.Now()
		// Policy update triggers Build SubscriptionEdges , we dont need to send twice
		//Skip updating ApplicationLastUpdated
	} else if subscriptionUpdated {
		ApplicationLastUpdated = time.Now()
	}
}

// internal function to inline the errors
func processSyncErrors(re map[string]error, verb string) []SyncError {
	ret := []SyncError{}
	for uid, e := range re {
		glog.Errorf("Resource %s cannot be %s: %s", uid, verb, e)
		ret = append(ret, SyncError{
			ResourceUID: uid,
			Message:     e.Error(),
		})
	}

	return ret
}
