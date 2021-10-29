// /*
// IBM Confidential
// OCO Source Materials
// (C) Copyright IBM Corporation 2019 All Rights Reserved
// The source code for this program is not published or otherwise divested of its trade secrets,
// irrespective of what has been deposited with the U.S. Copyright Office.

// Copyright (c) 2020 Red Hat, Inc.
// */
// // Copyright Contributors to the Open Cluster Management project
package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"

	// "time"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/open-cluster-management/search-aggregator/pkg/config"
	db "github.com/open-cluster-management/search-aggregator/pkg/dbconnector"
)

// SyncEvent - Object sent by the collector with the resources to change.
type SyncEvent struct {
	ClearAll     bool `json:"clearAll,omitempty"`
	AddResources []map[string]interface{}
	AddEdges     []db.Edge
	RequestId    int
}

// DeleteResourceEvent - Contains the information needed to delete an existing resource.
type DeleteResourceEvent struct {
	UID string `json:"uid,omitempty"`
}

// SyncResponse - Response to a SyncEvent
type SyncResponse struct {
	TotalAdded     int
	TotalResources int
	Version        string
	RequestId      int
}

var database *pgxpool.Pool

const TOTAL_CLUSTERS = 1
const CLUSTER_SHARDING bool = false

// // SyncError is used to respond with errors.
// type SyncError struct {
// 	ResourceUID string
// 	Message     string // Often comes out of a golang error using .Error()
// }

// SyncResources - Process Add, Update, and Delete events.
func SyncResources(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	// fmt.Println("Request:", r)
	params := mux.Vars(r)

	// fmt.Println("params:", params["uid"]) //returns empty

	clusterName := params["uid"]

	// fmt.Println(clusterName)

	// Limit amount of concurrent requests to prevent overloading Redis.
	// Give priority to the local-cluster, because it's the hub and this is how we debug search.
	// TODO: The next step is to degrade performance instead of rejecting the request.
	//       We will give priority to nodes over edges after reaching certain load.
	//       Will also prioritize small updates over a large resync.
	//if len(PendingRequests) >= config.Cfg.RequestLimit && clusterName != "local-cluster" { //start here
	//	glog.Warningf("Too many pending requests (%d). Rejecting sync from %s", len(PendingRequests), clusterName)
	//	http.Error(w, "Aggregator has many pending requests, retry later.", http.StatusTooManyRequests)
	//	return
	//}

	glog.Info("Starting SyncResources() for cluster: ", clusterName)

	metrics := InitSyncMetrics(clusterName)
	defer metrics.CompleteSyncEvent()

	// 	subscriptionUpdated := false                // flag to decide the time when last suscription was changed
	// 	subscriptionUIDMap := make(map[string]bool) // map to hold exisiting subscription uids
	response := SyncResponse{Version: config.AGGREGATOR_API_VERSION}
	// fmt.Println("Response set")

	// 	// Function that sends the current response and the given status code.
	// 	// If you want to bail out early, make sure to call return right after.
	fmt.Println("Setting up function that sends the current response and the given status code.")
	respond := func(status int) {
		statusMessage := fmt.Sprintf(
			"Responding to cluster %s with requestId %d, status %d",
			clusterName,
			response.RequestId,
			status,
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
	glog.Infof(
		"Processing Request { request: %d, add: %d }",
		syncEvent.RequestId, len(syncEvent.AddResources))

	for i := 0; i < TOTAL_CLUSTERS; i++ {

		if CLUSTER_SHARDING {
			tableName := fmt.Sprintf("cluster%d", i)
			db.InsertFunction(tableName, syncEvent.AddResources, syncEvent.AddEdges, fmt.Sprintf("cluster%d", i))
		} else {
			tableName := "resources"
			db.InsertFunction(tableName, syncEvent.AddResources, syncEvent.AddEdges, fmt.Sprintf("cluster%d", i))
			//db.InsertEdgesFunction(syncEvent.AddEdges, database, fmt.Sprintf("cluster%d", i))
		}
	}
	// metrics.EdgeSyncEnd = time.Now()

	// metrics.SyncEnd = time.Now()
	// metrics.LogPerformanceMetrics(syncEvent)

	// glog.V(2).Infof("syncResources complete. Done updating resources for cluster %s, preparing response", clusterName)
	// response.TotalResources = computeNodeCount(clusterName) // This goes out to the DB, so it can take a second
	// response.TotalEdges = computeIntraEdges(clusterName)
	PrintMemUsage("After inserting data into resources or cluster tables.")
	respond(http.StatusOK)

	return
}

func PrintMemUsage(PrintStatement string) {
	fmt.Println(PrintStatement)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Println("\nMEMORY USAGE:")
	fmt.Printf("\tAlloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\n\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\n\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\n\tNumGC = %v\n\n", m.NumGC)
}
func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

// 	// if any Node with kind Subscription Added then subscriptionUpdated
// 	for i := range syncEvent.AddResources {
// 		if (!subscriptionUpdated) && (syncEvent.AddResources[i].Properties["kind"] == "Subscription") {
// 			glog.V(3).Infof("Will trigger Intercluster - Added Node %s ", syncEvent.AddResources[i].Properties["name"])
// 			subscriptionUpdated = true
// 			break
// 		}
// 	}
// 	if !subscriptionUpdated {
// 		for i := range syncEvent.UpdateResources {
// 			if (!subscriptionUpdated) && (syncEvent.UpdateResources[i].Properties["kind"] == "Subscription") {
// 				glog.V(3).Infof("Will trigger Intercluster - Updated Node %s ",
// 					syncEvent.UpdateResources[i].Properties["name"])
// 				subscriptionUpdated = true
// 				break
// 			}
// 		}
// 	}

// 	if subscriptionUpdated {
// 		ApplicationLastUpdated = time.Now()
// 	}
// }

// internal function to inline the errors
// func processSyncErrors(re map[string]error, verb string) []SyncError {
// 	ret := []SyncError{}
// 	for uid, e := range re {
// 		glog.Errorf("Resource %s cannot be %s: %s", uid, verb, e)
// 		ret = append(ret, SyncError{
// 			ResourceUID: uid,
// 			Message:     e.Error(),
// 		})
// 	}

// 	return ret
// }

//end

// 	response.AddErrors = processSyncErrors(insertResponse.ResourceErrors, "inserted")
// 	respond(http.StatusBadRequest)
// 	return
// } //end here
// fmt.Sprintln("Inserting ", tableName)
// if !SINGLE_TABLE {
// 	insertEdges(addEdges, edgeStmt, fmt.Sprintf("cluster%d", i))
// }
// database.Exec("COMMIT TRANSACTION")

// metrics.NodeSyncStart = time.Now()
// insertResponse := db.ChunkedInsert(syncEvent.AddResources, clusterName) *
// response.TotalAdded = insertResponse.SuccessfulResources // could be 0
// if insertResponse.ConnectionError != nil {
// 	respond(http.StatusServiceUnavailable)
// 	return
// } else if len(insertResponse.ResourceErrors) != 0 {
// 	response.AddErrors = processSyncErrors(insertResponse.ResourceErrors, "inserted")
// 	respond(http.StatusBadRequest)
// 	return
// }

// 		// UPDATE Resources

// 		updateResponse := db.ChunkedUpdate(syncEvent.UpdateResources)
// 		response.TotalUpdated = updateResponse.SuccessfulResources // could be 0
// 		if updateResponse.ConnectionError != nil {
// 			respond(http.StatusServiceUnavailable)
// 			return
// 		} else if len(updateResponse.ResourceErrors) != 0 {
// 			response.UpdateErrors = processSyncErrors(updateResponse.ResourceErrors, "updated")
// 			respond(http.StatusBadRequest)
// 			return
// 		}

// 		// DELETE Resources

// 		// reformat to []string
// 		deleteUIDS := make([]string, 0, len(syncEvent.DeleteResources))
// 		for _, de := range syncEvent.DeleteResources {
// 			deleteUIDS = append(deleteUIDS, de.UID)
// 			// If we are deleting any subscriptions better run interclusteredges - Setting flag to true
// 			if !subscriptionUpdated {
// 				if _, ok := subscriptionUIDMap[de.UID]; ok {
// 					subscriptionUpdated = true
// 				}
// 			}

// 		}

// 		deleteResponse := db.ChunkedDelete(deleteUIDS)
// 		response.TotalDeleted = deleteResponse.SuccessfulResources // could be 0
// 		if deleteResponse.ConnectionError != nil {
// 			respond(http.StatusServiceUnavailable)
// 			return
// 		} else if len(deleteResponse.ResourceErrors) != 0 {
// 			response.DeleteErrors = processSyncErrors(deleteResponse.ResourceErrors, "deleted")
// 			respond(http.StatusBadRequest)
// 			return
// 		}
// 		metrics.NodeSyncEnd = time.Now()

// 		// Insert Edges
// 		metrics.EdgeSyncStart = time.Now()
// 		glog.V(4).Info("Sync cluster ", clusterName, ": Number of edges to insert: ", len(syncEvent.AddEdges))
// 		insertEdgeResponse := db.ChunkedInsertEdge(syncEvent.AddEdges, clusterName)
// 		response.TotalEdgesAdded = insertEdgeResponse.SuccessfulResources // could be 0
// 		if insertEdgeResponse.ConnectionError != nil {
// 			respond(http.StatusServiceUnavailable)
// 			return
// 		} else if len(insertEdgeResponse.ResourceErrors) != 0 {
// 			response.AddEdgeErrors = processSyncErrors(insertEdgeResponse.ResourceErrors, "inserted by edge")
// 			respond(http.StatusBadRequest)
// 			return
// 		}

// 		// Delete Edges
// 		glog.V(4).Info("Sync cluster ", clusterName, ": Number of edges to delete: ", len(syncEvent.DeleteEdges))
// 		deleteEdgeResponse := db.ChunkedDeleteEdge(syncEvent.DeleteEdges, clusterName)
// 		response.TotalEdgesDeleted = deleteEdgeResponse.SuccessfulResources // could be 0
// 		if deleteEdgeResponse.ConnectionError != nil {
// 			respond(http.StatusServiceUnavailable)
// 			return
// 		} else if len(deleteEdgeResponse.ResourceErrors) != 0 {
// 			response.DeleteEdgeErrors = processSyncErrors(deleteEdgeResponse.ResourceErrors, "removed by edge")
// 			respond(http.StatusBadRequest)
// 			return
// 		}

// 		metrics.EdgeSyncEnd = time.Now()
// 	}
// 	metrics.SyncEnd = time.Now()
// 	metrics.LogPerformanceMetrics(syncEvent)

// glog.V(2).Infof("syncResources complete. Done updating resources for cluster %s, preparing response", clusterName)
// response.TotalResources = computeNodeCount(clusterName) // This goes out to the DB, so it can take a second

// 	response.TotalEdges = computeIntraEdges(clusterName)

// 	respond(http.StatusOK)

// 	// update the timestamp if we made any changes Kind = Subscription

// 	// if any Node with kind Subscription Added then subscriptionUpdated
// 	for i := range syncEvent.AddResources {
// 		if (!subscriptionUpdated) && (syncEvent.AddResources[i].Properties["kind"] == "Subscription") {
// 			glog.V(3).Infof("Will trigger Intercluster - Added Node %s ", syncEvent.AddResources[i].Properties["name"])
// 			subscriptionUpdated = true
// 			break
// 		}
// 	}
// 	if !subscriptionUpdated {
// 		for i := range syncEvent.UpdateResources {
// 			if (!subscriptionUpdated) && (syncEvent.UpdateResources[i].Properties["kind"] == "Subscription") {
// 				glog.V(3).Infof("Will trigger Intercluster - Updated Node %s ",
// 					syncEvent.UpdateResources[i].Properties["name"])
// 				subscriptionUpdated = true
// 				break
// 			}
// 		}
// 	}

// 	if subscriptionUpdated {
// 		ApplicationLastUpdated = time.Now()
// 	}
// }

// // internal function to inline the errors
// func processSyncErrors(re map[string]error, verb string) []SyncError {
// 	ret := []SyncError{}
// 	for uid, e := range re {
// 		glog.Errorf("Resource %s cannot be %s: %s", uid, verb, e)
// 		ret = append(ret, SyncError{
// 			ResourceUID: uid,
// 			Message:     e.Error(),
// 		})
// 	}

// 	return ret
// }
// }
