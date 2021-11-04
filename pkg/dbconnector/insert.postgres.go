package dbconnector

import (
	"context"
	"encoding/json"
	"fmt"

	// "glog"
	"sync"
	"time"

	// "strings"

	"github.com/golang/glog"
	pgx "github.com/jackc/pgx/v4"
	// pgxpool "github.com/jackc/pgx/v4/pgxpool"
)

// var pool *pgxpool.Pool //change to new variable so we can use the pool package in place of dbclient
// var InsertChan chan *Record
var batch *pgx.Batch

// // from benchmark:
func InsertFunction(tableName string, records []map[string]interface{}, edges []Edge, clusterName string) {
	wg := &sync.WaitGroup{}
	start := time.Now()
	//fmt.Print(".")
	//fmt.Println(len(records))
	// fmt.Println("The uid of the first record is: ", records[0]["uid"].(string))
	// firstUID := records[0]["uid"].(string)
	// clusterName = strings.Split(firstUID, "-")[0]
	//fmt.Printf("clusterName: %s\n", clusterName)
	//_, err := database.Exec(context.Background(), fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (uid TEXT PRIMARY KEY,  edgesTo TEXT,edgesFrom TEXT)", clusterName))
	// fmt.Println("pool", database)
	//if err != nil {
	//	fmt.Printf("Error Table creation: %s\n", clusterName)
	//}
	batch := &pgx.Batch{}
	for idx, record := range records {
		//fmt.Println("Iterating Records.... ", ctr)

		// lastUID = strings.Replace(record["uid"].(string), "local-cluster", clusterName, 1) //grab uid
		lastUID = record["uid"].(string)
		fmt.Printf("lastUID: %s for cluster %s", lastUID, clusterName)
		properties, _ := record["properties"].(map[string]interface{})
		// fmt.Println(record["properties"])
		//fmt.Println("len edges ", len(edges))
		eTo := findEdgesTo(lastUID, edges)
		eFrom := findEdgesFrom(lastUID, edges)
		record := &Record{TableName: tableName, UID: lastUID, Cluster: clusterName, Properties: properties}
		batch.Queue(fmt.Sprintf("INSERT into %s values($1,$2,$3,$4,$5)", record.TableName), record.UID, record.Cluster, record.Properties, eTo, eFrom)
		fmt.Println("")
		fmt.Printf("TIME: %v \n\n", time.Since(start))
		//batch.Queue(fmt.Sprintf("INSERT into %s values($1,$2,$3)", clusterName), record.UID, eTo, eFrom)
		if idx%50 == 0 { // 50 inserts at each post
			//fmt.Printf("Posting : %d\n", idx)
			// InsertChan <- batch
			wg.Add(1)
			go sendBatch(*batch, wg)
			batch = &pgx.Batch{}
		}

	}
	if batch.Len() > 0 {
		fmt.Printf("Posting rest of the bucket: %d\n", batch.Len())
		wg.Add(1)
		go sendBatch(*batch, wg)
		batch = &pgx.Batch{}
	}
}

// tx.Commit(context.Background())

// func batchInsert(instance string) {

// 	for {

// 		batch := <-InsertChan

// 		br := database.SendBatch(context.Background(), batch) //br = batch results
// 		res, err := br.Exec()
// 		// fmt.Println("batch sent to pool")
// 		if err != nil {
// 			log.Fatal("res: ", res.RowsAffected(), "  err: ", err, batch.Len())
// 		}
// 		br.Close()

// 	}
// }

func sendBatch(batch pgx.Batch, wg *sync.WaitGroup) {
	defer wg.Done()

	// klog.Info("Sending batch")
	br := Pool.SendBatch(context.Background(), &batch)
	res, err := br.Exec()
	if err != nil {
		glog.Error("Error sending batch. res: ", res, "  err: ", err, batch.Len())

		// TODO: Need to report the errors back.
	}
	// klog.Info("Batch response: ", res)
	br.Close()
}

func findEdgesTo(sourceUID string, edges []Edge) string {
	result := make(map[string][][]string)
	for _, edge := range edges {
		//fmt.Println(edge.SourceUID, " == source UID NOT equal", sourceUID)
		if edge.SourceUID == sourceUID {

			edgeType := edge.EdgeType
			kind := edge.DestKind
			temp := make([]string, 0)
			temp = append(temp, edge.DestUID)
			temp = append(temp, kind)
			destUIDs, exist := result[edgeType]
			if exist {
				result[edgeType] = append(destUIDs, temp)
			} else {
				result[edgeType] = [][]string{temp}
			}
		}
	}
	edgeJSON, _ := json.Marshal(result)
	return string(edgeJSON)
}
func findEdgesFrom(destUID string, edges []Edge) string {
	result := make(map[string][][]string)
	for _, edge := range edges {

		//edgeMap := edge.(map[string]interface{})
		if edge.DestUID == destUID {
			edgeType := edge.EdgeType
			kind := edge.SourceKind
			temp := make([]string, 0)
			temp = append(temp, edge.SourceUID)
			temp = append(temp, kind)
			srcUIDs, exist := result[edgeType]
			if exist {
				result[edgeType] = append(srcUIDs, temp)
			} else {
				result[edgeType] = [][]string{temp}
			}
		}
	}
	edgeJSON, _ := json.Marshal(result)
	return string(edgeJSON)
}
