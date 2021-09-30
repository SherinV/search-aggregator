package dbconnector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	pgx "github.com/jackc/pgx/v4"
	pgxpool "github.com/jackc/pgx/v4/pgxpool"
	"github.com/jlpadilla/benchmark-postgres/pkg/dbclient"
)

var pool *pgxpool.Pool //change to new variable so we can use the pool package in place of dbclient
var InsertChan chan *Record

// // from benchmark:
func InsertFunction(tableName string, records []map[string]interface{}, db *pgxpool.Pool, clusterName string) {
	fmt.Print(".")
	fmt.Println(tableName)
	for _, record := range records {
		fmt.Println("Record:", record)
		lastUID = strings.Replace(record["uid"].(string), "local-cluster", clusterName, 1) //grab uid
		fmt.Println("UID:", lastUID)
		var err error
		if SINGLE_TABLE {

			var data map[string]interface{}
			bytes := []byte(record["data"].(string)) // error is here
			if err := json.Unmarshal(bytes, &data); err != nil {
				panic(err)
			}

			// fmt.Printf("Pushing to insert channnel... cluster %s. %s\n", clusterName, lastUID)
			record := &Record{TableName: tableName, UID: lastUID, Cluster: clusterName, Name: "TODO:Name here", Properties: data}
			InsertChan <- record

		} else {
			// _, err = statement.Exec(context.Background(), lastUID, record["data"])

			record := &dbclient.Record{UID: lastUID, Cluster: clusterName, Name: "TODO:Name here", Properties: record["data"].(map[string]interface{})}
			dbclient.InsertChan <- record
		}
		if err != nil {
			fmt.Println("Error inserting record:", err, record)
			panic(err)
		}
	}
	// tx.Commit(context.Background())
}

const BatchSize = 500

// Process records using batched INSERT requests.
func batchInsert(instance string) {
	batch := &pgx.Batch{}

	for {
		record, more := <-InsertChan
		if more {
			json, err := json.Marshal(record.Properties)
			if err != nil {
				panic(fmt.Sprintf("Error Marshaling json. %v %v", err, json))
			} else {
				batch.Queue(fmt.Sprintf("INSERT into %s values($1,$2,$3)", record.TableName), record.UID, record.Cluster, string(json))
			}
		}

		if batch.Len() == BatchSize || (!more && batch.Len() > 0) {
			fmt.Print("+")
			br := pool.SendBatch(context.Background(), batch)
			res, err := br.Exec()
			if err != nil {
				fmt.Println("res: ", res, "  err: ", err, batch.Len())
			}
			br.Close()
			batch = &pgx.Batch{}
		}
		if !more {
			break
		}
	}
}
