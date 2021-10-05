package dbconnector

import (
	"context"
	"fmt"
	"strings"

	pgx "github.com/jackc/pgx/v4"
	pgxpool "github.com/jackc/pgx/v4/pgxpool"
)

// var pool *pgxpool.Pool //change to new variable so we can use the pool package in place of dbclient
// var InsertChan chan *Record
var batch *pgx.Batch

// // from benchmark:
func InsertFunction(tableName string, records []map[string]interface{}, database *pgxpool.Pool, clusterName string) {
	fmt.Print(".")
	fmt.Println(tableName)
	fmt.Println("pool", database)

	for _, record := range records {
		// fmt.Printf("Record: %T\n", record)
		lastUID = strings.Replace(record["uid"].(string), "local-cluster", clusterName, 1) //grab uid

		// fmt.Println("UID:", lastUID)
		var err error
		if SINGLE_TABLE {
			properties, _ := record["properties"].(map[string]interface{})
			// fmt.Println(record["properties"])

			record := &Record{TableName: tableName, UID: lastUID, Cluster: clusterName, Properties: properties}
			InsertChan <- record

			if err != nil {
				fmt.Println("Error inserting record:", err, record)
				panic(err)
			}
		}
	}
}

// tx.Commit(context.Background())

const BatchSize = 10 //resources at a time

func batchInsert(instance string) {
	batch := &pgx.Batch{}

	for {
		record := <-InsertChan

		batch.Queue(fmt.Sprintf("INSERT into %s values($1,$2,$3)", record.TableName), record.UID, record.Cluster, record.Properties)
		fmt.Println("batch queued")
		fmt.Println(batch)
		if batch.Len() == BatchSize || (batch.Len() > 0) {
			fmt.Print("+")
			fmt.Println("POOL:", database)
			fmt.Println("context", context.Background())
			br := database.SendBatch(context.Background(), batch) //br = batch results
			res, err := br.Exec()
			fmt.Println("batch sent to pool")
			if err != nil {
				fmt.Println("res: ", res, "  err: ", err, batch.Len())
			}
			br.Close()
			batch = &pgx.Batch{}
		} else {
			break
		}

	}
}
