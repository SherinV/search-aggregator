package dbconnector

import (
	"context"
	"fmt"
	"log"
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
	fmt.Println(len(records))

	// fmt.Println("pool", database)
	batch := &pgx.Batch{}
	for idx, record := range records {
		//fmt.Println("Iterating Records.... ", ctr)
		//fmt.Printf("Record UID: %s\n", record["uid"].(string))
		lastUID = strings.Replace(record["uid"].(string), "local-cluster", clusterName, 1) //grab uid

		// fmt.Println("UID:", lastUID)
		var err error
		if SINGLE_TABLE {
			properties, _ := record["properties"].(map[string]interface{})
			// fmt.Println(record["properties"])
			record := &Record{TableName: tableName, UID: lastUID, Cluster: clusterName, Properties: properties}
			batch.Queue(fmt.Sprintf("INSERT into %s values($1,$2,$3)", record.TableName), record.UID, record.Cluster, record.Properties)
			if idx%50 == 0 { // 50 inserts at each post
				fmt.Printf("Posting : %d\n", idx)
				InsertChan <- batch
				batch = &pgx.Batch{}
			}

			if err != nil {
				fmt.Println("Error inserting record:", err, record)
				panic(err)
			}
		}
	}
	if batch.Len() > 0 {
		fmt.Printf("Posting rest of the bucket: %d\n", batch.Len())
		InsertChan <- batch
		batch = &pgx.Batch{}
	}
}

// tx.Commit(context.Background())

const BatchSize = 200 //resources at a time

func batchInsert(instance string) {

	for {

		batch := <-InsertChan

		br := database.SendBatch(context.Background(), batch) //br = batch results
		res, err := br.Exec()
		// fmt.Println("batch sent to pool")
		if err != nil {
			log.Fatal("res: ", res.RowsAffected(), "  err: ", err, batch.Len())
		}
		br.Close()

	}
}
