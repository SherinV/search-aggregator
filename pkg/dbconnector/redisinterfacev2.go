/*
IBM Confidential
OCO Source Materials
(C) Copyright IBM Corporation 2019 All Rights Reserved
The source code for this program is not published or otherwise divested of its trade secrets,
irrespective of what has been deposited with the U.S. Copyright Office.
Copyright (c) 2020 Red Hat, Inc.
*/
// Copyright Contributors to the Open Cluster Management project

package dbconnector

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"

	rg2 "github.com/redislabs/redisgraph-go"
)

var Store DBStore

// Interface for the DB dependency. Used for mocking rg.
type DBStore interface {
	Query(q string) (*rg2.QueryResult, error)
}

type QueryResult struct {
	Results    [][]string
	Statistics []string
}

//type QueryResult rg2.QueryResult

// type RedisGraphStoreV2 struct{}
type PostgreSQL struct{}

// Executes the given query against redisgraph.
// Called by the other functions in this file
// Not fully implemented
// func (RedisGraphStoreV2) Query(q string) (*rg2.QueryResult, error) {
// 	// Get connection from the pool
// 	conn := Pool.Get() // This will block if there aren't any valid connections that are available.
// 	defer conn.Close()
// 	g := rg2.Graph{
// 		Conn: conn,
// 		Id:   GRAPH_NAME,
// 	}
// 	result, err := g.Query(q)
// 	if err != nil {
// 		glog.Error("Error fetching results from RedisGraph V2 : ", err)
// 		glog.V(4).Info("Failed query: ", q)
// 	}
// 	return result, err

// }

var database *pgxpool.Pool //this will be defined in pool.go

func (PostgreSQL) Query(q string, printResult bool) {

	// Open the PostgreSQL database.
	database = dbclient.GetConnection() //this will probably be moved out of function/passed down from whereever connection initially made

	//get conenction from the pool
	conn := Pool.Get()
	defer conn.Close()

	rows, err := database.Query(context.Background(), q) //querying database
	if err != nil {
		fmt.Println("Error executing query: ", err)
	}

	for rows.Next() { //printing query rows values optional
		rowValues, _ := rows.Values()
		fmt.Println(rowValues)
	}

	return rows, err
}
