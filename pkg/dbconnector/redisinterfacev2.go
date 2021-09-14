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

	pgx "github.com/jackc/pgx/v4"
)

var Store DBStore

// Interface for the DB dependency. Used for mocking rg.
type DBStore interface {
	Query(q string) (pgx.Rows, error)
}

type QueryResult struct {
	Results    [][]string
	Statistics []string
}

//type QueryResult rg2.QueryResult

type PostgreSQL struct{}

// Executes the given query against redisgraph.
func (PostgreSQL) Query(q string, printResult bool) (pgx.Rows, error) {

	//get connection from the pool
	conn := GetDBConnection()
	defer conn.Close()

	rows, err := conn.Query(context.Background(), q) //querying database
	if err != nil {
		fmt.Println("Error executing query: ", err)
	}

	// for rows.Next() { //printing query rows values optional
	// 	rowValues, _ := rows.Values()
	// 	fmt.Println(rowValues)
	// }

	return rows, err
}
