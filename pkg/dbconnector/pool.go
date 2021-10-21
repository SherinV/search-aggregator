/*
IBM Confidential
OCO Source Materials
(C) Copyright IBM Corporation 2019 All Rights Reserved
The source code for this program is not published or otherwise divested of its trade secrets,
irrespective of what has been deposited with the U.S. Copyright Office.

Copyright (c) 2020 Red Hat, Inc.
*/
// Copyright Contributors to the Open Cluster Management project

// pool.go will set up database connection and pools by getting user creds and validate connection, then it will create schema and tables for database.

package dbconnector

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	pgxpool "github.com/jackc/pgx/v4/pgxpool"
)

var Pool *pgxpool.Pool
var err error

const (
	IDLE_TIMEOUT     = 60 // ReadinessProbe runs every 30 seconds, this keeps the connection alive between probe intervals.
	maxConnections   = 8
	SINGLE_TABLE     = true
	CLUSTER_SHARDING = false
	TOTAL_CLUSTERS   = 2
)

var lastUID string
var database *pgxpool.Pool
var InsertChan chan *Record

func init() {
	InsertChan = make(chan *Record, 100)
	glog.Info("In init dbconnector")
	Pool, err = setUpDBConnection()

	database = Pool

	createTables()
	fmt.Println("Created tables")

	go batchInsert("goroutine1")
	go batchInsert("goroutine2")

	// if database == nil {
	// 	fmt.Println("nil")
	// } else {
	// 	fmt.Println("Not nil")ß
	// }

	if err != nil {
		glog.Error("Error connecting to db", err)
	}
}

func createTables() {
	// start building tabels

	fmt.Println("Building tables")

	if SINGLE_TABLE {

		if CLUSTER_SHARDING {
			for i := 0; i < TOTAL_CLUSTERS; i++ {
				clusterName := fmt.Sprintf("cluster%d", i)

				dquery := fmt.Sprintf("drop table if exists %s cascade; ", clusterName)

				database.Exec(context.Background(), dquery)
				database.Exec(context.Background(), "COMMIT TRANSACTION")

				cquery := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (uid TEXT PRIMARY KEY, cluster TEXT, data JSONB)", clusterName)

				_, err := database.Exec(context.Background(), cquery)
				if err != nil {
					fmt.Println(err)
				}
			}
		} else { //  single table but not cluster sharding

			database.Exec(context.Background(), "DROP TABLE resources")
			database.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS resources (uid TEXT PRIMARY KEY, cluster TEXT, data JSONB)")

		}
	} else {
		database.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS resources (uid TEXT PRIMARY KEY, data TEXT)")
		database.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS relationships (sourceId TEXT, destId TEXT)")

	}

}

// SELECT table_name FROM information_schema.tables where table_name like 'cluster%' check tables

func getEnvOrUseDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func GetDBConnection() *pgxpool.Pool {
	if Pool != nil {
		err := validateDBConnection(Pool, time.Now())

		if err != nil {
			glog.Fatal("Connection to db unsuccessful*** END")
			panic(err)
		} else {
			glog.Info("Connection to db successful")
		}
		return Pool
	} else {
		glog.Fatal("No Connection to db available. Pool is NIL")
	}
	return nil
}

func setUpDBConnection() (*pgxpool.Pool, error) {
	DB_HOST := getEnvOrUseDefault("DB_HOST", "localhost")
	DB_USER := getEnvOrUseDefault("DB_USER", "hippo")
	DB_NAME := getEnvOrUseDefault("DB_NAME", "hippo")
	// DB_PASSWORD := url.QueryEscape(getEnvOrUseDefault("DB_PASSWORD", ""))

	DB_PASSWORD := url.QueryEscape(getEnvOrUseDefault("DB_PASSWORD", ""))
	DB_PORT, err := strconv.Atoi(getEnvOrUseDefault("DB_PORT", "5432"))
	if err != nil {
		DB_PORT = 5432
		glog.Error("Error parsing db port, using default 5432")
	}

	database_url := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
	glog.Info("Connecting to PostgreSQL at: ", strings.ReplaceAll(database_url, DB_PASSWORD, "*****"))
	config, connerr := pgxpool.ParseConfig(database_url)
	if connerr != nil {
		glog.Info("Error connecting to DB:", connerr)
	}
	config.MaxConns = maxConnections
	conn, err := pgxpool.ConnectConfig(context.Background(), config)

	if err != nil {
		glog.Error("Error connecting to database. Original error: ", err)
		return nil, err
	}
	return conn, nil
}

// Used by the pool to test if redis connections are still okay. If they have been idle for less than a minute,
// just assumes they are okay. If not, calls PING.
func validateDBConnection(c *pgxpool.Pool, t time.Time) error {
	// if time.Since(t) < IDLE_TIMEOUT*time.Second {
	// 	return nil
	// }
	err := c.Ping(context.Background()) //c.Do("PING")
	return err
}
