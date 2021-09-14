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
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	// "github.com/gomodule/redigo/redis"
	pgxpool "github.com/jackc/pgx/v4/pgxpool"
)

// A global redis pool for other parts of this package to use
// var Pool *redis.Pool
var Pool *pgxpool.Pool
var err error

const (
	IDLE_TIMEOUT = 60 // ReadinessProbe runs every 30 seconds, this keeps the connection alive between probe intervals.
	GRAPH_NAME   = "search-db"
)

const maxConnections = 8

// Initializes the pool using functions in this file.
// Also initializes the Store interface.
func init() {

	Pool, err = setUpDBConnection()
	if err != nil {
		glog.Error("Error connecting to db", err)
	}
}

func getEnvOrUseDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func GetDBConnection() *pgxpool.Pool {
	err := validateDBConnection(Pool, time.Now())
	if err != nil {
		panic(err)
	}
	glog.Info("Connection to db successful")

	return Pool
}

func setUpDBConnection() (*pgxpool.Pool, error) {
	DB_HOST := getEnvOrUseDefault("DB_HOST", "localhost")
	DB_USER := getEnvOrUseDefault("DB_USER", "hippo")
	DB_NAME := getEnvOrUseDefault("DB_NAME", "hippo")
	// DB_PASSWORD := url.QueryEscape(getEnvOrUseDefault("DB_PASSWORD", ""))

	DB_PASSWORD := url.QueryEscape(getEnvOrUseDefault("DB_PASSWORD", "esNi;fblfWFTZpo8,QIw[;hI"))
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
	if time.Since(t) < IDLE_TIMEOUT*time.Second {
		return nil
	}
	err := c.Ping(context.Background()) //c.Do("PING")
	return err
}
