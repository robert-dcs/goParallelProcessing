package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"sync"
)

const dbUrl = "postgres://postgres:321@localhost:5432/postgres"

type DbConnection struct {
	dbConn *pgxpool.Pool
}

func newConnection(ctx context.Context) DbConnection {
	dbConnection, err := pgxpool.Connect(ctx, dbUrl)
	if err != nil {
		panic(err)
	}
	return DbConnection{
		dbConn: dbConnection,
	}

}

func (c DbConnection) dropAndCreateDatabase(ctx context.Context) {
	tx, err := c.dbConn.Begin(ctx)
	if err != nil {
		panic(err.Error())
	}

	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "DROP TABLE IF EXISTS person")
	if err != nil {
		panic(err.Error())
	}

	_, err = tx.Exec(ctx, "CREATE TABLE person(id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL)")
	if err != nil {
		panic(err.Error())
	}

	err = tx.Commit(ctx)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("Db initialized")
}

func (c DbConnection) persistPerson(ctx context.Context, person string, wg *sync.WaitGroup) error {
	if wg != nil {
		defer wg.Done()
	}

	sql := fmt.Sprintf("insert into person(name) values ('%s')", person)
	_, err := c.dbConn.Exec(ctx, sql)
	if err != nil {
		return err
	}

	return nil
}

func (c DbConnection) countRows(ctx context.Context) error {
	var numberOfRows int

	sql := "SELECT count(*) FROM person"
	err := c.dbConn.QueryRow(ctx, sql).Scan(&numberOfRows)
	if err != nil {
		return err
	}
	fmt.Printf("Db number of rows: %v \n", numberOfRows)

	return nil
}
func (c DbConnection) getFirstRecord(ctx context.Context) error {
	var firstRecord string

	sql := "SELECT name FROM person ORDER BY id ASC LIMIT 1"
	err := c.dbConn.QueryRow(ctx, sql).Scan(&firstRecord)
	if err != nil {
		return err
	}
	fmt.Printf("First record: %v \n", firstRecord)

	return nil
}

func (c DbConnection) getLastRecord(ctx context.Context) error {
	var lastRecord string

	sql := "SELECT name FROM person ORDER BY id DESC LIMIT 1"
	err := c.dbConn.QueryRow(ctx, sql).Scan(&lastRecord)
	if err != nil {
		return err
	}
	fmt.Printf("Last record: %v \n", lastRecord)

	return nil
}
