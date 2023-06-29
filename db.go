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
	fmt.Println("Tabled dropped and created.")
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
