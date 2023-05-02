package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
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
	// Rollback is safe to call even if the tx is already closed, so if
	// the tx commits successfully, this is a no-op
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "DROP TABLE IF EXISTS persons")
	if err != nil {
		panic(err.Error())
	}

	_, err = tx.Exec(ctx, "CREATE TABLE persons(id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL)")
	if err != nil {
		panic(err.Error())
	}

	err = tx.Commit(ctx)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("Tabled dropped and created.")
}

func (c DbConnection) persistPerson(ctx context.Context, person string) error {
	defer wg.Done()
	tx, err := c.dbConn.Begin(ctx)
	if err != nil {
		return err
	}
	// Rollback is safe to call even if the tx is already closed, so if
	// the tx commits successfully, this is a no-op
	defer tx.Rollback(ctx)

	sql := fmt.Sprintf("insert into persons(name) values ('%s')", person)
	_, err = tx.Exec(ctx, sql)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}
