package main

import (
	"context"
	"fmt"
	"github.com/xuri/excelize/v2"
	"sync"
	"time"
)

func main() {
	ctx := context.Background()
	var listOfPeople []string
	file, err := excelize.OpenFile("sample/data1000.xlsx")
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, rowsError := file.Rows("Sheet1")
	if rowsError != nil {
		fmt.Println(rowsError)
		return
	}

	for rows.Next() {
		row, rowError := rows.Columns()
		if rowError != nil {
			fmt.Println(rowError)
		}
		for _, colCell := range row {
			listOfPeople = append(listOfPeople, colCell)
		}
	}
	if err = rows.Close(); err != nil {
		fmt.Println(err)
	}
	fmt.Printf("First record from sample: %s\n", listOfPeople[0])
	fmt.Printf("Last record from sample: %s\n", listOfPeople[len(listOfPeople)-1])

	synchronousProcessing(ctx, listOfPeople)
	parallelProcessing(ctx, listOfPeople)
}

func synchronousProcessing(ctx context.Context, listOfPeople []string) {
	personDb := newConnection(ctx)
	personDb.dropAndCreateDatabase(ctx)
	start := time.Now()
	for _, person := range listOfPeople {
		dbError := personDb.persistPerson(ctx, person, nil)
		if dbError != nil {
			panic(dbError)
		}
	}
	elapsed := time.Since(start).Milliseconds()
	fmt.Printf("[Go] Parallel implementation tooked %d milliseconds.\n", elapsed)
	fmt.Printf("[Go] Processed %d records.\n", len(listOfPeople))
}

func parallelProcessing(ctx context.Context, listOfPeople []string) {
	var wg sync.WaitGroup
	personDb := newConnection(ctx)
	personDb.dropAndCreateDatabase(ctx)

	start := time.Now()
	count := 0
	for _, person := range listOfPeople {
		wg.Add(1)
		personP := person
		go func() {
			err := personDb.persistPerson(ctx, personP, &wg)
			if err != nil {
				fmt.Println(err)
			}
		}()
		if count >= 2000 {
			wg.Wait()
			count = 0
		}
		count++
	}
	wg.Wait()
	elapsed := time.Since(start).Milliseconds()
	fmt.Printf("[Go] Parallel implementation tooked %d milliseconds.\n", elapsed)
	fmt.Printf("[Go] Processed %d records.\n", len(listOfPeople))
}
