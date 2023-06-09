package main

import (
	"context"
	"fmt"
	"github.com/xuri/excelize/v2"
	"strconv"
	"sync"
	"time"
)

type algorithmProcessor struct {
	con DbConnection
}

func main() {
	sampleSize := 1000000
	ctx := context.Background()
	var listOfPeople []string
	file, err := excelize.OpenFile("sample/data" + strconv.Itoa(sampleSize) + ".xlsx")
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
	for i := 0; i < 1; i++ {
		synchronousProcessing(ctx, listOfPeople)
		parallelProcessing(ctx, listOfPeople)
		parallelProcessing2(ctx, listOfPeople)
	}
}

func synchronousProcessing(ctx context.Context, listOfPeople []string) {
	personDb := newConnection(ctx)
	defer personDb.dbConn.Close()
	personDb.dropAndCreateDatabase(ctx)
	counter := 0

	start := time.Now()
	for _, person := range listOfPeople {
		counter++
		dbError := personDb.persistPerson(ctx, person, nil)
		if dbError != nil {
			panic(dbError)
		}
	}
	elapsed := time.Since(start).Milliseconds()

	fmt.Printf("[Go] Synchronous implementation tooked %d milliseconds.\n", elapsed)
	fmt.Printf("[Go] Processed %d records.\n", len(listOfPeople))
}

func parallelProcessing(ctx context.Context, listOfPeople []string) {
	var wg sync.WaitGroup
	personDb := newConnection(ctx)
	defer personDb.dbConn.Close()
	personDb.dropAndCreateDatabase(ctx)
	count := 0

	start := time.Now()
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

	fmt.Printf("[Go] Parallel implementation 1 tooked %d milliseconds.\n", elapsed)
	fmt.Printf("[Go] Processed %d records.\n", len(listOfPeople))
}

func parallelProcessing2(ctx context.Context, listOfPeople []string) {
	personDb := newConnection(ctx)
	defer personDb.dbConn.Close()
	personDb.dropAndCreateDatabase(ctx)

	workers := 2000
	results := make(chan error, len(listOfPeople))

	processPerson := func(person string) {
		err := personDb.persistPerson(ctx, person, nil)
		results <- err
	}

	start := time.Now()
	for i := 0; i < workers; i++ {
		go func() {
			for _, person := range listOfPeople {
				processPerson(person)
			}
		}()
	}

	for i := 0; i < len(listOfPeople); i++ {
		<-results
	}
	elapsed := time.Since(start).Milliseconds()

	fmt.Printf("[Go] Parallel implementation 2 took %d milliseconds.\n", elapsed)
	fmt.Printf("[Go] Processed %d records.\n", len(listOfPeople))
}
