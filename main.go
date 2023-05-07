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
	f, err := excelize.OpenFile("data.xlsx")
	if err != nil {
		fmt.Println(err)
		return
	}

	rows, err := f.Rows("Sheet1")
	if err != nil {
		fmt.Println(err)
		return
	}

	for rows.Next() {
		row, err := rows.Columns()
		if err != nil {
			fmt.Println(err)
		}
		for _, colCell := range row {
			listOfPeople = append(listOfPeople, colCell)
		}
	}
	if err = rows.Close(); err != nil {
		fmt.Println(err)
	}

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
	elapsed := time.Since(start).Seconds()
	fmt.Printf("Levou %f tempo e processou sincronamente %d linhas.\n", elapsed, len(listOfPeople))
	fmt.Printf("%s e a primeira pessoa da lista.\n%s e a ultima essoa da lista.\n", listOfPeople[0], listOfPeople[len(listOfPeople)-1])
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
	elapsed := time.Since(start).Seconds()

	fmt.Printf("Levou %f segundos e processou em paralelo %d linhas.\n", elapsed, len(listOfPeople))
	fmt.Printf("%s e a primeira pessoa da lista.\n%s e a ultima essoa da lista.\n", listOfPeople[0], listOfPeople[len(listOfPeople)-1])
}
