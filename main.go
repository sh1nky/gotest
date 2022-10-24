package main

import (
	"fmt"
	"strconv"
	"sync"
	"test/config"
	"test/entities"
	"test/models"
	"time"
)

var operationModel *models.OperationModel

func main() {
	start := time.Now()
	countThreads := getCountThreads()
	result, err := getPreparedOperationModel()
	operationModel = result

	if err != nil {
		return
	}

	var wg sync.WaitGroup

	ch := make(chan entities.Operation)

	for i := 0; i < countThreads; i++ {
		wg.Add(1)
		go worker(ch, &wg, i+1)
	}

	result2, err2 := operationModel.Db.Query("SELECT * FROM operations")
	if err2 != nil {
		return
	}

	for result2.Next() {
		var Operation entities.Operation

		err := result2.Scan(
			&Operation.Id,
			&Operation.OperationId,
			&Operation.Side,
			&Operation.AccountNumber,
			&Operation.CustomerAccount,
			&Operation.Amount,
			&Operation.Status,
			&Operation.Comment,
			&Operation.CreatedAt,
			&Operation.UpdatedAt)

		if err != nil {
			return
		} else {
			ch <- Operation
		}
	}

	close(ch)
	wg.Wait()
	fmt.Printf("Execution time: %.2f s", time.Now().Sub(start).Seconds())

	return
}

func getCountThreads() int {
	var countThreads int

	fmt.Print("Введите количество потоков: ")
	fmt.Scan(&countThreads)

	return countThreads
}

func getPreparedOperationModel() (*models.OperationModel, error) {
	db, err := config.GetMySqlDB()

	if err != nil {
		fmt.Println(err)
		return nil, err
	} else {
		operationModel := &models.OperationModel{
			Db: db,
		}

		rowsAffected, err2 := operationModel.PrepareOperations()
		if err2 != nil {
			fmt.Println(err2)
			return nil, err2
		} else {
			fmt.Println("Rows Affected:", rowsAffected)
			return operationModel, nil
		}
	}
}

func worker(ch chan entities.Operation, wg *sync.WaitGroup, id int) {
	for operation := range ch {
		operation.Status = models.StatusCompleted
		operation.Comment = strconv.Itoa(id)
		_, err := operationModel.Update(operation)

		if err != nil {
			fmt.Println(err)
			ch <- operation
		}
	}

	wg.Done()
}
