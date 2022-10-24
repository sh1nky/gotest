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
	var wg sync.WaitGroup

	countThreads := getCountThreads()
	result, err := getPreparedOperationModel()

	if err != nil {
		return
	}

	operationModel = result
	Operations, err2 := operationModel.GetOperations()

	if err2 != nil {
		return
	}

	finishForWorker := 0
	ceil := len(Operations) % countThreads
	imprecision := 0

	for i := 0; i < countThreads; i++ {
		wg.Add(1)

		if i < ceil {
			imprecision++
		}

		startForWorker := finishForWorker
		finishForWorker = len(Operations)/countThreads*(i+1) + imprecision

		go worker(Operations[startForWorker:finishForWorker], &wg, i+1)
	}

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
			fmt.Println("Operations were repaired:", rowsAffected)
			return operationModel, nil
		}
	}
}

func worker(Operations []entities.Operation, wg *sync.WaitGroup, id int) {

	_, err := operationModel.LockOperations(Operations[0].Id, Operations[len(Operations)-1].Id)

	if err != nil {
		fmt.Println(err)
	}

	for _, operation := range Operations {
		operation.Status = models.StatusCompleted
		operation.Comment = strconv.Itoa(id)
		_, err := operationModel.Update(operation)

		if err != nil {
			fmt.Println(err)
		}
	}

	defer wg.Done()
}
