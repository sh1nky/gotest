package models

import (
	"database/sql"
	"fmt"
	"test/entities"
	"time"
)

const StatusNew = "new"
const StatusLocked = "locked"
const StatusCompleted = "completed"

type OperationModel struct {
	Db *sql.DB
}

func (operationModel OperationModel) PrepareOperations() (int64, error) {
	result, err := operationModel.Db.Exec("UPDATE operations SET status = ?, comment = ?", StatusNew, "")
	if err != nil {
		return 0, err
	} else {
		return result.RowsAffected()
	}
}

func (operationModel OperationModel) Update(Operation entities.Operation) (int64, error) {
	sqlUpdate := `UPDATE operations SET 
                      operation_id = ?,
                      side = ?,
                      account_number = ?,
                      customer_account = ?,
                      amount = ?,
                      status = ?,
                      comment = ?,
                      updated_at = ? 
                  WHERE id = ?`

	result, err := operationModel.Db.Exec(sqlUpdate,
		Operation.OperationId,
		Operation.Side,
		Operation.AccountNumber,
		Operation.CustomerAccount,
		Operation.Amount,
		Operation.Status,
		Operation.Comment,
		time.Now(),
		Operation.Id)

	if err == nil {
		return result.RowsAffected()
	} else {
		return 0, err
	}
}

func (operationModel OperationModel) GetOperations() ([]entities.Operation, error) {
	var operations []entities.Operation

	result, err := operationModel.Db.Query("SELECT * FROM operations")
	if err != nil {
		return operations, err
	}

	for result.Next() {
		var Operation entities.Operation

		err := result.Scan(
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

		if err == nil {
			operations = append(operations, Operation)
		}
	}

	return operations, nil
}

func (operationModel OperationModel) GetOperation() (*entities.Operation, error) {
	var Operation entities.Operation
	sqlSelect := `SELECT * FROM operations WHERE comment = ? AND status = ? LIMIT 1`
	sqlUpdate := `UPDATE operations SET status = ?, comment = ? WHERE id = ?`

	result := operationModel.Db.QueryRow(sqlSelect, "", StatusNew)
	err := result.Scan(
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
		return &entities.Operation{}, err
	}

	result2, err := operationModel.Db.Exec(sqlUpdate, StatusLocked, "", Operation.Id)

	fmt.Println(result2)

	return &Operation, nil
}

func (operationModel OperationModel) LockOperations(from string, to string) (int64, error) {
	sqlUpdate := `UPDATE operations SET 
                      status = ?
                  WHERE id BETWEEN ? AND ?`

	result, err := operationModel.Db.Exec(sqlUpdate,
		StatusLocked,
		from,
		to,
	)

	if err != nil {
		return 0, err
	} else {
		return result.RowsAffected()
	}
}
