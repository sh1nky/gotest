package config

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

func GetMySqlDB() (db *sql.DB, err error) {
	dbDriver := "mysql"
	dbUser := "root"
	dbPass := "253014qwezxcA."
	dbName := "test"
	db, err = sql.Open(dbDriver, dbUser+":"+dbPass+"@/"+dbName)
	return
}
