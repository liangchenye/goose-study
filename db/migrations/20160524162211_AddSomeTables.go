package main

import (
	"database/sql"
	"fmt"
	"strings"
)

type Namespace struct {
	ID      int
	Name    string
	Version string
}

const (
	listOldNamespace = `SELECT id, name FROM Namespace`
	listNewNamespace = `SELECT id, name, version FROM Namespace`

	upNamespaceTable   = `ALTER TABLE Namespace ADD version VARCHAR(128) NULL`
	upNamespaceData    = `UPDATE Namespace SET name = $2, version = $3 WHERE id = $1`
	downNamespaceData  = `UPDATE Namespace SET name = $2 WHERE id = $1`
	downNamespaceTable = `ALTER TABLE Namespace DROP COLUMN version`
)

const (
	insertOldNamespace = `INSERT INTO Namespace(name) VALUES($1)`
)

func testListNew(txn *sql.Tx) {
	rows, _ := txn.Query(listNewNamespace)
	defer rows.Close()

	for rows.Next() {
		var namespace Namespace

		rows.Scan(&namespace.ID, &namespace.Name, &namespace.Version)
		fmt.Println(namespace)
	}
}

func testListOld(txn *sql.Tx) {
	rows, _ := txn.Query(listOldNamespace)
	defer rows.Close()

	for rows.Next() {
		var namespace Namespace

		rows.Scan(&namespace.ID, &namespace.Name)
		fmt.Println(namespace)
	}
}

func testInitOld(txn *sql.Tx) {
	fmt.Println("init old testDB")
	nss := []Namespace{
		{Name: "abc:1"},
		{Name: "abc:2"},
		{Name: "xyz:1"},
	}
	for _, ns := range nss {
		txn.Exec(insertOldNamespace, ns.Name)
	}

}

// Up is executed when this migration is applied
func Up_20160524162211(txn *sql.Tx) {
	testInitOld(txn)
	fmt.Println("up start")
	Up_20160524162211_Namespace(txn)

	testListNew(txn)
	fmt.Println("up end")
}

// Down is executed when this migration is rolled back
func Down_20160524162211(txn *sql.Tx) {
	//	testInitNew(txn)

	fmt.Println("down start")
	Down_20160524162211_Namespace(txn)
	testListOld(txn)
	fmt.Println("down end")
}

func Up_20160524162211_Namespace(txn *sql.Tx) {
	rows, err := txn.Query(listOldNamespace)
	if err != nil {
		fmt.Println("Fail to get namespaces: ", err)
		return
	}
	defer rows.Close()

	var namespaces []Namespace
	for rows.Next() {
		var namespace Namespace

		err = rows.Scan(&namespace.ID, &namespace.Name)
		if err != nil {
			fmt.Println("listOldNamespace.Scan: ", err)
			return
		}
		namespaces = append(namespaces, namespace)
	}

	if _, err = txn.Exec(upNamespaceTable); err != nil {
		fmt.Println("upNamespaceTable: ", err)
		return
	}

	for _, namespace := range namespaces {
		updated := strings.Split(namespace.Name, ":")
		if len(updated) != 2 {
			txn.Exec(upNamespaceData, namespace.ID, namespace.Name, "unknown")
		} else {
			fmt.Println("upd ", namespace, updated[0], updated[1])
			_, err := txn.Exec(upNamespaceData, namespace.ID, updated[0], updated[1])
			if err != nil {
				fmt.Println("get err", err)
				return
			}
		}
	}
}

func Down_20160524162211_Namespace(txn *sql.Tx) {
	rows, err := txn.Query(listNewNamespace)
	if err != nil {
		fmt.Println("Fail to get namespaces: ", err)
		return
	}
	defer rows.Close()

	var namespaces []Namespace
	for rows.Next() {
		var namespace Namespace

		err = rows.Scan(&namespace.ID, &namespace.Name, &namespace.Version)
		if err != nil {
			fmt.Println("listNewNamespace.Scan: ", err)
			return
		}

		namespaces = append(namespaces, namespace)
	}

	if _, err = txn.Exec(downNamespaceTable); err != nil {
		fmt.Println("downNamespaceTable: ", err)
		return
	}

	for _, namespace := range namespaces {
		txn.Exec(downNamespaceData, namespace.ID, namespace.Name+":"+namespace.Version)
	}

}
