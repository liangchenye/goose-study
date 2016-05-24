package main

import (
	"database/sql"
	"fmt"
	"github.com/guregu/null/zero"
	"strings"
)

type Namespace struct {
	ID      int
	Name    string
	Version string
}

type Layer struct {
	LayerID     int
	NamespaceID int
}

const (
	listOldNamespace = `SELECT id, name FROM Namespace`
	listNewNamespace = `SELECT id, name, version FROM Namespace`

	upLayerTable   = `ALTER TABLE Layer DROP COLUMN namespace_id`
	downLayerTable = `ALTER TABLE Layer ADD namespace_id INT NULL REFERENCES Namespace;
			CREATE INDEX ON Layer (namespace_id)`
	downLayerData = `UPDATE Layer set namespace_id = $2 where id = $1`
	listNewLayer  = `SELECT id FROM Layer`
	listOldLayer  = `SELECT id, namespace_id FROM Layer`

	createLayerNamespaceTable = `CREATE TABLE IF NOT EXISTS LayerNamespace (
				id SERIAL PRIMARY KEY,
				layer_id INT NOT NULL REFERENCES Layer ON DELETE CASCADE,
				namespace_id INT NOT NULL REFERENCES Namespace ON DELETE CASCADE,
			        UNIQUE (layer_id, namespace_id));`
	//			CREATE INDEX ON LayerNamespace (layer_id);
	//			CREATE INDEX ON LayerNamespace (layer_id, namespace_id);`
	dropLayerNamespaceTable  = `DROP TABLE IF EXISTS LayerNamespace`
	insertLayerNamespaceData = `INSERT INTO LayerNamespace(layer_id, namespace_id) VALUES($1, $2)`
	listLayerNamespace       = `SELECT layer_id, namespace_id FROM LayerNamespace`

	upNamespaceTable   = `ALTER TABLE Namespace ADD version VARCHAR(128) NULL`
	downNamespaceTable = `ALTER TABLE Namespace DROP COLUMN version`
	upNamespaceData    = `UPDATE Namespace SET name = $2, version = $3 WHERE id = $1`
	downNamespaceData  = `UPDATE Namespace SET name = $2 WHERE id = $1`
)

const (
	insertOldNamespace = `INSERT INTO Namespace(name) VALUES($1)`
	insertOldLayer     = `INSERT INTO Layer(name, engineversion, parent_id, namespace_id, created_at)
	    VALUES($1, $2, $3, $4, CURRENT_TIMESTAMP)`
)

func testListNewLayer(txn *sql.Tx) {
	rows, err := txn.Query(listLayerNamespace)
	if err != nil {
		fmt.Println("Fail to get layers: ", err)
		return
	} else {
		fmt.Println(rows)
	}
	defer rows.Close()

	for rows.Next() {
		var layer Layer
		if err = rows.Scan(&layer.LayerID, &layer.NamespaceID); err != nil {
			fmt.Println("listLayerNamespace.Scan: ", err)
		} else {
			fmt.Println(layer)
		}
	}
}

func testListOldLayer(txn *sql.Tx) {
	rows, _ := txn.Query(listOldLayer)
	defer rows.Close()

	for rows.Next() {
		var layer Layer

		rows.Scan(&layer.LayerID, &layer.NamespaceID)
		fmt.Println(layer)
	}
}

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

func testInitOldLayer(txn *sql.Tx) {

	var parentID zero.Int

	for id := 1; id < 5; id++ {
		_, err := txn.Exec(insertOldLayer, fmt.Sprintf("%d", id), 1, parentID, id)
		if err != nil {
			fmt.Println("get err in iol", err)
		}
	}
}

// Up is executed when this migration is applied
func Up_20160524162211(txn *sql.Tx) {
	//	testInitOld(txn)
	fmt.Println("up start")
	//	Up_20160524162211_Namespace(txn)

	//	testInitOldLayer(txn)
	//
	//	testListNew(txn)
	testListOldLayer(txn)
	Up_20160524162211_LayerNamespace(txn)
	testListNewLayer(txn)
	fmt.Println("up end")
}

// Down is executed when this migration is rolled back
func Down_20160524162211(txn *sql.Tx) {
	//	testInitNew(txn)

	fmt.Println("down start")
	//	Down_20160524162211_Namespace(txn)
	//	testListOld(txn)
	Down_20160524162211_LayerNamespace(txn)
	fmt.Println("down end")
}

func Up_20160524162211_LayerNamespace(txn *sql.Tx) {
	fmt.Println("up ln")
	if output, err := txn.Exec(createLayerNamespaceTable); err != nil {
		fmt.Println("failed to create layer namespace ", err)
		return
	} else {
		fmt.Println("ok to c ln ", output)
	}

	rows, err := txn.Query(listOldLayer)
	if err != nil {
		fmt.Println("Fail to get layers: ", err)
		return
	}
	defer rows.Close()

	layerNamespace := make(map[string]Layer)
	for rows.Next() {
		var layer Layer

		err = rows.Scan(&layer.LayerID, &layer.NamespaceID)
		if err != nil {
			fmt.Println("listOldNamespace.Scan: ", err)
			return
		}
		layerNamespace[fmt.Sprintf("%d#%d", layer.LayerID, layer.NamespaceID)] = layer
	}

	for _, layer := range layerNamespace {
		if _, err = txn.Exec(insertLayerNamespaceData, layer.LayerID, layer.NamespaceID); err != nil {
			fmt.Println("err in insert lnd", err)
			return
		}
	}

	if _, err := txn.Exec(upLayerTable); err != nil {
		fmt.Println("fail to uplayer table", err)
	}

}

func Down_20160524162211_LayerNamespace(txn *sql.Tx) {
	if _, err := txn.Exec(downLayerTable); err != nil {
		fmt.Println("failed to down layer ", err)
	}

	rows, err := txn.Query(listLayerNamespace)
	if err != nil {
		fmt.Println("Fail to get layers: ", err)
		return
	}
	defer rows.Close()

	layerNamespace := make(map[int]int)
	for rows.Next() {
		var layer Layer

		if err = rows.Scan(&layer.LayerID, &layer.NamespaceID); err != nil {
			fmt.Println("listLayerNamespace.Scan: ", err)
			return
		}
		layerNamespace[layer.LayerID] = layer.NamespaceID
	}

	for layerID, namespaceID := range layerNamespace {
		txn.Exec(downLayerData, layerID, namespaceID)
	}

	if _, err := txn.Exec(dropLayerNamespaceTable); err != nil {
		fmt.Println("failed to drop layer namespace ", err)
	}
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
