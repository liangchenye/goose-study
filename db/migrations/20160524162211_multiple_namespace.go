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
	listDownNamespace = `SELECT id, name FROM Namespace`
	listUpNamespace   = `SELECT id, name, version FROM Namespace`

	upLayerTable   = `ALTER TABLE Layer DROP COLUMN namespace_id`
	downLayerTable = `ALTER TABLE Layer ADD namespace_id INT NULL REFERENCES Namespace;
	  		  CREATE INDEX ON Layer (namespace_id)`
	downLayerData = `UPDATE Layer set namespace_id = $2 where id = $1`
	listUpLayer   = `SELECT id FROM Layer`
	listDownLayer = `SELECT id, namespace_id FROM Layer`

	createLayerNamespace = `CREATE TABLE IF NOT EXISTS LayerNamespace (
				  id SERIAL PRIMARY KEY,
				  layer_id INT NOT NULL REFERENCES Layer ON DELETE CASCADE,
				  namespace_id INT NOT NULL REFERENCES Namespace ON DELETE CASCADE,
			          UNIQUE (layer_id, namespace_id));
		  	        CREATE INDEX ON LayerNamespace (layer_id);
				CREATE INDEX ON LayerNamespace (layer_id, namespace_id);`
	dropLayerNamespace   = `DROP TABLE IF EXISTS LayerNamespace`
	insertLayerNamespace = `INSERT INTO LayerNamespace(layer_id, namespace_id) VALUES($1, $2)`
	listLayerNamespace   = `SELECT layer_id, namespace_id FROM LayerNamespace`

	upNamespaceTable   = `ALTER TABLE Namespace ADD version VARCHAR(128) NULL`
	downNamespaceTable = `ALTER TABLE Namespace DROP COLUMN version`
	upNamespaceData    = `UPDATE Namespace SET name = $2, version = $3 WHERE id = $1`
	downNamespaceData  = `UPDATE Namespace SET name = $2 WHERE id = $1`
)

const (
	insertDownNamespace = `INSERT INTO Namespace(name) VALUES($1)`
	insertDownLayer     = `INSERT INTO Layer(name, engineversion, parent_id, namespace_id, created_at)
	    VALUES($1, $2, $3, $4, CURRENT_TIMESTAMP)`
)

func testListLayerNamespace(txn *sql.Tx) {
	fmt.Println("testListLayerNamespace start")
	defer fmt.Println("testListLayerNamespace end")

	rows, err := txn.Query(listLayerNamespace)
	if err != nil {
		fmt.Println("listLayerNamespace: ", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var layer Layer
		if err = rows.Scan(&layer.LayerID, &layer.NamespaceID); err != nil {
			fmt.Println("listLayerNamespace.Scan: ", err)
			return
		} else {
			fmt.Println(layer)
		}
	}
}

func testListDownLayer(txn *sql.Tx) {
	fmt.Println("testListDownLayer start")
	defer fmt.Println("testListDownLayer end")

	rows, err := txn.Query(listDownLayer)
	if err != nil {
		fmt.Println("listDownLayer: ", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var layer Layer
		if err = rows.Scan(&layer.LayerID, &layer.NamespaceID); err != nil {
			fmt.Println("listDownLayer.Scan: ", err)
			return
		} else {
			fmt.Println(layer)
		}
	}
}

func testListUpNamespace(txn *sql.Tx) {
	fmt.Println("testListUpNamespace start")
	defer fmt.Println("testListUpNamespace end")

	rows, err := txn.Query(listUpNamespace)
	if err != nil {
		fmt.Println("listUpNamespace: ", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var namespace Namespace

		if err = rows.Scan(&namespace.ID, &namespace.Name, &namespace.Version); err != nil {
			fmt.Println("listUpNamespace.Scan: ", err)
			return
		} else {
			fmt.Println(namespace)
		}
	}
}

func testListDownNamespace(txn *sql.Tx) {
	fmt.Println("testListDown start")
	defer fmt.Println("testListDown end")

	rows, err := txn.Query(listDownNamespace)
	if err != nil {
		fmt.Println("listDownNamespace: ", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var namespace Namespace

		if err = rows.Scan(&namespace.ID, &namespace.Name); err != nil {
			fmt.Println("listDownNamespace: ", err)
			return
		} else {
			fmt.Println(namespace)
		}
	}
}

func testInitDownNamespace(txn *sql.Tx) {
	fmt.Println("init old testDB")

	nss := []Namespace{
		{Name: "abc:1"},
		{Name: "abc:2"},
		{Name: "xyz:1"},
	}
	for _, ns := range nss {
		if _, err := txn.Exec(insertDownNamespace, ns.Name); err != nil {
			fmt.Println("insertDownNamespace: ", err)
			return
		}
	}

}

func testInitDownLayer(txn *sql.Tx) {
	var parentID zero.Int

	for id := 1; id < 5; id++ {
		_, err := txn.Exec(insertDownLayer, fmt.Sprintf("%d", id), 1, parentID, id)
		if err != nil {
			fmt.Println("insertDownLayer: ", err)
			return
		}
	}
}

// Up is executed when this migration is applied
func Up_20160524162211(txn *sql.Tx) {
	//	testInitDownNamespace(txn)
	fmt.Println("up start")
	defer fmt.Println("up end")

	testListDownLayer(txn)
	testListDownNamespace(txn)
	if err := Up_20160524162211_Namespace(txn); err != nil {
		fmt.Println(err)
		return
	}
	if err := Up_20160524162211_LayerNamespace(txn); err != nil {
		fmt.Println(err)
		return
	}
	testListUpNamespace(txn)
	testListLayerNamespace(txn)
}

// Down is executed when this migration is rolled back
func Down_20160524162211(txn *sql.Tx) {
	//	testInitUp(txn)

	fmt.Println("down start")
	defer fmt.Println("down end")

	testListUpNamespace(txn)
	testListLayerNamespace(txn)

	if err := Down_20160524162211_Namespace(txn); err != nil {
		fmt.Println(err)
		return
	}
	if err := Down_20160524162211_LayerNamespace(txn); err != nil {
		fmt.Println(err)
		return
	}
	testListDownLayer(txn)
	testListDownNamespace(txn)
}

func Up_20160524162211_LayerNamespace(txn *sql.Tx) error {
	if _, err := txn.Exec(createLayerNamespace); err != nil {
		txn.Rollback()
		return fmt.Errorf("createLayerNamespace: %v", err)
	}

	rows, err := txn.Query(listDownLayer)
	if err != nil {
		txn.Rollback()
		return fmt.Errorf("listDownLayer: %v", err)
	}
	defer rows.Close()

	layerNamespace := make(map[string]Layer)
	for rows.Next() {
		var layer Layer

		err = rows.Scan(&layer.LayerID, &layer.NamespaceID)
		if err != nil {
			txn.Rollback()
			return fmt.Errorf("listDownNamespace.Scan: %v", err)
		}
		layerNamespace[fmt.Sprintf("%d#%d", layer.LayerID, layer.NamespaceID)] = layer
	}

	for _, layer := range layerNamespace {
		if _, err = txn.Exec(insertLayerNamespace, layer.LayerID, layer.NamespaceID); err != nil {
			txn.Rollback()
			return fmt.Errorf("insertLayerNamespace: %v", err)
		}
	}

	if _, err := txn.Exec(upLayerTable); err != nil {
		txn.Rollback()
		return fmt.Errorf("upLayerTable: %v", err)
	}

	return nil
}

func Down_20160524162211_LayerNamespace(txn *sql.Tx) error {
	if _, err := txn.Exec(downLayerTable); err != nil {
		txn.Rollback()
		return fmt.Errorf("downLayerTable: %v", err)
	}

	rows, err := txn.Query(listLayerNamespace)
	if err != nil {
		txn.Rollback()
		return fmt.Errorf("listLayerNamespace: %v", err)
	}
	defer rows.Close()

	layerNamespace := make(map[int]int)
	for rows.Next() {
		var layer Layer

		if err = rows.Scan(&layer.LayerID, &layer.NamespaceID); err != nil {
			txn.Rollback()
			return fmt.Errorf("listLayerNamespace.Scan: %v", err)
		}
		// Only keeps one of the namespaces
		layerNamespace[layer.LayerID] = layer.NamespaceID
	}

	for layerID, namespaceID := range layerNamespace {
		if _, err = txn.Exec(downLayerData, layerID, namespaceID); err != nil {
			txn.Rollback()
			return fmt.Errorf("downLayerData: %v", err)
		}
	}

	if _, err := txn.Exec(dropLayerNamespace); err != nil {
		txn.Rollback()
		return fmt.Errorf("dropLayerNamespace: %v", err)
	}

	return nil
}

func Up_20160524162211_Namespace(txn *sql.Tx) error {
	rows, err := txn.Query(listDownNamespace)
	if err != nil {
		return fmt.Errorf("Fail to get namespaces: %v", err)
	}
	defer rows.Close()

	var namespaces []Namespace
	for rows.Next() {
		var namespace Namespace

		err = rows.Scan(&namespace.ID, &namespace.Name)
		if err != nil {
			txn.Rollback()
			return fmt.Errorf("listDownNamespace.Scan: %v", err)
		}
		namespaces = append(namespaces, namespace)
	}

	if _, err = txn.Exec(upNamespaceTable); err != nil {
		txn.Rollback()
		return fmt.Errorf("upNamespaceTable: %v", err)
	}

	for _, namespace := range namespaces {
		var name string
		var version string
		if updated := strings.Split(namespace.Name, ":"); len(updated) != 2 {
			name = namespace.Name
			version = "unknown"
		} else {
			name = updated[0]
			version = updated[1]

		}

		if _, err = txn.Exec(upNamespaceData, namespace.ID, name, version); err != nil {
			txn.Rollback()
			return fmt.Errorf("upNamespaceData: %v", err)
		}
	}

	return nil
}

func Down_20160524162211_Namespace(txn *sql.Tx) error {
	rows, err := txn.Query(listUpNamespace)
	if err != nil {
		txn.Rollback()
		return fmt.Errorf("listUpNamespace: %v", err)
	}
	defer rows.Close()

	var namespaces []Namespace
	for rows.Next() {
		var namespace Namespace

		err = rows.Scan(&namespace.ID, &namespace.Name, &namespace.Version)
		if err != nil {
			txn.Rollback()
			return fmt.Errorf("listUpNamespace.Scan: %v", err)
		}
		namespaces = append(namespaces, namespace)
	}

	if _, err = txn.Exec(downNamespaceTable); err != nil {
		txn.Rollback()
		return fmt.Errorf("downNamespaceTable: %v", err)
	}

	for _, namespace := range namespaces {
		if _, err = txn.Exec(downNamespaceData, namespace.ID, namespace.Name+":"+namespace.Version); err != nil {
			txn.Rollback()
			return fmt.Errorf("downNamespaceData: %v", err)
		}
	}

	return nil
}
