package main

import (
	"fmt"
	"path/filepath"
	"runtime"

	"bitbucket.org/liamstask/goose/lib/goose"
)

func main() {
	source := "postgres://postgres@localhost"
	migrate(source)
}

func migrate(source string) error {

	_, filename, _, _ := runtime.Caller(1)
	migrationDir := filepath.Join(filepath.Dir(filename), "/db/migrations/")
	conf := &goose.DBConf{
		MigrationsDir: migrationDir,
		Driver: goose.DBDriver{
			Name:    "postgres",
			OpenStr: source,
			Import:  "github.com/lib/pq",
			Dialect: &goose.PostgresDialect{},
		},
	}

	// Determine the most recent revision available from the migrations folder.
	target, err := goose.GetMostRecentDBVersion(conf.MigrationsDir)
	if err != nil {
		return fmt.Errorf("pgsql: could not get most recent migration: %v", err)
	}

	// Run migrations.
	err = goose.RunMigrations(conf, conf.MigrationsDir, target)
	if err != nil {
		return fmt.Errorf("pgsql: an error occured while running migrations: %v", err)
	}

	fmt.Println("database migration ran successfully")
	return nil
}
