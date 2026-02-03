package dataplatformtestinternalmodels

import (
	"context"
	"database/sql"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/infra/config/databaseconfig"
	"samsaradev.io/infra/thunder/sqlgen"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/dbtools/dbhelpers"
	"samsaradev.io/infra/dbtools/dbhelpers/queryinterceptors"
)

type DataplatformtestinternalModels struct {
	*Bridges

	db        *sql.DB
	sqlgenDB  *sqlgen.DB
	isTestEnv bool

	dbConfig databaseconfig.DatabaseConfig
}

func newDataplatformtestinternalModels(config *config.AppConfig, dbConfig databaseconfig.DatabaseConfig, lc fx.Lifecycle) (*DataplatformtestinternalModels, error) {
	db, err := dbhelpers.OpenMySQLWithLifecycle(lc, dbConfig)
	if err != nil {
		return nil, oops.Wrapf(err, "cannot open db")
	}
	sqlgenDB := sqlgen.NewDB(db, sqlgenSchema)

	// Add default query interceptors.
	sqlgenDB = sqlgenDB.
		WithQueryColumnValuesInterceptors(queryinterceptors.DefaultQueryColumnValuesInterceptors()...).
		WithQueryFilterInterceptors(queryinterceptors.DefaultQueryFilterInterceptors()...)

	// In testing, we set our db connection to panic on un-indexed queries.
	if config.Params.IsTest {
		sqlgenDB, err = sqlgenDB.WithPanicOnNoIndex()
		if err != nil {
			return nil, err
		}
	}

	models := &DataplatformtestinternalModels{
		Bridges:   &Bridges{},
		isTestEnv: dbConfig.IsTest,
		db:        db,
		sqlgenDB:  sqlgenDB,
		dbConfig:  dbConfig,
	}

	models.initializeBridges()

	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			models.Shutdown()
			return nil
		},
	})

	return models, nil
}

func (m *DataplatformtestinternalModels) DB() *sql.DB {
	return m.db
}

func (m *DataplatformtestinternalModels) SqlgenDB() *sqlgen.DB {
	return m.sqlgenDB
}

func (m *DataplatformtestinternalModels) WithSqlgenTx(ctx context.Context) (context.Context, *sql.Tx, error) {
	return m.sqlgenDB.WithTx(ctx)
}

func (m *DataplatformtestinternalModels) Shutdown() {
	if m.db == nil {
		return
	}

	dbhelpers.CloseMySQL(m.db, m.dbConfig)
	m.db = nil
}
