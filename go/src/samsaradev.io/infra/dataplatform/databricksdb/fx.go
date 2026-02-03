package databricksdb

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"time"

	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/infra/config"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/security/secrets"
)

type DB struct {
	*sql.DB
}

func NewDatabricksDB(appConfig *config.AppConfig, lc fx.Lifecycle, databricksApiToken string, jdbcHttpPath string) (*DB, error) {
	if appConfig.IsTest {
		// Not supported in test.
		return nil, nil
	}

	u, err := url.Parse(appConfig.DatabricksE2UrlBase)
	if err != nil {
		return nil, oops.Wrapf(err, "parse: %s", appConfig.DatabricksE2UrlBase)
	}
	u.User = url.UserPassword("token", databricksApiToken)
	u.Path = "/" + strings.TrimPrefix(jdbcHttpPath, "/")

	dsn := u.String()
	db, err := sql.Open(dbName, dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(50)

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return oops.Wrapf(db.Close(), "")
		},
	})

	return &DB{DB: db}, nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(func(appConfig *config.AppConfig, lc fx.Lifecycle) (*DB, error) {
		databricksGqlUserApiTokenResp, err := secrets.DefaultService().GetSecretValueFromKey(secrets.DatabricksGqlUserApiToken)
		if err != nil {
			return nil, oops.Wrapf(err, "Could not get %s", secrets.DatabricksGqlUserApiToken)
		}

		return NewDatabricksDB(appConfig, lc, databricksGqlUserApiTokenResp.StringValue, appConfig.DatabricksJdbcHttpPath)
	})

}
