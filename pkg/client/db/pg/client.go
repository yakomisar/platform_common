package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"

	"platform_common/pkg/client/db"
)

type pgClient struct {
	masterDB db.DB
}

func New(ctx context.Context, dsn string) (db.Client, error) {
	dbc, err := pgxpool.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the db %v", err)
	}

	return &pgClient{
		masterDB: &pg{
			dbc: dbc,
		},
	}, nil
}

func (c *pgClient) DB() db.DB {
	return c.masterDB
}

func (c *pgClient) Close() error {
	if c.masterDB != nil {
		c.masterDB.Close()
	}

	return nil
}
