package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/dapr/components-contrib/state"
	"github.com/dapr/kit/logger"
)

type ClickHouseStateStore struct {
	client     clickhouse.Conn
	tableName  string
	logger     logger.Logger
	metadata   *clickHouseMetadata
	features   []state.Feature
}

type clickHouseMetadata struct {
	ClickHouseURL string `mapstructure:"clickhouseURL"`
	DatabaseName  string `mapstructure:"databaseName"`
	TableName     string `mapstructure:"tableName"`
	Username      string `mapstructure:"username"`
	Password      string `mapstructure:"password"`
}

// NewClickHouseStateStore returns a new ClickHouse state store.
func NewClickHouseStateStore(logger logger.Logger) state.Store {
	return &ClickHouseStateStore{
		logger:   logger,
		features: []state.Feature{state.FeatureETag, state.FeatureTransactional},
	}
}

func (c *ClickHouseStateStore) Init(metadata state.Metadata) error {
	c.metadata = &clickHouseMetadata{}
	err := metadata.Decode(c.metadata)
	if err != nil {
		return fmt.Errorf("failed to decode metadata: %w", err)
	}

	if c.metadata.ClickHouseURL == "" {
		return fmt.Errorf("clickhouseURL is required")
	}

	if c.metadata.DatabaseName == "" {
		return fmt.Errorf("databaseName is required")
	}

	if c.metadata.TableName == "" {
		return fmt.Errorf("tableName is required")
	}

	c.tableName = c.metadata.TableName

	// Connect to ClickHouse
	ctx := context.Background()
	c.client, err = clickhouse.Open(&clickhouse.Options{
		Addr: []string{c.metadata.ClickHouseURL},
		Auth: clickhouse.Auth{
			Database: c.metadata.DatabaseName,
			Username: c.metadata.Username,
			Password: c.metadata.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Debug: true,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Create table if it doesn't exist
	err = c.createTable(ctx)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func (c *ClickHouseStateStore) createTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			key String,
			value String,
			etag String,
			created_at DateTime64(3),
			updated_at DateTime64(3)
		) ENGINE = MergeTree()
		ORDER BY (key)
	`, c.tableName)

	return c.client.Exec(ctx, query)
}

func (c *ClickHouseStateStore) Features() []state.Feature {
	return c.features
}

func (c *ClickHouseStateStore) Delete(ctx context.Context, req *state.DeleteRequest) error {
	query := fmt.Sprintf("ALTER TABLE %s DELETE WHERE key = ?", c.tableName)
	return c.client.Exec(ctx, query, req.Key)
}

func (c *ClickHouseStateStore) BulkDelete(ctx context.Context, req []state.DeleteRequest) error {
	for _, r := range req {
		err := c.Delete(ctx, &r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ClickHouseStateStore) Get(ctx context.Context, req *state.GetRequest) (*state.GetResponse, error) {
	var value string
	var etag string
	var updatedAt time.Time

	query := fmt.Sprintf("SELECT value, etag, updated_at FROM %s WHERE key = ?", c.tableName)
	err := c.client.QueryRow(ctx, query, req.Key).Scan(&value, &etag, &updatedAt)
	if err != nil {
		if err == clickhouse.ErrNoRows {
			return &state.GetResponse{}, nil
		}
		return nil, err
	}

	var data interface{}
	err = json.Unmarshal([]byte(value), &data)
	if err != nil {
		return nil, err
	}

	return &state.GetResponse{
		Data:     []byte(value),
		ETag:     &etag,
		Metadata: map[string]string{"updated_at": updatedAt.Format(time.RFC3339)},
	}, nil
}

func (c *ClickHouseStateStore) BulkGet(ctx context.Context, req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	// Not implemented
	return false, nil, nil
}

func (c *ClickHouseStateStore) Set(ctx context.Context, req *state.SetRequest) error {
	value, err := json.Marshal(req.Value)
	if err != nil {
		return err
	}

	now := time.Now()
	etag := fmt.Sprintf("%d", now.UnixNano())

	query := fmt.Sprintf(`
		INSERT INTO %s (key, value, etag, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		value = VALUES(value),
		etag = VALUES(etag),
		updated_at = VALUES(updated_at)
	`, c.tableName)

	return c.client.Exec(ctx, query, req.Key, string(value), etag, now, now)
}

func (c *ClickHouseStateStore) BulkSet(ctx context.Context, req []state.SetRequest) error {
	for _, r := range req {
		err := c.Set(ctx, &r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *ClickHouseStateStore) Multi(ctx context.Context, request *state.TransactionalStateRequest) error {
	for _, op := range request.Operations {
		switch op.Operation {
		case state.Upsert:
			setReq, ok := op.Request.(state.SetRequest)
			if !ok {
				return fmt.Errorf("invalid request type for upsert operation")
			}
			err := c.Set(ctx, &setReq)
			if err != nil {
				return err
			}
		case state.Delete:
			delReq, ok := op.Request.(state.DeleteRequest)
			if !ok {
				return fmt.Errorf("invalid request type for delete operation")
			}
			err := c.Delete(ctx, &delReq)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported operation: %s", op.Operation)
		}
	}
	return nil
}

func (c *ClickHouseStateStore) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
} 