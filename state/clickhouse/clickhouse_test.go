package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/dapr/components-contrib/state"
	"github.com/dapr/components-contrib/tests/conformance/state"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testClickHouseURL = "localhost:9000"
	testDatabaseName  = "dapr_test"
	testTableName     = "dapr_state_store"
	testUsername      = "default"
	testPassword      = "default"
)

func TestClickHouseStateStore(t *testing.T) {
	store := NewClickHouseStateStore(logger.NewLogger("test"))
	metadata := state.Metadata{
		Properties: map[string]string{
			"clickhouseURL": testClickHouseURL,
			"databaseName":  testDatabaseName,
			"tableName":     testTableName,
			"username":      testUsername,
			"password":      testPassword,
		},
	}

	err := store.Init(metadata)
	require.NoError(t, err)

	// Test Set
	key := "test-key"
	value := "test-value"
	err = store.Set(context.Background(), &state.SetRequest{
		Key:   key,
		Value: value,
	})
	require.NoError(t, err)

	// Test Get
	resp, err := store.Get(context.Background(), &state.GetRequest{
		Key: key,
	})
	require.NoError(t, err)
	assert.Equal(t, value, string(resp.Data))

	// Test Delete
	err = store.Delete(context.Background(), &state.DeleteRequest{
		Key: key,
	})
	require.NoError(t, err)

	// Verify deletion
	resp, err = store.Get(context.Background(), &state.GetRequest{
		Key: key,
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Data)
}

func TestConformance(t *testing.T) {
	store := NewClickHouseStateStore(logger.NewLogger("test"))
	metadata := state.Metadata{
		Properties: map[string]string{
			"clickhouseURL": testClickHouseURL,
			"databaseName":  testDatabaseName,
			"tableName":     testTableName,
			"username":      testUsername,
			"password":      testPassword,
		},
	}

	err := store.Init(metadata)
	require.NoError(t, err)

	// Clean up after tests
	defer func() {
		// Drop the test table
		ctx := context.Background()
		query := "DROP TABLE IF EXISTS " + testTableName
		chStore := store.(*ClickHouseStateStore)
		err := chStore.client.Exec(ctx, query)
		require.NoError(t, err)
	}()

	// Run conformance tests
	config := confstate.NewTestConfig("clickhouse", []string{
		"etag",
		"ttl",
		"transaction",
		"query",
		"delete-with-prefix",
	}, map[string]interface{}{
		"badEtag": "bad-etag",
	})
	confstate.ConformanceTests(t, store, config)
} 