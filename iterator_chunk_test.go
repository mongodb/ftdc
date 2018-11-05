package ftdc

import (
	"context"
	"os"
	"testing"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunkIterator(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real integration test for runtime")
	}

	t.Parallel()

	t.Run("CanceledContexts", func(t *testing.T) {
		file, err := os.Open("metrics.ftdc")
		require.NoError(t, err)
		defer file.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		iter := ReadChunks(ctx, file)
		assert.False(t, iter.Next(ctx))

		assert.NoError(t, readDiagnostic(ctx, file, make(chan *bson.Document)))
	})
}
