package ftdc

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCSVIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmp, err := ioutil.TempDir("", "ftdc-csv-")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(tmp)) }()

	t.Run("Write", func(t *testing.T) {
		iter := ReadChunks(ctx, bytes.NewBuffer(newChunk(10)))
		out := &bytes.Buffer{}
		err := WriteCSV(ctx, iter, out)
		require.NoError(t, err)

		lines := strings.Split(out.String(), "\n")
		assert.Len(t, lines, 12)
	})
	t.Run("ResuseIterPass", func(t *testing.T) {
		iter := ReadChunks(ctx, bytes.NewBuffer(newChunk(10)))
		err := DumpCSV(ctx, iter, filepath.Join(tmp, "dump"))
		require.NoError(t, err)
		err = DumpCSV(ctx, iter, filepath.Join(tmp, "dump"))
		require.NoError(t, err)
	})
	t.Run("Dump", func(t *testing.T) {
		iter := ReadChunks(ctx, bytes.NewBuffer(newChunk(10)))
		err := DumpCSV(ctx, iter, filepath.Join(tmp, "dump"))
		require.NoError(t, err)
	})
	t.Run("DumpMixed", func(t *testing.T) {
		iter := ReadChunks(ctx, bytes.NewBuffer(newMixedChunk(10)))
		err := DumpCSV(ctx, iter, filepath.Join(tmp, "dump"))
		require.NoError(t, err)
	})
	t.Run("WriteWithSchemaChange", func(t *testing.T) {
		iter := ReadChunks(ctx, bytes.NewBuffer(newMixedChunk(10)))
		out := &bytes.Buffer{}
		err := WriteCSV(ctx, iter, out)

		require.Error(t, err)
	})
}
