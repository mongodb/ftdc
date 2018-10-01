package ftdc

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectSystemInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real integration test for runtime")
	}

	//t.Parallel()

	dir, err := ioutil.TempDir("build", "ftdc-")
	require.NoError(t, err)

	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()

	t.Run("CollectData", func(t *testing.T) {
		opts := CollectSysInfoOptions{
			OutputFilePrefix: filepath.Join(dir, fmt.Sprintf("sysinfo.%d.%s",
				os.Getpid(),
				time.Now().Format("2006-01-02.15-04-05"))),
			ChunkSizeBytes:     10 * 100,
			FlushInterval:      2 * time.Second,
			CollectionInterval: 100 * time.Millisecond,
		}
		var cancel context.CancelFunc
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = CollectSysInfo(ctx, opts)
		require.NoError(t, err)
	})
	t.Run("ReadData", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		files, err := ioutil.ReadDir(dir)
		require.NoError(t, err)
		assert.True(t, len(files) >= 1)

		for idx, info := range files {
			t.Run(fmt.Sprintf("FileNo.%d", idx), func(t *testing.T) {
				path := filepath.Join(dir, info.Name())
				f, err := os.Open(path)
				require.NoError(t, err)
				defer f.Close()
				iter := ReadMetrics(ctx, f)
				counter := 0
				for iter.Next(ctx) {
					counter++
					doc := iter.Document()
					assert.NotNil(t, doc)
					assert.True(t, doc.Len() > 100)
				}
				assert.True(t, counter > 0)
				assert.NoError(t, iter.Err())
			})
		}
	})
}
