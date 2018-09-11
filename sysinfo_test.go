package ftdc

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCollectSystemInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real integration test for runtime")
	}

	dir, err := ioutil.TempDir("build", "ftdc-")
	require.NoError(t, err)
	t.Parallel()

	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()

	opts := CollectSysInfoOptions{
		OutputFilePrefix: filepath.Join(dir, fmt.Sprintf("sysinfo.%d.%s",
			os.Getpid(),
			time.Now().Format("2006-01-02.15-04-05"))),
		ChunkSizeBytes:     10 * 1000,
		FlushInterval:      5 * time.Second,
		CollectionInterval: 100 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err = CollectSysInfo(ctx, opts)
	require.NoError(t, err)
}
