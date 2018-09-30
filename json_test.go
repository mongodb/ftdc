package ftdc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mongodb/grip/message"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectJSONOptions(t *testing.T) {
	for _, test := range []struct {
		name  string
		valid bool
		opts  CollectJSONOptions
	}{
		{
			name:  "Nil",
			valid: false,
		},
		{
			name:  "FileWithIoReader",
			valid: false,
			opts: CollectJSONOptions{
				FileName:    "foo",
				InputSource: &bytes.Buffer{},
			},
		},
		{
			name:  "JustIoReader",
			valid: true,
			opts: CollectJSONOptions{
				InputSource: &bytes.Buffer{},
			},
		},
		{
			name:  "JustFile",
			valid: true,
			opts: CollectJSONOptions{
				FileName: "foo",
			},
		},
		{
			name:  "FileWithFollow",
			valid: true,
			opts: CollectJSONOptions{
				FileName: "foo",
				Follow:   true,
			},
		},
		{
			name:  "ReaderWithFollow",
			valid: false,
			opts: CollectJSONOptions{
				InputSource: &bytes.Buffer{},
				Follow:      true,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.valid {
				assert.NoError(t, test.opts.validate())
			} else {
				assert.Error(t, test.opts.validate())
			}
		})
	}
}

func makeSysInfoDocuments(num int) ([][]byte, error) {
	out := [][]byte{}

	for i := 0; i < num; i++ {
		info := message.CollectSystemInfo().(*message.SystemInfo)
		info.Base = message.Base{} // avoid collecting data from the base package.
		data, err := json.Marshal(info)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		out = append(out, data)
	}

	return out, nil
}

func writeStream(docs [][]byte, writer io.Writer) error {
	for _, doc := range docs {
		_, err := writer.Write(doc)
		if err != nil {
			return err
		}

		_, err = writer.Write([]byte("\n"))
		if err != nil {
			return err
		}
	}
	return nil
}

func TestCollectJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real integration test for runtime")
	}
	// t.Parallel()

	dir, err := ioutil.TempDir("build", "ftdc-")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
	}()

	hundredDocs, err := makeSysInfoDocuments(100)
	require.NoError(t, err)

	t.Run("SingleReaderIdealCase", func(t *testing.T) {
		buf := &bytes.Buffer{}

		err = writeStream(hundredDocs, buf)
		require.NoError(t, err)

		reader := bytes.NewReader(buf.Bytes())

		opts := CollectJSONOptions{
			OutputFilePrefix: filepath.Join(dir, fmt.Sprintf("json.%d.%s",
				os.Getpid(),
				time.Now().Format("2006-01-02.15-04-05"))),
			ChunkSizeBytes: 10 * 1000,
			FlushInterval:  100 * time.Millisecond,
			InputSource:    reader,
		}

		err = CollectJSONStream(ctx, opts)
		assert.NoError(t, err)
	})
	t.Run("SingleReaderBotchedDocument", func(t *testing.T) {
		buf := &bytes.Buffer{}
		docs, err := makeSysInfoDocuments(10)
		require.NoError(t, err)

		docs[2] = docs[len(docs)-1][1:] // break the last document docuemnt

		err = writeStream(docs, buf)
		require.NoError(t, err)

		reader := bytes.NewReader(buf.Bytes())

		opts := CollectJSONOptions{
			OutputFilePrefix: filepath.Join(dir, fmt.Sprintf("json.%d.%s",
				os.Getpid(),
				time.Now().Format("2006-01-02.15-04-05"))),
			ChunkSizeBytes: 10 * 1000,
			FlushInterval:  10 * time.Millisecond,
			InputSource:    reader,
		}

		err = CollectJSONStream(ctx, opts)
		assert.Error(t, err)
	})
	t.Run("ReadFromFile", func(t *testing.T) {
		fn := filepath.Join(dir, "json-read-file-one")
		f, err := os.Create(fn)
		require.NoError(t, err)

		require.NoError(t, writeStream(hundredDocs, f))
		require.NoError(t, f.Close())

		opts := CollectJSONOptions{
			OutputFilePrefix: filepath.Join(dir, fmt.Sprintf("json.%d.%s",
				os.Getpid(),
				time.Now().Format("2006-01-02.15-04-05"))),
			ChunkSizeBytes: 10 * 1000,
			FlushInterval:  500 * time.Millisecond,
			FileName:       fn,
		}

		err = CollectJSONStream(ctx, opts)
		assert.NoError(t, err)
	})
	t.Run("FollowFile", func(t *testing.T) {
		fn := filepath.Join(dir, "json-read-file-two")
		f, err := os.Create(fn)
		require.NoError(t, err)

		go func() {
			time.Sleep(10 * time.Millisecond)
			require.NoError(t, writeStream(hundredDocs, f))
			require.NoError(t, f.Close())
		}()

		ctx, cancel = context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()
		opts := CollectJSONOptions{
			OutputFilePrefix: filepath.Join(dir, fmt.Sprintf("json.%d.%s",
				os.Getpid(),
				time.Now().Format("2006-01-02.15-04-05"))),
			ChunkSizeBytes: 10 * 1000,
			FlushInterval:  500 * time.Millisecond,
			FileName:       fn,
			Follow:         true,
		}

		err = CollectJSONStream(ctx, opts)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "operation aborted")
	})
}
