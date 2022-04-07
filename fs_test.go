package datastore

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

var st Store

func init() {
	dir, err := os.MkdirTemp("", "datastore-fs-test")
	if err != nil {
		panic(err)
	}
	s, err := NewFS(dir)
	if err != nil {
		panic(err)
	}
	st = s
}

func TestFSNew(t *testing.T) {
	require.NotNil(t, st)
}

func TestFS_CreateBucket(t *testing.T) {
	err := st.CreateBucket(context.Background(), testBucket)
	require.NoError(t, err)
}

func TestFS_Write(t *testing.T) {
	ctx := context.TODO()

	err := st.Write(ctx, testBucket, testFilename, []byte(testFileValue))
	require.NoError(t, err)
}

func TestFS_Read(t *testing.T) {
	ctx := context.TODO()

	data, err := st.Read(ctx, testBucket, testFilename)
	require.NoError(t, err)
	require.Equal(t, testFileValue, string(data))
}

func TestFS_WriteSteam(t *testing.T) {
	ctx := context.TODO()

	err := st.WriteStream(ctx, testBucket, testStreamFilename, bytes.NewReader([]byte(testFileValue)))
	require.NoError(t, err)
}

func TestFS_ReadStream(t *testing.T) {
	ctx := context.TODO()

	reader, err := st.ReadStream(ctx, testBucket, testStreamFilename)
	require.NoError(t, err)

	defer reader.Close()
	b, err := ioutil.ReadAll(reader)

	require.NoError(t, err)
	require.Equal(t, testFileValue, string(b))
}

func TestFS_Delete(t *testing.T) {
	ctx := context.TODO()

	err := st.Delete(ctx, testBucket, testFilename)
	require.NoError(t, err)

	err = st.Delete(ctx, testBucket, testStreamFilename)
	require.NoError(t, err)

	_, err = st.Read(ctx, testBucket, testFilename)
	require.Error(t, err)

	_, err = st.Read(ctx, testBucket, testStreamFilename)
	require.Error(t, err)
}
