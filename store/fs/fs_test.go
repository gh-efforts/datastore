package fs

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/bitrainforest/datastore/store"
	"github.com/stretchr/testify/require"
)

const (
	testFilename       = "test.txt"
	testStreamFilename = "stream-test.txt"
	testFileValue      = "hello world!"
	testBucket         = "test"
)

var st store.Store

func init() {
	dir, err := os.MkdirTemp("", "datastore-fs-test")
	if err != nil {
		panic(err)
	}
	s, err := New(dir)
	if err != nil {
		panic(err)
	}
	st = s
}

func TestNew(t *testing.T) {
	require.NotNil(t, st)
}

func Test_CreateBucket(t *testing.T) {
	err := st.CreateBucket(context.Background(), testBucket)
	require.NoError(t, err)
}

func Test_Write(t *testing.T) {
	ctx := context.TODO()

	err := st.Write(ctx, testBucket, testFilename, []byte(testFileValue))
	require.NoError(t, err)
}

func Test_Read(t *testing.T) {
	ctx := context.TODO()

	data, err := st.Read(ctx, testBucket, testFilename)
	require.NoError(t, err)
	require.Equal(t, testFileValue, string(data))
}

func Test_WriteSteam(t *testing.T) {
	ctx := context.TODO()

	err := st.WriteStream(ctx, testBucket, testStreamFilename, bytes.NewReader([]byte(testFileValue)))
	require.NoError(t, err)
}

func Test_ReadStream(t *testing.T) {
	ctx := context.TODO()

	reader, err := st.ReadStream(ctx, testBucket, testStreamFilename)
	require.NoError(t, err)

	defer reader.Close() // nolint: errcheck
	b, err := ioutil.ReadAll(reader)

	require.NoError(t, err)
	require.Equal(t, testFileValue, string(b))
}

func Test_Delete(t *testing.T) {
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
