package datastore

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
)

const (
	testFilename       = "test.txt"
	testStreamFilename = "stream-test.txt"
	testFileValue      = "hello world!"
	testBucket         = "test"
)

func makeStore(t *testing.T) Store {
	t.Helper()

	st, err := NewS3("127.0.0.1:9000", "minioadmin", "minioadmin", false)
	require.NoError(t, err)

	return st
}

func TestS3New(t *testing.T) {
	st := makeStore(t)

	require.NotNil(t, st)
}

func TestS3_CreateBucket(t *testing.T) {
	st := makeStore(t)

	err := st.CreateBucket(context.Background(), testBucket)

	require.NoError(t, err)
}

func TestS3_Write(t *testing.T) {
	st := makeStore(t)
	ctx := context.TODO()

	err := st.Write(ctx, testBucket, testFilename, []byte(testFileValue))
	require.NoError(t, err)
}

func TestS3_Read(t *testing.T) {
	st := makeStore(t)
	ctx := context.TODO()

	data, err := st.Read(ctx, testBucket, testFilename)
	require.NoError(t, err)
	require.Equal(t, testFileValue, string(data))
}

func TestS3_WriteSteam(t *testing.T) {
	st := makeStore(t)
	ctx := context.TODO()

	err := st.WriteStream(ctx, testBucket, testStreamFilename, bytes.NewReader([]byte(testFileValue)))
	require.NoError(t, err)
}

func TestS3_ReadStream(t *testing.T) {
	st := makeStore(t)
	ctx := context.TODO()

	reader, err := st.ReadStream(ctx, testBucket, testStreamFilename)
	require.NoError(t, err)

	defer reader.Close()
	b, err := ioutil.ReadAll(reader)

	require.NoError(t, err)
	require.Equal(t, testFileValue, string(b))
}

func TestS3_Delete(t *testing.T) {
	st := makeStore(t)
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
