package s3

import (
	"bytes"
	"context"
	"github.com/bitrainforest/datastore/store"
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

func makeStore(t *testing.T) store.Store {
	t.Helper()

	st, err := New("127.0.0.1:9000", "minioadmin", "minioadmin", false)
	require.NoError(t, err)

	return st
}

func TestNew(t *testing.T) {
	st := makeStore(t)

	require.NotNil(t, st)
}

func Test_CreateBucket(t *testing.T) {
	st := makeStore(t)

	err := st.CreateBucket(context.Background(), testBucket)

	require.NoError(t, err)
}

func Test_Write(t *testing.T) {
	st := makeStore(t)
	ctx := context.TODO()

	err := st.Write(ctx, testBucket, testFilename, []byte(testFileValue))
	require.NoError(t, err)
}

func Test_Read(t *testing.T) {
	st := makeStore(t)
	ctx := context.TODO()

	data, err := st.Read(ctx, testBucket, testFilename)
	require.NoError(t, err)
	require.Equal(t, testFileValue, string(data))
}

func Test_WriteSteam(t *testing.T) {
	st := makeStore(t)
	ctx := context.TODO()

	err := st.WriteStream(ctx, testBucket, testStreamFilename, bytes.NewReader([]byte(testFileValue)))
	require.NoError(t, err)
}

func Test_ReadStream(t *testing.T) {
	st := makeStore(t)
	ctx := context.TODO()

	reader, err := st.ReadStream(ctx, testBucket, testStreamFilename)
	require.NoError(t, err)

	defer reader.Close()
	b, err := ioutil.ReadAll(reader)

	require.NoError(t, err)
	require.Equal(t, testFileValue, string(b))
}

func Test_Delete(t *testing.T) {
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
