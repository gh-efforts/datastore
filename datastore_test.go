package datastore

import (
	"bytes"
	"context"
	"github.com/bitrainforest/datastore/store/fs"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

var ds *Datastore

func TestNew(t *testing.T) {
	dir, err := os.MkdirTemp("", "datastore-test")
	if err != nil {
		panic(err)
	}
	st, err := fs.New(dir)
	require.NoError(t, err)
	require.NotNil(t, st)
	ds = New("test", st)
}

func TestDatastore_WriteMessage(t *testing.T) {
	err := ds.WriteMessage(context.TODO(), 100, []byte("test"))
	require.NoError(t, err)
}

func TestDatastore_ReadMessage(t *testing.T) {
	msg, err := ds.ReadMessage(context.TODO(), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("test"), msg)
}

func TestDatastore_WriteCompacted(t *testing.T) {
	err := ds.WriteCompacted(context.TODO(), 100, []byte("test"))
	require.NoError(t, err)
}

func TestDatastore_ReadCompacted(t *testing.T) {
	msg, err := ds.ReadCompacted(context.TODO(), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("test"), msg)
}

func TestDatastore_WriteImplicit(t *testing.T) {
	err := ds.WriteImplicit(context.TODO(), 100, []byte("test"))
	require.NoError(t, err)
}

func TestDatastore_ReadImplicit(t *testing.T) {
	msg, err := ds.ReadImplicit(context.TODO(), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("test"), msg)
}

func TestDatastore_WriteSnapshot(t *testing.T) {
	err := ds.WriteSnapshot(context.TODO(), 100, bytes.NewReader([]byte("test")))
	require.NoError(t, err)
}

func TestDatastore_ReadSnapshot(t *testing.T) {
	reader, err := ds.ReadSnapshot(context.TODO(), 100)
	require.NoError(t, err)
	defer reader.Close() // nolint: errcheck
	msg, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, []byte("test"), msg)
}
