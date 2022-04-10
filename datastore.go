package datastore

import (
	"context"
	"github.com/bitrainforest/datastore/store"
	"io"
)

type Datastore struct {
	bucket string
	store  store.Store
}

func New(bucket string, store store.Store) *Datastore {
	return &Datastore{bucket, store}
}

func (d *Datastore) ReadMessage(ctx context.Context, height uint64) ([]byte, error) {
	return d.store.Read(ctx, d.bucket, KeyBuilder(Messages, height, true))
}

func (d *Datastore) WriteMessage(ctx context.Context, height uint64, data []byte) error {
	return d.store.Write(ctx, d.bucket, KeyBuilder(Messages, height, true), data)
}

func (d *Datastore) ReadCompacted(ctx context.Context, height uint64) ([]byte, error) {
	return d.store.Read(ctx, d.bucket, KeyBuilder(Compacted, height, true))
}
func (d *Datastore) WriteCompacted(ctx context.Context, height uint64, data []byte) error {
	return d.store.Write(ctx, d.bucket, KeyBuilder(Compacted, height, true), data)
}

func (d *Datastore) ReadImplicit(ctx context.Context, height uint64) ([]byte, error) {
	return d.store.Read(ctx, d.bucket, KeyBuilder(Implicit, height, true))
}

func (d *Datastore) WriteImplicit(ctx context.Context, height uint64, data []byte) error {
	return d.store.Write(ctx, d.bucket, KeyBuilder(Implicit, height, true), data)
}

func (d *Datastore) ReadSnapshot(ctx context.Context, height uint64) (io.ReadCloser, error) {
	return d.store.ReadStream(ctx, d.bucket, KeyBuilder(Snapshot, height, false))
}

func (d *Datastore) WriteSnapshot(ctx context.Context, height uint64, data io.Reader) error {
	return d.store.WriteStream(ctx, d.bucket, KeyBuilder(Snapshot, height, false), data)
}
