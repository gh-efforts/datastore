package store

import (
	"context"
	"io"
)

type Store interface {
	CreateBucket(ctx context.Context, bucket string) error
	Read(ctx context.Context, bucket, key string) ([]byte, error)
	ReadStream(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	Write(ctx context.Context, bucket, key string, value []byte) error
	WriteStream(ctx context.Context, bucket, key string, value io.Reader) error
	Delete(ctx context.Context, bucket, key string) error
}
