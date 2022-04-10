package s3

import (
	"bytes"
	"context"
	"github.com/bitrainforest/datastore/store"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"io/ioutil"
)

type S3 struct {
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	secure          bool
	client          *minio.Client
}

func (s *S3) CreateBucket(ctx context.Context, bucket string) error {
	return s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
}

func (s *S3) Read(ctx context.Context, bucket, key string) ([]byte, error) {
	reader, err := s.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer reader.Close() // nolint: errcheck

	return ioutil.ReadAll(reader)
}

// ReadStream is read as a stream, which the caller must close when finished.
func (s *S3) ReadStream(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	return s.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
}

func (s *S3) Write(ctx context.Context, bucket, key string, value []byte) error {
	_, err := s.client.PutObject(ctx, bucket, key, bytes.NewReader(value), int64(len(value)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (s *S3) WriteStream(ctx context.Context, bucket, key string, value io.Reader) error {
	_, err := s.client.PutObject(ctx, bucket, key, value, -1, minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (s *S3) Delete(ctx context.Context, bucket, key string) error {
	return s.client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
}

func New(endpoint string, accessKeyID string, secretAccessKey string, secure bool) (store.Store, error) {

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: secure,
	})

	if err != nil {
		return nil, err
	}

	return &S3{
		endpoint:        endpoint,
		accessKeyID:     accessKeyID,
		secretAccessKey: secretAccessKey,
		secure:          secure,
		client:          client,
	}, nil
}

var _ store.Store = (*S3)(nil)
