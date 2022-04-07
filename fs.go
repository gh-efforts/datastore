package datastore

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
)

type FS struct {
	path string
}

func NewFS(path string) (Store, error) {
	if err := initPath(path); err != nil {
		return nil, err
	}
	return &FS{path: path}, nil
}

func (s *FS) CreateBucket(_ context.Context, bucket string) error {
	bucketPath := path.Join(s.path, bucket)
	return os.Mkdir(bucketPath, 0755)
}

func (s *FS) Read(_ context.Context, bucket, key string) ([]byte, error) {
	file, err := os.Open(path.Join(s.path, bucket, key))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return ioutil.ReadAll(file)
}

// ReadStream is read as a stream, which the caller must close when finished.
func (s *FS) ReadStream(_ context.Context, bucket, key string) (io.ReadCloser, error) {
	file, err := os.Open(path.Join(s.path, bucket, key))
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (s *FS) Write(_ context.Context, bucket, key string, value []byte) error {
	return ioutil.WriteFile(path.Join(s.path, bucket, key), value, 0644)
}

func (s *FS) WriteStream(_ context.Context, bucket, key string, value io.Reader) error {
	file, err := os.Create(path.Join(s.path, bucket, key))
	if err != nil {
		return err
	}
	_, err = io.Copy(file, value)
	return err
}

func (s *FS) Delete(_ context.Context, bucket, key string) error {
	return os.Remove(path.Join(s.path, bucket, key))
}

var _ Store = (*FS)(nil)

func initPath(path string) error {
	s, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(path, 0755); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	if !s.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}

	return nil
}
