# datastore

[![Lint Code Base](https://github.com/bitrainforest/datastore/actions/workflows/linter.yml/badge.svg)](https://github.com/bitrainforest/datastore/actions/workflows/linter.yml)

Unified API for multiple backend stores.

Backends: `S3`, `Minio`, `FS`

It used by [BitRainforest](https://github.com/bitrainforest).

## Install

```bash
go get -u github.com/bitrainforest/datastore
```

## Usage

```go
package main

import (
 "bytes"
 "context"
 "fmt"
 "github.com/bitrainforest/datastore"
 "github.com/bitrainforest/datastore/store/fs"
 "github.com/bitrainforest/datastore/store/s3"
)

func main() {
 
 //st, err := fs.New("/tmp") file system
 
 // minio, s3 compatible
 st, err := s3.New("endpoint", "accessKey", "secretKey", false)
 if err != nil {
  panic(err)
 }
 ds := datastore.New(st, "bucket")

 // read message
 v, err := ds.ReadMessage(context.TODO(), 12)
 if err != nil {
  panic(err)
 }
 fmt.Println(string(v))

 //write snapshot 
 err = ds.WriteSnapshot(context.TODO(), 100, bytes.NewReader([]byte("hello world")))
 if err != nil {
  panic(err)
 }
}
```
