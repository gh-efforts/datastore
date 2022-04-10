package datastore

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const splitWidth = 10000

type KeyType string

const (
	Snapshot  KeyType = "snapshot"
	Compacted KeyType = "compacted"
	Implicit  KeyType = "implicit"
	Messages  KeyType = "messages"
)

var KeyTypeMap = map[KeyType]struct{}{}

func init() {
	KeyTypeMap[Snapshot] = struct{}{}
	KeyTypeMap[Compacted] = struct{}{}
	KeyTypeMap[Implicit] = struct{}{}
	KeyTypeMap[Messages] = struct{}{}
}

type Key struct {
	height uint64
	ft     KeyType
}

func NewKey(height uint64, ft KeyType) *Key {
	return &Key{
		height: height,
		ft:     ft,
	}
}

func ParseKey(key string) (*Key, error) {
	var (
		kt     KeyType
		prefix string
		name   string
	)

	sp := strings.Split(key, "/")

	if len(sp) == 2 {
		kt = KeyType(sp[0])
		name = sp[1]
	} else if len(sp) == 3 {
		kt = KeyType(sp[0])
		prefix = sp[1]
		name = sp[2]
	} else {
		return nil, errors.New("invalid key")
	}

	if _, ok := KeyTypeMap[kt]; !ok {
		return nil, errors.New("invalid key type")
	}
	fmt.Println(string(kt), prefix, name)
	suf := strings.Split(name, ".")
	if len(suf) != 2 {
		return nil, errors.New("invalid key suffix")
	}
	h, err := strconv.ParseUint(suf[0], 10, 64)
	if err != nil {
		return nil, err
	}

	switch kt {
	case Messages, Implicit:
		if suf[1] != "json" {
			return nil, fmt.Errorf("invalid key suffix: %s, expect: %s", suf[1], "json")
		}
	case Snapshot, Compacted:
		if suf[1] != "car" {
			return nil, fmt.Errorf("invalid key suffix: %s, expect: %s", suf[1], "car")
		}
	}

	if kt == Snapshot {
		if prefix != "" {
			return nil, fmt.Errorf("invalid key prefix: %s, expect: %s", prefix, "")
		}
	} else {
		p := strconv.FormatUint(h/splitWidth, 10)
		if prefix != p {
			return nil, fmt.Errorf("invalid key prefix: %s, expect: %s", prefix, p)
		}
	}

	return NewKey(h, kt), nil
}

func (k *Key) Value(splitPrefix bool) string {
	return KeyBuilder(k.ft, k.height, splitPrefix)
}

func (k *Key) Default() string {
	if k.ft == Snapshot {
		return KeyBuilder(k.ft, k.height, false)
	}
	return KeyBuilder(k.ft, k.height, true)
}

func (k *Key) Type() KeyType {
	return k.ft
}

func (k *Key) Height() uint64 {
	return k.height
}

func KeyBuilder(kt KeyType, height uint64, splitPrefix bool) string {
	var suffix string
	switch kt {
	case Snapshot, Compacted:
		suffix = ".car"
	case Implicit, Messages:
		suffix = ".json"
	}
	if !splitPrefix {
		return fmt.Sprintf("%s/%d%s", kt, height, suffix)
	}
	return fmt.Sprintf("%s/%d/%d%s", kt, height/splitWidth, height, suffix)
}
