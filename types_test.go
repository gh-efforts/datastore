package datastore

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewKey(t *testing.T) {
	tests := []struct {
		ft        KeyType
		height    uint64
		expectKey *Key
	}{
		{
			ft:     Messages,
			height: 100012,
			expectKey: &Key{
				height: 100012,
				ft:     Messages,
			},
		},
		{
			ft:     Implicit,
			height: 250101,
			expectKey: &Key{
				height: 250101,
				ft:     Implicit,
			},
		},
		{
			ft:     Compacted,
			height: 1250101,
			expectKey: &Key{
				height: 1250101,
				ft:     Compacted,
			},
		},
		{
			ft:     Snapshot,
			height: 2250101,
			expectKey: &Key{
				height: 2250101,
				ft:     Snapshot,
			},
		},
	}

	for _, test := range tests {
		t.Run(string(test.ft), func(t *testing.T) {
			key := NewKey(test.height, test.ft)
			assert.Equal(t, test.expectKey, key)
		})
	}
}

func TestParseKey(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		expectError bool
		expectKey   *Key
	}{
		{
			name:        "right message",
			key:         "messages/10/100012.json",
			expectError: false,
			expectKey: &Key{
				height: 100012,
				ft:     Messages,
			},
		},
		{
			name:        "wrong message type",
			key:         "messages/10/100012.car",
			expectError: true,
			expectKey:   nil,
		},
		{
			name:        "wrong message prefix",
			key:         "messages/11/100012.json",
			expectError: true,
			expectKey:   nil,
		},

		{
			name:        "right implicit",
			key:         "implicit/230/2300012.json",
			expectError: false,
			expectKey: &Key{
				height: 2300012,
				ft:     Implicit,
			},
		},
		{
			name:        "wrong implicit type",
			key:         "implicit/230/2300012.car",
			expectError: true,
			expectKey:   nil,
		},
		{
			name:        "wrong implicit prefix",
			key:         "implicit/232/2300012.json",
			expectError: true,
			expectKey:   nil,
		},
		{
			name:        "right compacted",
			key:         "compacted/123/1230012.car",
			expectError: false,
			expectKey: &Key{
				height: 1230012,
				ft:     Compacted,
			},
		},
		{
			name:        "wrong message type",
			key:         "compacted/123/1230012.json",
			expectError: true,
			expectKey:   nil,
		},
		{
			name:        "wrong message prefix",
			key:         "compacted/120/1230012.car",
			expectError: true,
			expectKey:   nil,
		},
		{
			name:        "right snapshot",
			key:         "snapshot/100012.car",
			expectError: false,
			expectKey: &Key{
				height: 100012,
				ft:     Snapshot,
			},
		},
		{
			name:        "wrong snapshot type",
			key:         "snapshot/100012.json",
			expectError: true,
			expectKey:   nil,
		},
		{
			name:        "wrong snapshot prefix",
			key:         "snapshot/11/100012.car",
			expectError: true,
			expectKey:   nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key, err := ParseKey(test.key)
			if test.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expectKey, key)
			}
		})
	}
}
