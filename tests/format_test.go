package test

import (
	"github.com/avinassh/go-caskdb/caskdb"
	"testing"
)

func Test_encodeHeader(t *testing.T) {
	tests := []struct {
		timestamp uint32
		keySize   uint32
		valueSize uint32
	}{
		{10, 10, 10},
		{0, 0, 0},
		{10000, 10000, 10000},
	}
	for _, tt := range tests {
		data := caskdb.EncodeHeader(tt.timestamp, tt.keySize, tt.valueSize)
		timestamp, keySize, valueSize := caskdb.DecodeHeader(data)
		if timestamp != tt.timestamp {
			t.Errorf("encodeHeader() timestamp = %v, want %v", timestamp, tt.timestamp)
		}
		if keySize != tt.keySize {
			t.Errorf("encodeHeader() keySize = %v, want %v", keySize, tt.keySize)
		}
		if valueSize != tt.valueSize {
			t.Errorf("encodeHeader() valueSize = %v, want %v", valueSize, tt.valueSize)
		}
	}
}

func Test_encodeKV(t *testing.T) {
	tests := []struct {
		timestamp uint32
		key       string
		value     string
		size      int
	}{
		{10, "hello", "world", caskdb.HeaderSize + 10},
		{0, "", "", caskdb.HeaderSize},
		{100, "🔑", "", caskdb.HeaderSize + 4},
	}
	for _, tt := range tests {
		size, data := caskdb.EncodeKV(tt.timestamp, tt.key, tt.value)
		timestamp, key, value := caskdb.DecodeKV(data)
		if timestamp != tt.timestamp {
			t.Errorf("encodeKV() timestamp = %v, want %v", timestamp, tt.timestamp)
		}
		if key != tt.key {
			t.Errorf("encodeKV() key = %v, want %v", key, tt.key)
		}
		if value != tt.value {
			t.Errorf("encodeKV() value = %v, want %v", value, tt.value)
		}
		if size != tt.size {
			t.Errorf("encodeKV() size = %v, want %v", size, tt.size)
		}
	}
}
