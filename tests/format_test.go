package test

import (
	"bytes"
	"github.com/amrable/go-caskdb/caskdb"
	"testing"
	"time"
)

func Test_encodeHeader(t *testing.T) {
	tests := []*caskdb.Header{
		{10, 1, 10, 10, 10},
		{0, 0, 0, 0, 0},
		{10000, 1, 10000, 10000, 1000},
	}
	for _, tt := range tests {
		newBuf := new(bytes.Buffer)
		//encode the header
		tt.EncodeHeader(newBuf)

		//encoded header should be 14bytes
		if len(newBuf.Bytes()) != caskdb.HeaderSize {
			t.Errorf("Invalid encode: expected header size = %v, got = %v", caskdb.HeaderSize, len(newBuf.Bytes()))
		}

		//decode the header
		result := &caskdb.Header{}
		result.DecodeHeader(newBuf.Bytes())

		if result.Timestamp != tt.Timestamp {
			t.Errorf("EncodeHeader() timestamp = %v, want %v", result.Timestamp, tt.Timestamp)
		}
		if result.KeySize != tt.KeySize {
			t.Errorf("EncodeHeader() keySize = %v, want %v", result.KeySize, tt.KeySize)
		}
		if result.ValSize != tt.ValSize {
			t.Errorf("EncodeHeader() valueSize = %v, want %v", result.ValSize, tt.ValSize)
		}
	}
}

func Test_encodeKV(t *testing.T) {
	k1, v1 := "hello", "world"
	h1 := caskdb.Header{Timestamp: uint32(time.Now().Unix()), KeySize: uint32(len(k1)), ValSize: uint32(len(v1)), Meta: 0}
	r1 := caskdb.Record{Header: h1, Key: k1, Value: v1, RecordSize: caskdb.HeaderSize + +h1.KeySize + h1.ValSize}

	k2, v2 := "", ""
	h2 := caskdb.Header{Timestamp: uint32(time.Now().Unix()), KeySize: uint32(len(k2)), ValSize: uint32(len(v2)), Meta: 1}
	r2 := caskdb.Record{Header: h2, Key: k2, Value: v2, RecordSize: caskdb.HeaderSize + h2.KeySize + h2.ValSize}

	k3, v3 := "🔑", ""
	h3 := caskdb.Header{Timestamp: uint32(time.Now().Unix()), KeySize: uint32(len(k3)), ValSize: uint32(len(v3)), Meta: 0}
	r3 := caskdb.Record{Header: h3, Key: k3, Value: v3, RecordSize: caskdb.HeaderSize + h3.KeySize + h3.ValSize}

	tests := []caskdb.Record{r1, r2, r3}
	for _, tt := range tests {
		//encode the record
		buf := new(bytes.Buffer)
		tt.EncodeKV(buf)

		//encoded buffer size should be equal to headersize + keysize + valuesize
		expectedSize := tt.RecordSize
		if uint32(len(buf.Bytes())) != expectedSize {
			t.Errorf("EncodeKV() invalid encoding, expected size=%v, got=%v", expectedSize, uint32(len(buf.Bytes())))
		}

		//decode the record
		result := &caskdb.Record{}
		result.DecodeKV(buf.Bytes())

		if result.Header.Timestamp != tt.Header.Timestamp {
			t.Errorf("EncodeKV() timestamp = %v, want %v", result.Header.Timestamp, tt.Header.Timestamp)
		}
		if result.Key != tt.Key {
			t.Errorf("EncodeKV() key = %v, want %v", result.Key, tt.Key)
		}

		if result.Value != tt.Value {
			t.Errorf("encodeKV() value = %v, want %v", result.Value, tt.Value)
		}

	}
}
