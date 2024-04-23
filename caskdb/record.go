package caskdb

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"time"
)

const HeaderSize = 17

type KeyEntry struct {
	timestamp uint32
	position  uint32
	totalSize uint32
}

type Record struct {
	Header     Header
	Key        string
	Value      string
	RecordSize uint32
}

func NewRecord(key, value string, isTombStone bool) *Record {
	meta := uint8(0)
	if isTombStone {
		meta = meta | 1
	}

	h := Header{
		Meta:      meta,
		Timestamp: uint32(time.Now().UnixMilli()),
		KeySize:   uint32(len([]byte(key))),
		ValSize:   uint32(len([]byte(value))),
	}

	r := Record{Header: h, Key: key, Value: value, RecordSize: HeaderSize + h.KeySize + h.ValSize}
	h.Checksum = r.CalculateCheckSum()

	return &r
}

func (r *Record) EncodeKV(buf *bytes.Buffer) error {
	err := r.Header.EncodeHeader(buf)
	buf.Write([]byte(r.Key))
	buf.Write([]byte(r.Value))
	return err
}

func (r *Record) DecodeKV(buf []byte) error {
	headerSlice := buf[:HeaderSize]
	err := r.Header.DecodeHeader(headerSlice)
	r.Key = string(buf[HeaderSize : HeaderSize+r.Header.KeySize])
	r.Value = string(buf[HeaderSize+r.Header.KeySize : HeaderSize+r.Header.KeySize+r.Header.ValSize])
	r.RecordSize = HeaderSize + r.Header.KeySize + r.Header.ValSize
	return err
}

func (r *Record) CalculateCheckSum() uint32 {
	// encode header
	headerBuf := new(bytes.Buffer)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.Meta)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.Timestamp)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.KeySize)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.ValSize)

	// encode kv
	kvBuf := append([]byte(r.Key), []byte(r.Value)...)

	buf := append(headerBuf.Bytes(), kvBuf...)
	return crc32.ChecksumIEEE(buf)
}

func (r *Record) IsDeleted() bool {
	return r.Header.Meta&1 == 1
}
