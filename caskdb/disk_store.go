package caskdb

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
)

type DiskStore struct {
	f    *os.File
	addr map[string]KeyEntry
}

func isFileExists(fileName string) bool {
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

func NewDiskStore(fileName string) (*DiskStore, error) {
	var f *os.File
	var e error
	var d *DiskStore

	if !isFileExists(fileName) {
		f, e = os.Create(fileName)
		d = &DiskStore{f, make(map[string]KeyEntry)}
	} else {
		f, e = os.OpenFile(fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.ModeAppend)

		if e != nil {
			return nil, e
		}
		d = &DiskStore{f, make(map[string]KeyEntry)}
		d.load()
	}
	return d, nil
}

func (d *DiskStore) load() {
	buf := make([]byte, 1024)
	n, e := d.f.Read(buf)
	if e != nil && e != io.EOF {
		panic(e)
	}

	offset := uint32(0)
	for offset < uint32(n) {
		h := Header{}
		r := Record{}
		err := h.DecodeHeader(buf[offset : offset+HeaderSize])
		err = r.DecodeKV(buf[offset : offset+HeaderSize+h.KeySize+h.ValSize])

		if err != nil {
			panic(err)
		}

		if h.Meta&1 == 0 {
			d.addr[r.Key] = KeyEntry{h.Timestamp, offset, HeaderSize + h.KeySize + h.ValSize}
		} else {
			delete(d.addr, r.Key)
		}

		offset = offset + HeaderSize + h.KeySize + h.ValSize
	}
}

func (d *DiskStore) Get(key string) string {
	keyDir, ok := d.addr[key]
	if !ok {
		return ""
	}
	seekVal := keyDir.position
	size := keyDir.totalSize
	_, err := d.f.Seek(int64(seekVal), 0)
	if err != nil {
		panic(err)
	}
	buf := make([]byte, size)
	_, err = io.ReadAtLeast(d.f, buf, int(size))
	if err != nil && err != io.EOF {
		panic(err)
	}
	r := Record{}
	err = r.DecodeKV(buf)
	if err != nil {
		panic(err)
	}

	return r.Value
}

func (d *DiskStore) Set(key string, value string) {
	stat, _ := d.f.Stat()
	r := NewRecord(key, value, false)

	buf := new(bytes.Buffer)
	err := r.EncodeKV(buf)

	if err != nil {
		panic(err)
	}

	d.addr[key] = KeyEntry{r.Header.Timestamp, uint32(stat.Size()), r.RecordSize}
	_, err = d.f.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}
}

func (d *DiskStore) Delete(key string) {
	r := NewRecord(key, "", true)

	buf := new(bytes.Buffer)
	err := r.EncodeKV(buf)

	if err != nil {
		panic(err)
	}

	delete(d.addr, key)

	_, err = d.f.Write(buf.Bytes())
	if err != nil {
		panic(err)
	}
}

func (d *DiskStore) Close() bool {
	err := d.f.Close()
	return err == nil
}
