package caskdb

import (
	"errors"
	"io"
	"io/fs"
	"os"
)

// DiskStore is a Log-Structured Hash Table as described in the BitCask paper. We
// keep appending the data to a file, like a log. DiskStorage maintains an in-memory
// hash table called KeyDir, which keeps the row's location on the disk.
//
// The idea is simple yet brilliant:
//   - Write the record to the disk
//   - Update the internal hash table to point to that byte offset
//   - Whenever we get a read request, check the internal hash table for the address,
//     fetch that and return
//
// KeyDir does not store values, only their locations.
//
// The above approach solves a lot of problems:
//   - Writes are insanely fast since you are just appending to the file
//   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
//     storage, there could be 2-3 disk seeks
//
// However, there are drawbacks too:
//   - We need to maintain an in-memory hash table KeyDir. A database with a large
//     number of keys would require more RAM
//   - Since we need to build the KeyDir at initialisation, it will affect the startup
//     time too
//   - Deleted keys need to be purged from the file to reduce the file size
//
// Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf
//
// DiskStore provides two simple operations to get and set key value pairs. Both key
// and value need to be of string type, and all the data is persisted to disk.
// During startup, DiskStorage loads all the existing KV pair metadata, and it will
// throw an error if the file is invalid or corrupt.
//
// Note that if the database file is large, the initialisation will take time
// accordingly. The initialisation is also a blocking operation; till it is completed,
// we cannot use the database.
//
// Typical usage example:
//
//		store, _ := NewDiskStore("books.db")
//	   	store.Set("othello", "shakespeare")
//	   	author := store.Get("othello")
type DiskStore struct {
	f    *os.File
	addr map[string][2]int64
}

func isFileExists(fileName string) bool {
	// https://stackoverflow.com/a/12518877
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
		d = &DiskStore{f, make(map[string][2]int64)}
	} else {
		f, e = os.OpenFile(fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.ModeAppend)

		if e != nil {
			return nil, e
		}
		d = &DiskStore{f, make(map[string][2]int64)}
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

	offset := 0
	for offset < n {
		_, keySize, valueSize := DecodeHeader(buf[offset : offset+HeaderSize])
		_, key, _ := DecodeKV(buf[offset : offset+HeaderSize+int(keySize)+int(valueSize)])
		d.addr[key] = [2]int64{int64(offset), HeaderSize + int64(keySize) + int64(valueSize)}
		offset = offset + HeaderSize + int(keySize) + int(valueSize)
	}
}

func (d *DiskStore) Get(key string) string {
	pos, ok := d.addr[key]
	if !ok {
		return ""
	}
	seekVal := pos[0]
	size := pos[1]
	_, err := d.f.Seek(seekVal, 0)
	if err != nil {
		panic(err)
	}
	buf := make([]byte, size)
	_, err = io.ReadAtLeast(d.f, buf, int(size))
	if err != nil && err != io.EOF {
		panic(err)
	}

	_, _, value := DecodeKV(buf)
	return value
}

func (d *DiskStore) Set(key string, value string) {
	stat, _ := d.f.Stat()
	size, encoded := EncodeKV(0, key, value)
	d.addr[key] = [2]int64{stat.Size(), int64(size)}
	_, err := d.f.Write(encoded)
	if err != nil {
		panic(err)
	}
}

func (d *DiskStore) Close() bool {
	err := d.f.Close()
	return err == nil
}
