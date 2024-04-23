package caskdb

import (
	"bytes"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
)

func Purge() {
	nonDeletedRecords := make(map[string]Record)
	for _, record := range ReadRecords() {
		if record.IsDeleted() {
			delete(nonDeletedRecords, record.Key)
		} else {
			nonDeletedRecords[record.Key] = record
		}
	}

	records := make([]Record, 0)

	for _, record := range nonDeletedRecords {
		log.WithFields(log.Fields{
			"Key":       record.Key,
			"Value":     record.Value,
			"isDeleted": record.IsDeleted(),
		}).Info()
		records = append(records, record)
	}
	err := WriteRecords(records)
	if err != nil {
		log.Fatal(err)
	}
}

func WriteRecords(records []Record) error {
	dir := os.Getenv("DB_DIR")
	filename := dir + "/" + "file"
	f, _ := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	defer f.Close()
	err := f.Truncate(0)
	buf := new(bytes.Buffer)
	for _, record := range records {
		err := record.EncodeKV(buf)
		if err != nil {
			return err
		}
	}
	_, err = f.Write(buf.Bytes())
	return err
}

func ReadRecords() []Record {
	dir := os.Getenv("DB_DIR")
	file := os.Getenv("DB_CURRENT_FILE")
	filename := dir + "/" + file
	f, _ := os.Open(filename)
	defer f.Close()

	buf := make([]byte, 1048576)
	n, e := f.Read(buf)
	var arr []Record
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

		arr = append(arr, r)
		offset = offset + HeaderSize + h.KeySize + h.ValSize
	}
	return arr
}
