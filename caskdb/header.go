package caskdb

import (
	"bytes"
	"encoding/binary"
)

type Header struct {
	Checksum  uint32
	Meta      uint8
	Timestamp uint32
	KeySize   uint32
	ValSize   uint32
}

func (h *Header) EncodeHeader(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, h.Checksum)
	binary.Write(buf, binary.LittleEndian, h.Meta)
	binary.Write(buf, binary.LittleEndian, h.Timestamp)
	binary.Write(buf, binary.LittleEndian, h.KeySize)
	binary.Write(buf, binary.LittleEndian, h.ValSize)
	return err
}

func (h *Header) DecodeHeader(buf []byte) error {
	err := binary.Read(bytes.NewBuffer(buf[0:4]), binary.LittleEndian, &h.Checksum)
	binary.Read(bytes.NewBuffer(buf[4:5]), binary.LittleEndian, &h.Meta)
	binary.Read(bytes.NewBuffer(buf[5:9]), binary.LittleEndian, &h.Timestamp)
	binary.Read(bytes.NewBuffer(buf[9:13]), binary.LittleEndian, &h.KeySize)
	binary.Read(bytes.NewBuffer(buf[13:17]), binary.LittleEndian, &h.ValSize)
	return err
}
