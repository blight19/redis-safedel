package encoder

import (
	"encoding/binary"
	"fmt"
	"github.com/blight19/redis-safedel/pkg/lzf"
	"hash"
	"hash/crc64"
	"math"
	"os"
	"strconv"
)

const (
	len6Bit      = 0
	len14Bit     = 1
	len32or64Bit = 2
	lenSpecial   = 3
	len32Bit     = 0x80
	len64Bit     = 0x81

	encodeInt8  = 0
	encodeInt16 = 1
	encodeInt32 = 2
	encodeLZF   = 3

	maxUint6  = 1<<6 - 1
	maxUint14 = 1<<14 - 1
	minInt24  = -1 << 23
	maxInt24  = 1<<23 - 1

	len14BitMask      byte = 0b01000000
	encodeInt8Prefix       = lenSpecial<<6 | encodeInt8
	encodeInt16Prefix      = lenSpecial<<6 | encodeInt16
	encodeInt32Prefix      = lenSpecial<<6 | encodeInt32
	encodeLZFPrefix        = lenSpecial<<6 | encodeLZF
)

const (
	opCodeIdle         = 248 /* LRU idle time. */
	opCodeFreq         = 249 /* LFU frequency. */
	opCodeAux          = 250 /* RDB aux field. */
	opCodeResizeDB     = 251 /* Hash table resize hint. */
	opCodeExpireTimeMs = 252 /* Expire time in milliseconds. */
	opCodeExpireTime   = 253 /* Old expire time in seconds. */
	opCodeSelectDB     = 254 /* DB number of the following keys. */
	opCodeEOF          = 255
)

type Encoder struct {
	writer   *os.File
	crc      hash.Hash64
	buffer   []byte
	compress bool
	existDB  map[uint]struct{} // store exist db size to avoid duplicate db
}

func NewEncoder(writer *os.File) *Encoder {
	return &Encoder{
		writer:   writer,
		crc:      crc64.New(crc64.MakeTable(crc64.ISO)),
		buffer:   make([]byte, 8),
		existDB:  make(map[uint]struct{}),
		compress: true,
	}
}

var rdbHeader = []byte("REDIS0003")

func (enc *Encoder) WriteHeader() error {

	err := enc.Write(rdbHeader)
	if err != nil {
		return err
	}

	return nil
}

// WriteDBHeader Write db index and resize db into rdb file
func (enc *Encoder) WriteDBHeader(dbIndex uint, keyCount, ttlCount uint64) error {

	if _, ok := enc.existDB[dbIndex]; ok {
		return fmt.Errorf("db %d existed", dbIndex)
	}
	enc.existDB[dbIndex] = struct{}{}
	err := enc.Write([]byte{opCodeSelectDB})
	if err != nil {
		return err
	}
	err = enc.writeLength(uint64(dbIndex))
	if err != nil {
		return err
	}
	err = enc.Write([]byte{opCodeResizeDB})
	if err != nil {
		return err
	}
	err = enc.writeLength(keyCount)
	if err != nil {
		return err
	}
	err = enc.writeLength(ttlCount)
	if err != nil {
		return err
	}
	return nil
}

// WriteAux writes aux object
func (enc *Encoder) WriteAux(key, value string) error {

	err := enc.Write([]byte{opCodeAux})
	if err != nil {
		return err
	}
	err = enc.writeString(key)
	if err != nil {
		return err
	}
	err = enc.writeString(value)
	if err != nil {
		return err
	}

	return nil
}

func (enc *Encoder) WriteEnd() error {
	defer enc.writer.Close()
	err := enc.Write([]byte{opCodeEOF})
	if err != nil {
		return err
	}
	checkSum := enc.crc.Sum(nil)
	_, err = enc.writer.Write(checkSum)
	if err != nil {
		return fmt.Errorf("write crc sum failed: %v", err)
	}
	enc.writer.Write([]byte{0x0a}) // Write LF
	return nil
}

func (enc *Encoder) Write(p []byte) error {
	_, err := enc.writer.Write(p)
	if err != nil {
		return fmt.Errorf("write data failed: %v", err)
	}
	_, err = enc.crc.Write(p)
	if err != nil {
		return fmt.Errorf("update crc table failed: %v", err)
	}
	return nil
}

func (enc *Encoder) writeString(s string) error {
	isInt, err := enc.tryWriteIntString(s)
	if err != nil {
		return err
	}
	if isInt {
		return nil
	}
	// Try LZF compression - under 20 bytes it's unable to compress even so skip it
	// see rdbSaveRawString at [rdb.c](https://github.com/redis/redis/blob/unstable/src/rdb.c#L449)
	if enc.compress && len(s) > 20 {
		err = enc.writeLZFString(s)
		if err == nil { // lzf may failed, while out > in
			return nil
		}
	}
	return enc.writeSimpleString(s)
}

func (enc *Encoder) tryWriteIntString(s string) (bool, error) {
	intVal, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		// is not a integer
		return false, nil
	}
	if intVal >= math.MinInt8 && intVal <= math.MaxInt8 {
		err = enc.Write([]byte{encodeInt8Prefix, byte(int8(intVal))})
	} else if intVal >= math.MinInt16 && intVal <= math.MaxInt16 {
		buf := enc.buffer[0:3]
		buf[0] = encodeInt16Prefix
		binary.LittleEndian.PutUint16(buf[1:], uint16(int16(intVal)))
		err = enc.Write(buf)
	} else if intVal >= math.MinInt32 && intVal <= math.MaxInt32 {
		buf := enc.buffer[0:5]
		buf[0] = encodeInt32Prefix
		binary.LittleEndian.PutUint32(buf[1:], uint32(int32(intVal)))
		err = enc.Write(buf)
	} else {
		// beyond int32 range, but within int64 range
		return false, nil
	}
	if err != nil {
		return true, err
	}
	return true, nil
}

func (enc *Encoder) writeLZFString(s string) error {
	out, err := lzf.Compress([]byte(s))
	if err != nil {
		return err
	}
	err = enc.Write([]byte{encodeLZFPrefix})
	if err != nil {
		return err
	}
	// Write compressed length
	err = enc.writeLength(uint64(len(out)))
	if err != nil {
		return err
	}
	// Write uncompressed length
	err = enc.writeLength(uint64(len(s)))
	if err != nil {
		return err
	}
	return enc.Write(out)
}

func (enc *Encoder) writeLength(value uint64) error {
	var buf []byte
	if value <= maxUint6 {
		// 00 + 6 bits of data
		enc.buffer[0] = byte(value)
		buf = enc.buffer[0:1]
	} else if value <= maxUint14 {
		enc.buffer[0] = byte(value>>8) | len14BitMask // high 6 bit and mask(0x40)
		enc.buffer[1] = byte(value)                   // low 8 bit
		buf = enc.buffer[0:2]
	} else if value <= math.MaxUint32 {
		buf = make([]byte, 5)
		buf[0] = len32Bit
		binary.BigEndian.PutUint32(buf[1:], uint32(value))
	} else {
		buf = make([]byte, 9)
		buf[0] = len64Bit
		binary.BigEndian.PutUint64(buf[1:], value)
	}
	return enc.Write(buf)
}

func (enc *Encoder) writeSimpleString(s string) error {
	err := enc.writeLength(uint64(len(s)))
	if err != nil {
		return err
	}
	return enc.Write([]byte(s))
}
