package client

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

func EncodeMessage(key []byte, value []byte) ([]byte, error) {
	w := new(bytes.Buffer)
	var err error
	// TODO: MagicByte
	err = binary.Write(w, binary.BigEndian, int8(0))
	if err != nil {
		return nil, err
	}

	// TODO: Attributes
	err = binary.Write(w, binary.BigEndian, int8(0))
	if err != nil {
		return nil, err
	}

	if key == nil {
		err = binary.Write(w, binary.BigEndian, int32(-1))
		if err != nil {
			return nil, err
		}
	} else {
		err = binary.Write(w, binary.BigEndian, int32(len(key)))
		if err != nil {
			return nil, err
		}
		_, err = w.Write(key)
		if err != nil {
			return nil, err
		}
	}

	if value == nil {
		err = binary.Write(w, binary.BigEndian, int32(-1))
		if err != nil {
			return nil, err
		}
	} else {
		err = binary.Write(w, binary.BigEndian, int32(len(value)))
		if err != nil {
			return nil, err
		}
		_, err = w.Write(value)
		if err != nil {
			return nil, err
		}
	}
	csum := crc32.ChecksumIEEE(w.Bytes())
	res := new(bytes.Buffer)
	err = binary.Write(res, binary.BigEndian, csum)
	if err != nil {
		return nil, err
	}
	_, err = res.Write(w.Bytes())
	if err != nil {
		return nil, err
	}
	return res.Bytes(), nil
}
