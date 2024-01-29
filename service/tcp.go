package service

import (
	"bufio"
	"fmt"
	"github.com/blight19/redis-safedel/pkg/encoder"
	"github.com/blight19/redis-safedel/pkg/proto"
	"github.com/spf13/viper"
	"net"
	"os"
	"path"
	"strings"
	"time"
)

// 01  00 0D 06
func HandleConnection(r net.Conn, name ...string) {
	if len(name) > 1 {
		panic("name too long")
	}
	defer r.Close()
	var fn string
	if len(name) == 1 {
		fn = name[0]
	} else {
		add := r.RemoteAddr().String()
		addrSplit := strings.Split(add, ":")[0]
		fn = addrSplit
	}
	en := NewRdb(fn)
	defer func() {
		fmt.Println("write end")
		if err := en.WriteEnd(); err != nil {
			fmt.Println(err)
		}
	}()
	reader := bufio.NewReader(r)

	for {
		read, err := proto.Read(reader)
		if err != nil {
			fmt.Println(1, err)
			return
		}
		s, err := proto.ReadStrings(read)
		if err != nil {
			fmt.Println(2, err)
			return
		}
		v := strings.ToUpper(s[0])
		switch v {
		case "SELECT":
			_, err := r.Write([]byte("+OK\r\n"))
			if err != nil {
				return
			}
		case "RESTORE":
			// restore a "0xxxx xxx xxx"
			en.Write([]byte(s[3][:1]))        //type
			en.Write([]byte{byte(len(s[1]))}) //key length
			en.Write([]byte(s[1]))            //key
			data := []byte(s[3])[1:]
			for i, i2 := range data {
				if data[i] == 9 && data[i+1] == 0 {
					break
				} else {
					en.Write([]byte{i2})
				}
			}
			_, err := r.Write([]byte("+OK\r\n"))
			if err != nil {
				return
			}
		default:
			_, err := r.Write([]byte("-ERR unknown command '" + s[0] + "'\r\n"))
			if err != nil {
				return
			}
		}
	}
}

func NewRdb(addr string) *encoder.Encoder {
	f := path.Join(viper.GetString("RDB_DIR"), addr)
	// if folder doesn't exist, create one
	if _, err := os.Stat(f); os.IsNotExist(err) {
		err = os.MkdirAll(f, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}
	filePath := path.Join(f, time.Now().Format("20060102150405")+".rdb")
	rdbFile, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	enc := encoder.NewEncoder(rdbFile)
	err = enc.WriteHeader()
	if err != nil {
		panic(err)
	}
	auxMap := map[string]string{
		"redis-ver":    "4.0.6",
		"redis-bits":   "64",
		"aof-preamble": "0",
	}
	for k, v := range auxMap {
		err = enc.WriteAux(k, v)
		if err != nil {
			panic(err)
		}
	}
	err = enc.WriteDBHeader(0, 0, 0)
	if err != nil {
		panic(err)
	}
	return enc
}
