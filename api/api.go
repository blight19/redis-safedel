package api

import (
	"encoding/json"
	"fmt"
	"github.com/blight19/redis-safedel/config"
	"net/http"
	"os"
	"strings"
)

var rdbDir = config.RDBDir

func GetHttpMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/list", GetList)
	mux.HandleFunc("/hosts", GetHosts)
	fileServer := http.FileServer(http.Dir(config.RDBDir))
	mux.Handle("/rdb/", http.StripPrefix("/rdb/", fileServer))
	return mux
}

func writeJson(w http.ResponseWriter, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(data)
}

func GetList(w http.ResponseWriter, r *http.Request) {
	// get parm from url
	host := r.URL.Query().Get("host")
	// list dir list
	dirName := fmt.Sprintf("%s/%s", rdbDir, host)
	// get file list
	fileList, err := os.ReadDir(dirName)
	if err != nil {
		HandleError(w, err)
		return
	}
	var files []string
	for _, file := range fileList {
		if !file.IsDir() && strings.HasSuffix(file.Name(), "rdb") {
			files = append(files, file.Name())
		}
	}
	marshal, err := json.Marshal(files)
	if err != nil {
		HandleError(w, err)
		return
	}
	writeJson(w, marshal)
}

func HandleError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = w.Write([]byte(err.Error()))
}

func GetHosts(w http.ResponseWriter, r *http.Request) {
	// get file list
	fileList, err := os.ReadDir(rdbDir)
	var files []string
	for _, file := range fileList {
		if file.IsDir() {
			files = append(files, file.Name())
		}
	}
	if err != nil {
		HandleError(w, err)
		return
	}
	marshal, err := json.Marshal(files)
	if err != nil {
		HandleError(w, err)
		return
	}
	writeJson(w, marshal)
}
