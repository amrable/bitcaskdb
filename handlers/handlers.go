package handlers

import (
	"encoding/json"
	"github.com/amrable/go-caskdb/caskdb"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"net/http"
)

type Record struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func Get(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	value := caskdb.Instance.Get(key)
	log.WithFields(log.Fields{
		"key":   key,
		"value": value,
	}).Info("Get record")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(value + "\n"))
}

func Set(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var data Record
	e := decoder.Decode(&data)
	if e != nil {
		panic(e)
	}
	log.WithFields(log.Fields{
		"key":   data.Key,
		"value": data.Value,
	}).Info("Set record")
	caskdb.Instance.Set(data.Key, data.Value)
	w.WriteHeader(http.StatusNoContent)
}

func Delete(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]

	log.WithFields(log.Fields{
		"key": key,
	}).Info("Delete record")
	caskdb.Instance.Delete(key)
	w.WriteHeader(http.StatusNoContent)
}
