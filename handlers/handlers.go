package handlers

import (
	"encoding/json"
	"github.com/avinassh/go-caskdb/caskdb"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
)

type Record struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func Get(w http.ResponseWriter, r *http.Request) {
	d, e := caskdb.NewDiskStore(os.Getenv("DB_DIR") + "/" + os.Getenv("DB_CURRENT_FILE"))
	defer d.Close()

	if e != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(e.Error() + "\n"))
	}

	key := mux.Vars(r)["key"]
	value := d.Get(key)
	log.WithFields(log.Fields{
		"key":   key,
		"value": value,
	}).Info("Get record")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(value + "\n"))
}

func Set(w http.ResponseWriter, r *http.Request) {
	d, _ := caskdb.NewDiskStore(os.Getenv("DB_DIR") + "/" + os.Getenv("DB_CURRENT_FILE"))
	defer d.Close()
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
	d.Set(data.Key, data.Value)
	w.WriteHeader(http.StatusNoContent)
}

func Delete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	// Implement your delete logic here
	log.Infof("Deleting key %s", key)

	// Return a success response
	w.WriteHeader(http.StatusNoContent)
}
