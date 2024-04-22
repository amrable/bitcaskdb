package main

import (
	"github.com/avinassh/go-caskdb/handlers"
	"github.com/gorilla/mux"
	"github.com/lpernett/godotenv"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	log.SetFormatter(&log.TextFormatter{})

	newpath := filepath.Join(".", os.Getenv("DB_DIR"))
	e := os.MkdirAll(newpath, os.ModePerm)
	if e != nil {
		log.Fatal(e)
	}

	r := mux.NewRouter()
	r.HandleFunc("/get/{key}", handlers.Get).Methods("GET")
	r.HandleFunc("/set", handlers.Set).Methods("POST")
	r.HandleFunc("/delete/{key}", handlers.Delete).Methods("DELETE")

	srv := &http.Server{
		Handler:      r,
		Addr:         "127.0.0.1:8000",
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}
