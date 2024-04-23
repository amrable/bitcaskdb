package caskdb

import "os"

var Instance *DiskStore

func Init() {
	Instance, _ = NewDiskStore(os.Getenv("DB_DIR") + "/" + os.Getenv("DB_CURRENT_FILE"))
}

func Close() {
	Instance.Close()
}
