package main

import (
	"bytes"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"sync"
	"time"
)

type KV struct {
	key   string
	value string
}

func timer(name string) func() {
	start := time.Now()
	return func() {
		fmt.Printf("%s took %v\n", name, time.Since(start))
	}
}

func main() {
	defer timer("main")()
	var wg sync.WaitGroup

	//for i := 0; i < 1000; i++ {
	//	wg.Add(1)
	//	go func() {
	//		defer wg.Done()
	//		url := fmt.Sprintf("http://localhost:8000/get/%d", i)
	//		_, err := http.Get(url)
	//		if err != nil {
	//			//log.Error(str, err.Error())
	//		} else {
	//			//log.Info(str)
	//		}
	//	}()
	//}

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			str := fmt.Sprintf("{\"key\":\"%d\",\"value\":\"%d\"}", i, i)
			var jsonStr = []byte(str)
			req, err := http.NewRequest("POST", "http://localhost:8000/set", bytes.NewBuffer(jsonStr))
			req.Header.Set("Content-Type", "application/json")
			client := &http.Client{}
			_, err = client.Do(req)

			if err != nil {
				log.Error(str, err.Error())
			} else {
				log.Info(str)
			}
		}(i)

		if i%20 == 0 && i > 10 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	wg.Wait()
}
