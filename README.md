# Bitcaskdb - Disk based Log Structured Hash Table Store

This project is implementation for [bitcask paper](https://riak.com/assets/bitcask-intro.pdf)

Inspired by [go-caskdb](https://github.com/avinassh/go-caskdb/tree/final)

## Steps to run locally
- clone the repo `git clone git@github.com:amrable/bitcaskdb.git`
- copy example.env to create .env file
- make sure that you have golang installed on your machine, run `go run main.go` to start the server
- To run from cli, open the terminal, cd to the root directory of the repo to run the following commands
  - run `./caskdb-client get key` to get the value of "key"
  - run `./caskdb-client set key value` to set the value of "key" to "value"
  - run `./caskdb-client delete key` to delete the value of "key"
- Or use http requests to set/get/delete

## WIP - TODOS
- [ ] Handle concurrent writes
- [X] Open file once instead of opening it for every request
- [ ] Create new file after hitting FILE_LIMIT
- [X] Create a garbage collector (merge process)
- [ ] Create/Use hint files
