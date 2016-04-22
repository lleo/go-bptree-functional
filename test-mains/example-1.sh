#!/usr/bin/env bash
cd $GOPATH/src/github.com/lleo/bptree-persistent/test-mains/
go run ./bptree-persistent-test.go -o 4 -r -n 2000 -print-tree-at-end -dont-print-ops
