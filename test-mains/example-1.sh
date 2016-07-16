#!/usr/bin/env bash
cd $GOPATH/src/github.com/lleo/go-bptree-functional/test-mains/
go run ./bptree-functional-test.go -o 4 -r -n 2000 -print-tree-at-end -dont-print-ops
