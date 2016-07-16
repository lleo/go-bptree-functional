#!/usr/bin/env zsh
cd $GOPATH/src/github.com/lleo/go-bptree-functional/test-mains/
go build bptree-functional-test.go

time ./bptree-functional-test -o 3 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-functional-test -o 7 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-functional-test -o 11 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-functional-test -o 13 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-functional-test -o 16 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-functional-test -o 19 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-functional-test -o 32 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-functional-test -o 64 -r -n 200000 -dont-print-ops > /dev/null
