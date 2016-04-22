#!/usr/bin/env zsh
cd $GOPATH/src/github.com/lleo/bptree-persistent/test-mains/
go build bptree-persistent-test.go

time ./bptree-persistent-test -o 3 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-persistent-test -o 7 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-persistent-test -o 11 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-persistent-test -o 13 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-persistent-test -o 16 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-persistent-test -o 19 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-persistent-test -o 32 -r -n 200000 -dont-print-ops > /dev/null

echo
time ./bptree-persistent-test -o 64 -r -n 200000 -dont-print-ops > /dev/null
