package main

import (
	"bufio"
	"bytes"
	"log"
	"os"
)

type CSVParser struct {
	f         *os.File
	separator []byte
	next      chan []byte
	errors    chan error
	headers   [][]byte
}

func NewCSVParser(filename string, separator []byte) (*CSVParser, error) {
	cp := CSVParser{f: nil, separator: separator, next: make(chan []byte), errors: make(chan error), headers: [][]byte{}}
	var err error

	// Open file to read
	cp.f, err = os.Open(filename)
	if err != nil {
		return nil, err
	}

	// Read the file, put lines on the channel
	go cp.read()
	return &cp, nil
}

func (cp *CSVParser) read() {
	scanner := bufio.NewScanner(cp.f)
	// Get Headers
	scanner.Scan()
	header_row := scanner.Bytes()
	next_entry(header_row, cp.separator)

	// Get Rows
	for scanner.Scan() {
		cp.next <- scanner.Bytes()
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func next_entry(row []byte, separator []byte) (next []byte, remaining []byte, next_len int) {
	// Quoted Field
	if row[0] == byte('"') {
		terminator := []byte("\"" + string(separator))
		next_sep := bytes.Index(row, terminator)
		if next_sep == -1 {
			return nil, nil, -1
		}
		next = row[1:next_sep]
		remaining = row[next_sep+len(terminator):]
		next_len = next_sep - 1
		return
	}
	layers := 0
	for i := 0; i < len(row)+1-len(separator); i++ {
		if row[i] == byte('"') {
			layers++
		}
		if bytes.Equal(row[i:i+len(separator)], separator) {

		}
	}
	return
}
