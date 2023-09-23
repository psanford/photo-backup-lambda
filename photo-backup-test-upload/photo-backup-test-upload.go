package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

var (
	url      = flag.String("url", "", "URL of upload_request handler")
	username = flag.String("username", "", "Basic auth username")
	password = flag.String("password", "", "Basic auth password")
	file     = flag.String("file", "", "Path to file to upload")
)

func main() {
	flag.Parse()
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if *url == "" {
		return fmt.Errorf("-url is required")
	}

	if *file == "" {
		return fmt.Errorf("-file is required")
	}
	f, err := os.Open(*file)
	if err != nil {
		return err
	}

	summer := sha256.New()
	_, err = io.Copy(summer, f)
	if err != nil {
		return err
	}

	id := hex.EncodeToString(summer.Sum(nil))
	stat, err := f.Stat()
	if err != nil {
		return err
	}

	size := stat.Size()
	mtime := stat.ModTime()

	name := filepath.Base(*file)

	header := make([]byte, 512)
	f.Seek(0, io.SeekStart)
	io.ReadFull(f, header)

	contentType := http.DetectContentType(header)

	dest, err := requestUploadURL(id, name, contentType, mtime, size)
	if err != nil {
		return err
	}

	log.Printf("upload dest: %+v\n", dest)

	if dest.Status == StatusSkipUpload {
		log.Printf("upload already exists, skipping. id=%s", id)
		return nil
	}

	f.Seek(0, io.SeekStart)
	err = uploadFile(f, size, dest)
	if err != nil {
		return err
	}

	log.Printf("Upload success!, id=%s", id)

	return nil

}

func uploadFile(r io.Reader, size int64, dest *UploadDestination) error {
	if dest.Method == "" {
		dest.Method = "PUT"
	}
	req, err := http.NewRequest(dest.Method, dest.URL, r)
	if err != nil {
		return err
	}

	req.Header = dest.Headers
	req.ContentLength = size

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("uploadFile: non-200 status code: %d\n%s\n", resp.StatusCode, body)
	}

	return nil
}

func requestUploadURL(id, name, contentType string, mtime time.Time, size int64) (*UploadDestination, error) {
	meta := FileMetadata{
		ID:          id,
		Name:        name,
		Mtime:       mtime,
		Bytes:       size,
		TestUpload:  true,
		ContentType: contentType,
	}

	jsontxt, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(jsontxt)

	req, err := http.NewRequest("POST", *url, buf)
	if err != nil {
		return nil, err
	}
	req.Header.Add("content-type", "application/json")
	req.SetBasicAuth(*username, *password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != http.StatusConflict {
		return nil, fmt.Errorf("non-200 status code: %d", resp.StatusCode)
	}

	var dest UploadDestination
	err = json.NewDecoder(resp.Body).Decode(&dest)
	if err != nil {
		return nil, err
	}

	return &dest, nil
}

type UploadDestination struct {
	Status  Status      `json:"status"`
	Error   string      `json:"error,omitempty"`
	URL     string      `json:"url"`
	Method  string      `json:"method"`
	Headers http.Header `json:"headers"`
}

type FileMetadata struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Mtime       time.Time `json:"mtime"`
	Bytes       int64     `json:"size"`
	ContentType string    `json:"content_type"`
	TestUpload  bool      `json:"test_upload"`
}

type Status string

var (
	StatusOK         Status = "ok"
	StatusSkipUpload Status = "skip" // file already exists
	StatusErr        Status = "error"
)
