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
	"strings"
	"time"

	"github.com/dsoprea/go-exif/v3"
	exifcommon "github.com/dsoprea/go-exif/v3/common"
)

var (
	url        = flag.String("url", "", "URL of upload_request handler")
	username   = flag.String("username", "", "Basic auth username")
	password   = flag.String("password", "", "Basic auth password")
	pendingDir = flag.String("pending_dir", "", "Path to pending files")
	doneDir    = flag.String("done_dir", "", "Path to move files to when upload completes")
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

	if *pendingDir == "" {
		return fmt.Errorf("-pending_dir is required")
	}
	files, err := os.ReadDir(*pendingDir)
	if err != nil {
		return err
	}

	err = os.MkdirAll(*doneDir, 0700)
	if err != nil {
		return err
	}

	for i, finfo := range files {
		err := func() error {
			i := i
			finfo := finfo
			srcPath := filepath.Join(*pendingDir, finfo.Name())
			f, err := os.Open(srcPath)
			if err != nil {
				return err
			}
			defer f.Close()

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

			name := filepath.Base(finfo.Name())

			header := make([]byte, 512)
			f.Seek(0, io.SeekStart)
			io.ReadFull(f, header)

			contentType := http.DetectContentType(header)

			contentParts := strings.SplitN(contentType, "/", 2)
			if contentParts[0] != "image" && contentParts[0] != "audio" && contentParts[0] != "video" {
				log.Printf("%s not a media file, content-type: %s", finfo.Name(), contentType)
				return nil
			}

			if contentParts[0] == "image" {
				f.Seek(0, io.SeekStart)
				exif, err := readExifInfo(f)
				if err != nil {
					log.Printf("read exif err: %s", err)
				} else {
					mtime = exif.Time
				}
			}

			log.Printf("[%d/%d] upload: %s\n", i+1, len(files), name)

			dest, err := requestUploadURL(id, name, contentType, mtime, size)
			if err != nil {
				return err
			}

			if dest.Status == StatusSkipUpload {
				log.Printf("upload already exists, skipping. id=%s", id)

				err = os.Rename(srcPath, filepath.Join(*doneDir, finfo.Name()))
				if err != nil {
					return err
				}

				return nil
			}

			f.Seek(0, io.SeekStart)
			err = uploadFile(f, size, dest)
			if err != nil {
				return err
			}

			err = os.Rename(srcPath, filepath.Join(*doneDir, finfo.Name()))
			if err != nil {
				return err
			}

			log.Printf("Upload success!, id=%s", id)
			return nil
		}()

		if err != nil {
			return err
		}
	}

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

type ExifInfo struct {
	Time  time.Time `json:"time"`
	Make  string    `json:"make"`
	Model string    `json:"model"`
}

func readExifInfo(r io.Reader) (*ExifInfo, error) {
	rawExif, err := exif.SearchAndExtractExifWithReader(r)
	if err != nil {
		return nil, fmt.Errorf("parse jpeg exif search err %w", err)
	}

	var meta ExifInfo
	im, err := exifcommon.NewIfdMappingWithStandard()
	if err != nil {
		return nil, fmt.Errorf("parse jpeg ifd mapping err %w", err)
	}

	ti := exif.NewTagIndex()

	_, index, err := exif.Collect(im, ti, rawExif)
	if err != nil {
		return nil, fmt.Errorf("parse jpeg collect err %w", err)
	}

	cb := func(ifd *exif.Ifd, entry *exif.IfdTagEntry) error {
		tagName := entry.TagName()
		value, _ := entry.Value()
		switch tagName {
		case "Make":
			meta.Make = value.(string)
		case "Model":
			meta.Model = value.(string)
		case "DateTime":
			meta.Time, _ = time.Parse("2006:01:02 15:04:05", value.(string))
		}
		return nil
	}

	err = index.RootIfd.EnumerateTagsRecursively(cb)
	if err != nil {
		return nil, fmt.Errorf("enumeratetagsrecursively err %w", err)
	}

	return &meta, nil
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
