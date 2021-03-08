package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/felixge/httpsnoop"
	"github.com/inconshreveable/log15"
	"github.com/psanford/lambdahttp/lambdahttpv2"
	"golang.org/x/crypto/bcrypt"
)

var (
	addr    = flag.String("listen-addr", "127.0.0.1:1234", "Host/Port to listen on")
	cliMode = flag.String("mode", "", "execution mode: http|lambda")

	ssmPrefix = "/prod/lambda/photo-backup/"
)

func main() {
	flag.Parse()
	logHandler := log15.StreamHandler(os.Stdout, log15.LogfmtFormat())
	log15.Root().SetHandler(logHandler)

	kv := newKV()

	bucket, err := kv.get("bucket")
	if err != nil {
		panic(err)
	}
	pathPrefix, err := kv.get("pathPrefix")
	if err != nil {
		panic(err)
	}

	bcryptPass, err := kv.get("bcryptPass")
	if err != nil {
		panic(err)
	}

	sess := session.Must(session.NewSession())
	s3client := s3.New(sess, &aws.Config{
		Region: aws.String("us-east-1"),
	})

	s := &server{
		s3:         s3client,
		bucket:     bucket,
		pathPrefix: pathPrefix,
		bcryptPass: bcryptPass,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/upload_request", s.handleUploadRequest)

	handler := logMiddleware(s.basicAuthMiddleware(mux))

	switch *cliMode {
	case "http":
		fmt.Printf("Listening on %s\n", *addr)
		panic(http.ListenAndServe(*addr, handler))
	default:
		lambda.Start(lambdahttpv2.NewLambdaHandler(handler))
	}
}

func (s *server) basicAuthMiddleware(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)

		_, password, authOK := r.BasicAuth()
		if authOK == false {
			http.Error(w, "Not authorized", 401)
			return
		}

		if err := bcrypt.CompareHashAndPassword([]byte(s.bcryptPass), []byte(password)); err != nil {
			http.Error(w, "Not authorized", 401)
			return
		}

		next.ServeHTTP(w, r)
	}
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		url := *r.URL
		host := r.Host

		lgr := log15.New("url", url.String(), "host", host, "remote_addr", r.RemoteAddr)

		childCtx := WithLgrContext(r.Context(), lgr)
		childReq := r.WithContext(childCtx)

		metrics := httpsnoop.CaptureMetrics(next, w, childReq)

		lgr.Info("request", "status", metrics.Code, "duration_ms", metrics.Duration.Milliseconds(), "resp_size", metrics.Written, "method", r.Method, "proto", r.Proto)
	})
}

type server struct {
	s3         *s3.S3
	bucket     string
	pathPrefix string
	bcryptPass string
}

type FileMetadata struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Mtime       time.Time `json:"mtime"`
	Bytes       int64     `json:"size"`
	ContentType string    `json:"content_type"`
	TestUpload  bool      `json:"test_upload"`
}

var (
	StatusOK         = "ok"
	StatusSkipUpload = "skip" // file already exists
	StatusErr        = "error"
)

type UploadDestination struct {
	Status  string      `json:"status"`
	Error   string      `json:"error,omitempty"`
	URL     string      `json:"url"`
	Method  string      `json:"method"`
	Headers http.Header `json:"headers"`
}

func (s *server) handleUploadRequest(w http.ResponseWriter, r *http.Request) {
	lgr := LgrFromContext(r.Context())

	if r.Method != "POST" {
		http.Error(w, "Bad Method", http.StatusMethodNotAllowed)
		return
	}

	dec := json.NewDecoder(r.Body)

	var meta FileMetadata
	err := dec.Decode(&meta)
	if err != nil {
		lgr.Error("decode json err", "err", err)
		resp := UploadDestination{
			Status: StatusErr,
			Error:  "bad request",
		}
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(resp)
		return
	}

	ts := meta.Mtime.Format("2006-01-02-15_04_05.9")
	s3Path := path.Join(s.pathPrefix, ts+"-"+meta.ID+"-"+meta.Name)

	lgr = lgr.New(
		"id", meta.ID,
		"filename", meta.Name,
		"path", s3Path,
		"size", meta.Bytes,
		"content-type", meta.ContentType,
		"mtime", meta.Mtime,
		"test-upload", meta.TestUpload,
	)

	_, err = s.s3.HeadObject(&s3.HeadObjectInput{
		Bucket: &s.bucket,
		Key:    &s3Path,
	})

	if err == nil {
		lgr.Error("filename_already_exists")
		resp := UploadDestination{
			Status: StatusSkipUpload,
		}
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(resp)
		return
	}

	putObjInput := &s3.PutObjectInput{
		Bucket:        &s.bucket,
		Key:           aws.String(s3Path),
		ContentLength: aws.Int64(meta.Bytes),
		ContentType:   aws.String(meta.ContentType),
		Metadata: map[string]*string{
			"filename": aws.String(meta.Name),
			"mtime":    aws.String(meta.Mtime.Format(time.RFC3339)),
		},
	}

	if meta.TestUpload {
		putObjInput.Metadata["test-upload"] = aws.String("true")
	}

	req, _ := s.s3.PutObjectRequest(putObjInput)

	url, err := req.Presign(1 * time.Minute)
	if err != nil {
		fmt.Println("error presigning request", err)
		return
	}

	resp := UploadDestination{
		Status: StatusOK,
		URL:    url,
		Method: "PUT",
	}
	resp.Headers = make(http.Header)
	resp.Headers.Set("content-length", strconv.Itoa(int(meta.Bytes)))
	resp.Headers.Set("content-type", meta.ContentType)
	for k, v := range putObjInput.Metadata {
		resp.Headers.Set("x-amz-meta-"+k, *v)
	}

	lgr.Info("upload_request_success")

	json.NewEncoder(w).Encode(resp)
}

func (kv *kv) get(key string) (string, error) {
	path := ssmPrefix + key
	req := ssm.GetParameterInput{
		Name:           &path,
		WithDecryption: aws.Bool(true),
	}

	resp, err := kv.client.GetParameter(&req)
	if err != nil {
		return "", fmt.Errorf("read key %s err: %w", key, err)
	}
	val := resp.Parameter.Value
	if val == nil {
		return "", errors.New("value is nil")
	}
	return *val, nil
}

func newKV() *kv {
	sess := session.Must(session.NewSession())
	ssmClient := ssm.New(sess)

	return &kv{
		client: ssmClient,
	}
}

type kv struct {
	client *ssm.SSM
}
