package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"
)

var baseURL string
var username string
var password string

func main() {
	flag.StringVar(&baseURL, "baseURL", "", "The base webdav URL of the Nextcloud server: https://cloud.example.com/remote.php/dav/files/")
	flag.StringVar(&username, "username", "", "The username of the Nextcloud user")
	flag.StringVar(&password, "password", "", "The password of the Nextcloud user")
	flag.Parse()

	if baseURL == "" || username == "" || password == "" {
		panic("baseURL, username and password are required")
	}
	baseURL = fmt.Sprintf("%s/%s", baseURL, username)

	go func() {
		ticker := time.NewTicker(time.Hour * 4)

		for ; true; <-ticker.C {
			dirName := getDirName()
			fileName := getFileName()

			err := createBackup(fileName)
			if err != nil {
				fmt.Println(err)
				continue
			}

			exists, err := dirNameExists(dirName)
			if err != nil {
				fmt.Println(err)
				continue
			}

			if !exists {
				_, err = createDirName(dirName)
				if err != nil {
					fmt.Println(err)
					continue
				}
			}

			_, err = uploadFileMultipart(fmt.Sprintf("%s/%s/%s", baseURL, dirName, fileName), fileName)
			if err != nil {
				fmt.Println(err)
				continue
			}

			err = deleteLocalBackup(fileName)
			if err != nil {
				fmt.Println(err)
				continue
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Hour * 24)
		for ; true; <-ticker.C {
			err := cleanOldBackups()
			if err != nil {
				fmt.Println(err)
				continue
			}
		}
	}()
}

// Author: https://gist.github.com/mattetti/5914158?permalink_comment_id=3422260#gistcomment-3422260
func uploadFileMultipart(url string, path string) (*http.Response, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	// Reduce number of syscalls when reading from disk.
	bufferedFileReader := bufio.NewReader(f)
	defer f.Close()

	// Create a pipe for writing from the file and reading to
	// the request concurrently.
	bodyReader, bodyWriter := io.Pipe()
	formWriter := multipart.NewWriter(bodyWriter)

	// Store the first write error in writeErr.
	var (
		writeErr error
		errOnce  sync.Once
	)
	setErr := func(err error) {
		if err != nil {
			errOnce.Do(func() { writeErr = err })
		}
	}
	go func() {
		partWriter, err := formWriter.CreateFormFile("file", path)
		setErr(err)
		_, err = io.Copy(partWriter, bufferedFileReader)
		setErr(err)
		setErr(formWriter.Close())
		setErr(bodyWriter.Close())
	}()

	req, err := newAuthedReq(http.MethodPut, url, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", formWriter.FormDataContentType())

	// This operation will block until both the formWriter
	// and bodyWriter have been closed by the goroutine,
	// or in the event of a HTTP error.
	resp, err := http.DefaultClient.Do(req)

	if writeErr != nil {
		return nil, writeErr
	}

	return resp, err
}

func createDirName(dirName string) (bool, error) {
	req, err := newAuthedReq("MKCOL", fmt.Sprintf("%s/%s", baseURL, dirName), nil)
	if err != nil {
		return false, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}

	return res.StatusCode == 201, nil
}

func dirNameExists(dirName string) (bool, error) {
	req, err := newAuthedReq(http.MethodGet, fmt.Sprintf("%s/%s", baseURL, dirName), nil)
	if err != nil {
		return false, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}

	return res.StatusCode == 200, nil
}

func deleteDirName(dirName string) (bool, error) {
	req, err := newAuthedReq("DELETE", fmt.Sprintf("%s/%s", baseURL, dirName), nil)
	if err != nil {
		return false, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}

	return res.StatusCode == 204, nil
}

func getDirName() string {
	return time.Now().Format(time.DateOnly)
}

func getFileName() string {
	return time.Now().Format(time.TimeOnly)
}

func createBackup(fileName string) error {
	var stdout, stderr bytes.Buffer

	cmd := exec.Command("pg_dumpall", "-f", fileName)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	fmt.Printf("Stdout: %s\n", stdout.String())
	fmt.Printf("Stderr: %s\n", stderr.String())

	// TODO Missing gzip

	return err
}

func deleteLocalBackup(fileName string) error {
	return os.Remove(fileName)
}

func cleanOldBackups() error {
	threeDaysAgoDirName := time.Now().AddDate(0, 0, -3).Format(time.DateOnly)
	exists, err := dirNameExists(threeDaysAgoDirName)
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	_, err = deleteDirName(threeDaysAgoDirName)
	if err != nil {
		return err
	}

	return nil
}

func newAuthedReq(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(username, password)

	return req, nil
}
