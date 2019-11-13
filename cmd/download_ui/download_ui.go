package main

import (
	"archive/zip"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const (
	version       = "v0.8.0"
	fileURL       = "https://github.com/cayleygraph/web/releases/download/" + version + "/web.zip"
	fileName      = "web.zip"
	directoryName = "ui"
)

func main() {
	log.Printf("Downloading %s to %s...", fileURL, fileName)
	if err := DownloadFile(fileName, fileURL); err != nil {
		panic(err)
	}
	log.Printf("Downloaded %s to %s", fileURL, fileName)

	log.Printf("Extracting %s to %s...", fileName, directoryName)
	err := Unzip(fileName, directoryName)
	if err != nil {
		panic(err)
	}
	log.Printf("Extracted %s to %s/", fileName, directoryName)
	err = os.Remove(fileName)
	if err != nil {
		panic(err)
	}
	log.Printf("Removed %s", fileName)
}

// DownloadFile will download a url to a local file. It's efficient because it will
// write as it downloads and not load the whole file into memory.
func DownloadFile(filepath string, url string) error {

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Received %v status code instead of 200 for %v", resp.Status, url)
	}

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	return err
}

// Unzip will decompress a zip archive, moving all files and folders
// within the zip file (parameter 1) to an output directory (parameter 2).
func Unzip(src string, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {

		// Store filename/path for returning and using later on
		fpath := filepath.Join(dest, f.Name)

		// Check for ZipSlip. More Info: http://bit.ly/2MsjAWE
		if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return fmt.Errorf("%s: illegal file path", fpath)
		}

		if f.FileInfo().IsDir() {
			// Make Folder
			os.MkdirAll(fpath, 0755)
			continue
		}

		// Make File
		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			return err
		}

		_, err = io.Copy(outFile, rc)

		if err != nil {
			return err
		}

		// Close the file without defer to close before next iteration of loop
		err = outFile.Close()

		if err != nil {
			return err
		}

		err = rc.Close()

		if err != nil {
			return err
		}
	}
	return nil
}
