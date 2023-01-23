package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func mergeFilesToSingleFile() error {
	// Merge all files into a single file
	fmt.Println("Merging files into a single file...")
	mergedFile, err := os.Create(".workload-urls")
	if err != nil {
		return err
	}
	defer mergedFile.Close()

	files, err := os.ReadDir(".workload-files")
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		f, err := os.Open(fmt.Sprintf(".workload-files/%s", file.Name()))
		if err != nil {
			return err
		}
		_, err = io.Copy(mergedFile, f)
		if err != nil {
			return err
		}
	}
	return nil
}

func downloadFile(url string, filepath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer f.Close()

	err = resp.Write(f)
	if err != nil {
		return err
	}
	return nil
}

func downloadWorkloadFiles() error {
	fmt.Println("Downloading workload files...")
	fmt.Println("Storing workload files in .workload-files directory")
	err := os.Mkdir(".workload-files", 0755)
	if err != nil {
		return err
	}

	fmt.Println("Downloading filelist.csv...")
	filelistURL := "https://db.in.tum.de/teaching/ws2223/clouddataprocessing/data/filelist.csv"
	res, err := http.Get(filelistURL)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	filelist := string(body)

	fmt.Print("Start downloading workload files...\n\n")
	files := strings.Split(filelist, "\n")
	for i, file := range files {
		if file == "" {
			continue
		}
		fmt.Printf("\033[2K\rDownloading file %d/%d", i+1, len(files)-1)
		err := downloadFile(file, fmt.Sprintf(".workload-files/%d.txt", i))
		if err != nil {
			return err
		}
	}
	fmt.Println("\n\nFinished downloading workload files.")
	err = mergeFilesToSingleFile()
	if err != nil {
		return err
	}

	fmt.Println("Removing .workload-files directory...")
	err = os.RemoveAll(".workload-files")
	if err != nil {
		return err
	}

	return nil
}

func main() {
	// Check if .workload-files directory exists, if not, download them
	if _, err := os.Stat(".workload-urls"); err == nil {
		fmt.Println("Workload URLs already exist. Skip downloading.")
	} else {
		err := downloadWorkloadFiles()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}
