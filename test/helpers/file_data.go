package helpers

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
)

func SetupDataFiles(path string, data map[string]string) error {
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to write data (create dir): %v", err)
	}
	for fileName, content := range data {
		filePath := fmt.Sprintf("%s/%s", path, fileName)
		if err := writeFileData(filePath, content); err != nil {
			return err
		}
	}
	return nil
}

func writeFileData(filePath string, content string) error {
	fh, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("failed to write data (open file '%s'): %v", filePath, err)
	}
	defer fh.Close()

	if _, err := fh.Write([]byte(content)); err != nil {
		return fmt.Errorf("failed to write data (write file '%s'): %v", filePath, err)
	}
	return nil
}

func readFileData(filePath string) (string, error) {
	fileData, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file '%s': %v", filePath, err)
	}
	return string(fileData), nil
}

func CheckDataFiles(path string, expectedData map[string]string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) != len(expectedData) {
		return fmt.Errorf("different files count")
	}
	for _, entry := range entries {
		expectedFileData, ok := expectedData[entry.Name()]
		if !ok {
			return fmt.Errorf("unexpected file '%s'", entry.Name())
		}

		filePath := fmt.Sprintf("%s/%s", path, entry.Name())
		scannedFileData, err := readFileData(filePath)
		if err != nil {
			return err
		}

		if err := compareJsons(scannedFileData, expectedFileData); err != nil {
			return err
		}
	}
	return nil
}

func compareJsons(scanned, expected string) error {
	scannedJsons, err := parseJsons(scanned)
	if err != nil {
		return fmt.Errorf("failed to parse scanned data: %v", err)
	}
	expectedJsons, err := parseJsons(expected)
	if err != nil {
		return fmt.Errorf("failed to parse expected data: %v", err)
	}
	if !reflect.DeepEqual(scannedJsons, expectedJsons) {
		return fmt.Errorf("failed to compare: got: '%v' expected: '%v'", scannedJsons, expectedJsons)
	}
	return nil
}

func parseJsons(data string) ([]any, error) {
	jsons := make([]any, 0)
	var line any
	for dataLine := range strings.SplitSeq(data, "\n") {
		if len(dataLine) == 0 {
			continue
		}
		if err := json.Unmarshal([]byte(dataLine), &line); err != nil {
			return []any{}, err
		}
		jsons = append(jsons, line)
	}
	return jsons, nil
}

// func parseTypedJsons[T any](data string) ([]T, error) {
// 	jsons := make([]any, 0)
// 	var line any
// 	for dataLine := range strings.SplitSeq(data, "\n") {
// 		if len(dataLine) == 0 {
// 			continue
// 		}
// 		if err := json.Unmarshal([]byte(dataLine), &line); err != nil {
// 			return []any{}, err
// 		}
// 		jsons = append(jsons, line)
// 	}
// 	return jsons, nil
// }

// func FilesDataToData[T any](filesData map[string]string) ([]T, error) {
// 	for fileName, fileData := range filesData {
// 		if len(fileData) == 0 {
// 			continue
// 		}
// 		jsons, err := parseJsons(fileData)
// 		if err != nil {
// 			return []T{}, fmt.Errorf("failed to parse filedata of '%s': %v", fileName, err)
// 		}
// 	}
// 	//	initialDataFiles := map[string]string{
// 	//		"0000000005.snap": `
// 	//
// 	// {"lsn":1,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-1","value":{"name":"Alice-1","age":1}}}
// 	// {"lsn":5,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-3","value":{"name":"Alice-3","age":8}}}
// 	// `,
// 	//
// 	//		"0000000006.jlog": ``,
// 	//	}
// }
