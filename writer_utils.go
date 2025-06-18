package kvdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var lsnRegexp = regexp.MustCompile(`^(?:.+/)?(\d+)\.([a-z]+)$`)

func writeTo(op *operation, file *os.File) error {
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}

	data = append(data, '\n')

	if _, err = file.Write(data); err != nil {
		return err
	}
	return nil
}

func writeManyTo(ops []*operation, file *os.File) error {
	res := make([]byte, 0)
	for _, op := range ops {
		data, err := json.Marshal(op)
		if err != nil {
			return err
		}
		res = append(res, data...)
		res = append(res, '\n')
	}
	_, err := file.Write(res)
	if err != nil {
		return err
	}
	return nil
}

func listDataFiles(dir string, extensions []string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	filesNames := make([]string, 0, len(entries))
	var name string
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		if !ent.Type().IsRegular() {
			continue
		}
		name = ent.Name()

		for _, ext := range extensions {
			if strings.HasSuffix(name, ext) {
				filesNames = append(filesNames, fmt.Sprintf("%s/%s", dir, name))
				break
			}
		}
	}

	return filesNames, nil
}

func deleteDataFiles(filesPathes []string) error {
	for _, filePath := range filesPathes {
		if err := os.Remove(filePath); err != nil {
			return err
		}
	}
	return nil
}

func lsn2str(lsn uint64) string {
	return fmt.Sprintf("%010d", lsn)
}

func str2lsn(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}

func getFileLsn(filePath string) (uint64, error) {
	match := lsnRegexp.FindStringSubmatch(filePath)
	if len(match) < 2 {
		return 0, fmt.Errorf("wrong fileName to get lsn: %s", filePath)
	}
	return str2lsn(match[1])
}

func loadDataFile(filePath string, applyTxn func(*operation) (uint64, error)) (uint64, error) {
	fh, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer fh.Close()

	rs := withReaderStats(fh)
	lsn, err := innerLoadDataFile(rs, applyTxn)
	if err != nil {
		return 0, err
	}

	log.Printf("loadFile %s: %s\n", fh.Name(), rs.HumanStats())
	return lsn, nil
}

func innerLoadDataFile(file io.Reader, applyTxn applyTxnFunc) (uint64, error) {
	dec := json.NewDecoder(bufio.NewReader(file))

	var err error
	var lsn uint64
	for {
		var op operation
		if err = dec.Decode(&op); err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		lsn, err = applyTxn(&op)
		if err != nil {
			return 0, err
		}
	}
	return lsn, nil
}

func closeFile(file *os.File) error {
	if err := file.Sync(); err != nil {
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}
	return nil
}
