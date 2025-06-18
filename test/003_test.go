package main_test

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/ochaton/kvdb/test/helpers"
)

func TestKVDBLoading(t *testing.T) {
	helpers.CleanDB(helpers.DbPath)
	data := map[string]string{
		"0000000001.jlog": `
{"lsn":1,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-1","value":{"name":"Alice-1","age":1}}}
{"lsn":2,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-2","value":{"name":"Alice-2","age":2}}}
{"lsn":3,"op":"del","time":1750280676,"record":{"tag":"users","key":"Alice-2","value":null}}
{"lsn":4,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-3","value":{"name":"Alice-3","age":3}}}
{"lsn":5,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-3","value":{"name":"Alice-3","age":8}}}
`,
		"0000000005.snap": `
{"lsn":1,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-1","value":{"name":"Alice-1","age":1}}}
{"lsn":5,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-3","value":{"name":"Alice-3","age":8}}}
`,
		"0000000006.jlog": `
{"lsn":6,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-4","value":{"name":"Alice-4","age":4}}}
{"lsn":7,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-5","value":{"name":"Alice-5","age":5}}}
{"lsn":8,"op":"del","time":1750280676,"record":{"tag":"users","key":"Alice-5","value":null}}
{"lsn":9,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-6","value":{"name":"Alice-6","age":6}}}
{"lsn":10,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-6","value":{"name":"Alice-6","age":8}}}
`,
		"0000000010.snap": `
{"lsn":1,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-1","value":{"name":"Alice-1","age":1}}}
{"lsn":5,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-3","value":{"name":"Alice-3","age":8}}}
{"lsn":6,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-4","value":{"name":"Alice-4","age":4}}}
{"lsn":10,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-6","value":{"name":"Alice-6","age":8}}}
`,
		"0000000011.jlog": `
{"lsn":11,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-7","value":{"name":"Alice-7","age":7}}}
`,
		"0000000020.jlog": `
{"lsn":12,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-8","value":{"name":"Alice-8","age":8}}}
`,
	}
	if err := os.MkdirAll(helpers.DbPath, 0755); err != nil {
		t.Fatalf("Failed to setup data: failed to create dir %v", err)
	}

	for fileName, content := range data {
		filePath := fmt.Sprintf("%s/%s", helpers.DbPath, fileName)
		fh, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0644)
		if err != nil {
			t.Fatalf("Failed to setup data: can not open file %v", err)
		}

		if _, err := fh.Write([]byte(content)); err != nil {
			t.Fatalf("Failed to setup data: can not write to file %v", err)
		}

		if err := fh.Close(); err != nil {
			t.Fatalf("Failed to setup data: can not close file %v", err)
		}
	}

	db, err := helpers.SetupDB(helpers.DbPath, false)
	if err != nil {
		t.Fatalf("%v", err)
	}
	usersSpace, err := db.NewSpace("users")
	if err != nil {
		t.Fatalf("failed to create space users: %v", err)
	}

	iter := usersSpace.Iter()
	scannedData := make([]helpers.TestUser, 0)
	for iter.HasNext() {
		ret := helpers.TestUser{}
		if err := iter.Next(&ret); err != nil {
			t.Fatalf("failed to scan space users: %v", err)
		}
		scannedData = append(scannedData, ret)
	}
	iter.Release()

	sort.Slice(scannedData, func(i, j int) bool {
		return scannedData[i].Name < scannedData[j].Name
	})
	expectedData := []helpers.TestUser{
		{Name: "Alice-1", Age: 1},
		{Name: "Alice-3", Age: 8},
		{Name: "Alice-4", Age: 4},
		{Name: "Alice-6", Age: 8},
		{Name: "Alice-7", Age: 7},
		{Name: "Alice-8", Age: 8},
	}
	if !reflect.DeepEqual(scannedData, expectedData) {
		t.Fatalf("failed to load: loaded data: %v expected data: %v", scannedData, expectedData)
	}
}
