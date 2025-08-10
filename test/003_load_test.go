package main_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/ochaton/kvdb"
	"github.com/ochaton/kvdb/test/helpers"
)

func TestKVDBLoading(t *testing.T) {
	initialDataFiles := map[string]string{
		"0000000001.jlog": `
{"lsn":1,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-1","value":{"name":"Alice-1","age":1}}}
{"lsn":2,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-2","value":{"name":"Alice-2","age":2}}}
{"lsn":3,"op":"del","time":1750280676,"record":{"space":"users","key":"Alice-2","value":null}}
{"lsn":4,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-3","value":{"name":"Alice-3","age":3}}}
{"lsn":5,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-3","value":{"name":"Alice-3","age":8}}}
`,
		"0000000005.snap": `
{"lsn":1,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-1","value":{"name":"Alice-1","age":1}}}
{"lsn":5,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-3","value":{"name":"Alice-3","age":8}}}
`,
		"0000000006.jlog": `
{"lsn":6,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-4","value":{"name":"Alice-4","age":4}}}
{"lsn":7,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-5","value":{"name":"Alice-5","age":5}}}
{"lsn":8,"op":"del","time":1750280676,"record":{"space":"users","key":"Alice-5","value":null}}
{"lsn":9,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-6","value":{"name":"Alice-6","age":6}}}
{"lsn":10,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-6","value":{"name":"Alice-6","age":8}}}
`,
		"0000000010.snap": `
{"lsn":1,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-1","value":{"name":"Alice-1","age":1}}}
{"lsn":5,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-3","value":{"name":"Alice-3","age":8}}}
{"lsn":6,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-4","value":{"name":"Alice-4","age":4}}}
{"lsn":10,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-6","value":{"name":"Alice-6","age":8}}}
`,
		"0000000011.jlog": `
{"lsn":11,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-7","value":{"name":"Alice-7","age":7}}}
`,
		"0000000020.jlog": `
{"lsn":12,"op":"set","time":1750280676,"record":{"space":"users","key":"Alice-8","value":{"name":"Alice-8","age":8}}}
`,
	}

	db, err := helpers.SetupDBWithData(helpers.DbPath, initialDataFiles)
	if err != nil {
		t.Fatalf("failed to setup db %v", err)
	}

	sp := kvdb.GetSpace[helpers.TestUser](db, "users")
	scannedData, err := sp.List().Collect()
	if err != nil {
		t.Fatalf("failed to scan space users: %v", err)
	}

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
	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		t.Fatalf("%v", err)
	}
}

func TestKVDBLoadingWithLastEmptyFile(t *testing.T) {
	initialDataFiles := map[string]string{
		"0000000001.jlog": `
{"lsn":1,"op":"set","time":1750280676,"record":{"tag":"users","key":"Alice-1","value":{"name":"Alice-1","age":1}}}`,
		"0000000002.jlog": "",
	}

	db, err := helpers.SetupDBWithData(helpers.DbPath, initialDataFiles)
	if err != nil {
		t.Fatalf("failed to setup db %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("failed to close db %v", err)
	}

	_, err = helpers.SetupDB(helpers.DbPath, false)
	if err != nil {
		t.Fatalf("failed to second setup db %v", err)
	}

	if err := helpers.CleanDB(helpers.DbPath); err != nil {
		t.Fatalf("%v", err)
	}
}
