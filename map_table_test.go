package gocassa

import (
	"context"
	"reflect"
	"testing"
)

func TestMapTable(t *testing.T) {
	tbl := ns.MapTable("customer81", "Id", Customer{})
	createIf(tbl.(TableChanger), t)
	joe := Customer{
		Id:   "33",
		Name: "Joe",
	}
	err := tbl.Set(joe).Run()
	if err != nil {
		t.Fatal(err)
	}
	res := &Customer{}
	err = tbl.Read("33", res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*res, joe) {
		t.Fatal(*res, joe)
	}
	err = tbl.Delete("33").Run()
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Read("33", res).Run()
	if err == nil {
		t.Fatal(res)
	}
}

func TestMapTableUpdate(t *testing.T) {
	tbl := ns.MapTable("customer82", "Id", Customer{})
	createIf(tbl.(TableChanger), t)
	joe := Customer{
		Id:   "33",
		Name: "Joe",
	}
	err := tbl.Set(joe).Run()
	if err != nil {
		t.Fatal(err)
	}
	res := &Customer{}
	err = tbl.Read("33", res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*res, joe) {
		t.Fatal(*res, joe)
	}
	err = tbl.Update("33", map[string]interface{}{
		"Name": "John",
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	err = tbl.Read("33", res).Run()
	if err != nil {
		t.Fatal(res, err)
	}
	if res.Name != "John" {
		t.Fatal(res)
	}
}

func TestMapTableMultiRead(t *testing.T) {
	tbl := ns.MapTable("customer83", "Id", Customer{})
	createIf(tbl.(TableChanger), t)
	joe := Customer{
		Id:   "33",
		Name: "Joe",
	}
	err := tbl.Set(joe).Run()
	if err != nil {
		t.Fatal(err)
	}
	jane := Customer{
		Id:   "34",
		Name: "Jane",
	}
	err = tbl.Set(jane).Run()
	if err != nil {
		t.Fatal(err)
	}
	customers := &[]Customer{}
	err = tbl.MultiRead([]interface{}{"33", "34"}, customers).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*customers) != 2 {
		t.Fatalf("Expected to multiread 2 records, got %d", len(*customers))
	}
	if !reflect.DeepEqual((*customers)[0], joe) {
		t.Fatalf("Expected to find joe, got %v", (*customers)[0])
	}
	if !reflect.DeepEqual((*customers)[1], jane) {
		t.Fatalf("Expected to find jane, got %v", (*customers)[1])
	}
}

func TestMapTableDeleteKeysFromMap(t *testing.T) {
	ctx := context.TODO()
	type TestTable struct {
		Id string
		MyMap map[string]int
	}
	tbl := ns.MapTable("map_testing", "Id", TestTable{})
	createIf(tbl.(TableChanger), t)

	entry := TestTable{Id: "1", MyMap: map[string]int{"A": 1, "B": 2, "C": 3}}
	err := tbl.Set(entry).RunWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	readEntry := &TestTable{}
	err = tbl.Read("1", readEntry).RunWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(*readEntry, entry) {
		t.Fatalf("Expected to find entry, got %v", *readEntry)
	}

	err = tbl.DeleteKeysFromMap("1", "MyMap", []interface{}{"A", "B"}).RunWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}

	newEntry := &TestTable{}
	err = tbl.Read("1", newEntry).RunWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}

	v, ok := newEntry.MyMap["A"]
	if ok {
		t.Fatalf("Expected key A to deleted instead has value %v", v)
	}

	v, ok = newEntry.MyMap["B"]
	if ok {
		t.Fatalf("Expected key B to deleted instead has value %v", v)
	}

}