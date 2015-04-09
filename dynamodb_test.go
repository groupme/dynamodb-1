package dynamodb

import (
	"net/url"
	"testing"

	"github.com/groupme/dynamo/dynamotest"
	"golang.org/x/net/context"
)

type MyItem struct {
	Name   string `ddb:"MyItem2,HASH"`
	Weight int
	Height int
}

func TestDynamo(t *testing.T) {
	// Setup local server
	server, err := dynamotest.New()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	// Setup client
	u, _ := url.Parse(server.URL())
	auth := Auth("your-access-key", "your-secret-key")
	endpoint := EndPoint(
		"Local",
		"local",
		u.Host,
		false,
	)
	client := Dial(endpoint, auth, nil)
	ctx := context.Background()

	// DeleteTable
	client.DeleteTable(ctx, "Test")

	// CreateTable
	_, err = client.CreateTable(ctx, "Test", &MyItem{}, 10, 10, nil, nil)
	if err != nil {
		t.Error(err)
	}

	// DescribeTable
	desc, err := client.DescribeTable(ctx, "Test")
	if err != nil {
		t.Fatal(err)
	}
	if desc.TableStatus != "ACTIVE" {
		t.Error("want", "ACTIVE")
		t.Error("got ", desc.TableStatus)
	}

	// Table and Session
	table := client.Table("Test")
	session := table.Session(context.Background())

	// Put
	item := MyItem{Name: "Tom", Weight: 80, Height: 179}
	err = session.Put(&item)
	if err != nil {
		t.Error(err)
	}

	// Get
	newI := MyItem{Name: "Tom"}
	session.Get(&newI, true)
	if newI.Weight != 80 {
		t.Fatal("Failed to put or get the item")
	}

	// Delete
	delI := MyItem{Name: "Tom"}
	err = session.Delete(&delI)
	if err != nil {
		t.Error(err)
	}
	newItem := MyItem{Name: "Tom"}
	err = session.Get(&newItem, true)
	if err == nil {
		t.Error("Item still present", err)
	}

	// Add
	err = session.Add(&item)
	if err != nil {
		t.Error("Can't add the item", err)
	}
	err = session.Add(&item)
	if err == nil {
		t.Error("Shouldn't be able to add two items with the same primary key")
	}

	// PutIf
	err = session.PutIf(&item, &item)
	if err != nil {
		t.Error("PutIf failed although nothing changed")
	}
	item2 := item
	item2.Height = 180
	err = session.PutIf(&item2, &item2)
	if err == nil {
		// should throw an error because height is currently 179
		err = session.PutIf(&item2, &item2)
	}
	err = session.PutIf(&item2, &item)
	if err != nil {
		// should not throw an error because we expect height to be 179
		t.Fatal("putif should not have thrown an error")
	}
}

func BenchmarkTablePut(b *testing.B) {
	server, table := setupBenchmark()
	defer server.Close()

	ctx := context.Background()
	item := MyItem{Name: "Tom", Weight: 80, Height: 179}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.Put(ctx, &item)
	}
}

func BenchmarkTableGet(b *testing.B) {
	server, table := setupBenchmark()
	defer server.Close()

	ctx := context.Background()
	item := MyItem{Name: "Tom", Weight: 80, Height: 179}
	table.Put(ctx, &item)

	b.ReportAllocs()
	b.ResetTimer()
	newItem := MyItem{Name: "Tom"}
	for i := 0; i < b.N; i++ {
		table.Get(ctx, &newItem, true)
	}
}

func setupBenchmark() (*dynamotest.DB, *Table) {
	server, _ := dynamotest.New()
	dbURL, _ := url.Parse(server.URL())
	auth := Auth("your-access-key", "your-secret-key")
	endpoint := EndPoint("DynamoDB Local", "local", dbURL.Host, false)
	client := Dial(endpoint, auth, nil)
	client.CreateTable(context.Background(), "Test", &MyItem{}, 10, 10, nil, nil)
	return server, client.Table("Test")
}
