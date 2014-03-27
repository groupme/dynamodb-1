package dynamodb

import (
	"log"
	"testing"
)

type MyItem struct {
	Name   string `keyType:"HASH"`
	Weight int
	Height int
}

func TestDynamo(t *testing.T) {
	auth := Auth("your-access-key", "your-secret-key")
	endpoint := EndPoint("DynamoDB Local", "home", "localhost:8000", false)
	dbClient := Dial(endpoint, auth, nil)

	dbClient.DeleteTable("Test")
	dbClient.CreateTable("Test", &MyItem{}, 10, 10, nil, nil)
	testT, _ := dbClient.DescribeTable("Test")
	if testT.TableStatus != "ACTIVE" {
		log.Fatal("Error creating table")
	}
	table := dbClient.Table("Test")
	item := MyItem{Name: "Tom", Weight: 80, Height: 179}
	table.Put(&item)
	newI := MyItem{Name: "Tom"}
	table.Get(&newI, true)
	if newI.Weight != 80 {
		log.Fatal("Failed to put or get the item")
	}
	delI := MyItem{Name: "Tom"}
	err := table.Del(&delI)
	if err != nil {
		log.Fatal("Failed to delete the item")
	}
	newItem := MyItem{Name: "Tom"}
	err = table.Get(&newItem, true)
	log.Printf("Item: %v", newItem)
	log.Printf("Error: %v", err)
	if err == nil {
		log.Fatal("Item still present")
	}
	err = table.Add(&item)
	if err != nil {
		log.Fatal("Can't add the item")
	}
	err = table.Add(&item)
	if err == nil {
		log.Fatal("Shouldn't be able to add two items with the same primary key")
	}
	err = table.PutIf(&item, &item)
	if err != nil {
		log.Fatal("putif failed although nothing changed")
	}

	item2 := item
	item2.Height = 180
	err = table.PutIf(&item2, &item2)
	if err == nil {
		// should throw an error because height is currently 179
		err = table.PutIf(&item2, &item2)
	}
	err = table.PutIf(&item2, &item)
	if err != nil {
		// should not throw an error because we expect height to be 179
		log.Fatal("putif should not have thrown an error")
	}
}
