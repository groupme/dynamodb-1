package dynamodb

type ProvisionedThroughput struct {
	ReadCapacityUnits  int
	WriteCapacityUnits int
}

type ProvisionedThroughputDesc struct {
	ProvisionedThroughput
	LastDecreaseDateTime   float64
	LastIncreaseDateTime   float64
	NumberOfDecreasesToday int
}

type KeyItem struct {
	AttributeName string
	KeyType       string
}
type AttributeDefinition struct {
	AttributeName string
	AttributeType string
}
type Projection struct {
	NonKeyAttributes []string
	ProjectionType   string
}
type Index struct {
	IndexName  string
	KeySchema  []KeyItem
	Projection Projection
}
type GlobalIndex struct {
	Index
	ProvisionedThroughput ProvisionedThroughput
}

type GlobalIndexUpdate struct {
	Update struct {
		IndexName             string
		ProvisionedThroughput ProvisionedThroughput
	}
}

type TablesList struct {
	LastEvaluatedTableName string
	TableNames             []string
}

type TableUpdate struct {
	GlobalSecondaryIndexUpdates []GlobalIndexUpdate
	ProvisionedThroughput       ProvisionedThroughput
	TableName                   string
}

type TableCreate struct {
	AttributeDefinitions   []AttributeDefinition
	KeySchema              []KeyItem
	GlobalSecondaryIndexes []GlobalIndex
	LocalSecondaryIndexes  []Index
	ProvisionedThroughput  ProvisionedThroughput
	TableName              string
}

type TableDesc struct {
	AttributeDefinitions   []AttributeDefinition
	CreationDateTime       float64
	GlobalSecondaryIndexes []struct {
		GlobalIndex
		IndexSizeBytes int
		IndexStatus    string
		ItemCount      int
		// is this king of overriding legal? See GlobalSecondaryIndex
		ProvisionedThroughput ProvisionedThroughputDesc
	}
	ItemCount             int
	KeySchema             []KeyItem
	LocalSecondaryIndexes []struct {
		Index
		IndexSizeBytes int
		ItemCount      int
	}
	ProvisionedThroughput ProvisionedThroughputDesc
	TableName             string
	TableSizeBytes        int
	TableStatus           string
}

type TableDescWrapper struct {
	Table TableDesc
}

type TableResponseWrapper struct {
	TableDescription TableDesc
}

type DynamoItem map[string]map[string]interface{}
type DynamoKey DynamoItem

type GetItem struct {
	Item DynamoItem
}
