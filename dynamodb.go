// Public Domain (-) 2012-2013 The Go DynamoDB Authors.
// See the Go DynamoDB UNLICENSE file for details.

// Package dynamodb implements a client library for
// interfacing with DynamoDB, Amazon's NoSQL Database
// Service.
//
// To start with, make sure that you have the appropriate
// AWS keys to instantiate an auth object:
//
//     auth := dynamodb.Auth("your-access-key", "your-secret-key")
//
// Next, assuming you are connecting directly to  Amazon's
// servers, choose one of the predefined endpoints like
// USEast1, EUWest1, etc.
//
//     endpoint := dynamodb.USWest2
//
// If you happen to be connecting to a region which hasn't
// been defined yet or want to connect to a DynamoDB Local
// instance for development, define your own custom
// endpoint, e.g.
//
//     endpoint := dynamodb.EndPoint("Test", "local", "localhost:8000", false)
//
// You are now ready to Dial the endpoint and instantiate a client:
//
//     client := dynamodb.Dial(endpoint, auth, nil)
//
// The third parameter is normally nil to Dial lets you specify a custom
// http.Transport should you need one. This is particularly
// useful in PaaS environments like Google App Engine where
// you might not be able use the standard transport. If you
// specify nil
//
// For example, on a restricted environment like Google App
// Engine, where the standard transport isn't available, you
// can use the transport they expose via the
// appengine/urlfetch package:
//
//     transport := &urlfetch.Transport{
//         Context:  appengine.NewContext(req),
//         Deadline: 10 * time.Second,
//     }
//
//     client := dynamodb.Dial(endpoint, auth, transport)
//
// In high throughput applications, you may see increased performance by
// increasing the number of connections available to the Client, e.g.
//
// 		transport := &http.Transport{MaxIdleConnsPerHost: 64}
//		client := dynamodb.Dial(endpoint, auth, transport)
//
// The heart of the package revolves around the Client. You
// instantiate it by calling Dial with an endpoint and
// authentication details, e.g.
//
//		import "dynamodb"
//
// 		auth := dynamodb.Auth("your-access-key", "your-secret-key")
//		client := dynamodb.Dial(dynamodb.USWest1, auth, nil)
//
//		query := table.Query()
//		query.Sort('-').Limit(20)
//
//		resp, err := client.Call("CreateTable", dynamodb.Map{
//         "TableName": "mytable",
//         "ProvisionedThroughput": dynamodb.Map{
//             "ReadCapacityUnits": 5,
//             "WriteCapacityUnits": 5,
//         },
//     })
//
package dynamodb

// TODO:
// query + index creation & management
// ERRORS and error handling
// (batch write / get)

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"strings"
	"time"

	"golang.org/x/net/context"
)

// ErrRetryExhausted is returned when MaxRetry is reached
var ErrRetryExhausted = errors.New("dynamodb: retry exhausted")

const (
	iso8601 = "20060102T150405Z"
)

type endpoint struct {
	name   string
	region string
	host   string
	tls    bool
	url    string
}

func (e endpoint) String() string {
	return fmt.Sprintf("<%s: %s>", e.name, e.host)
}

// EndPoint creates an endpoint struct for use with Dial.
// It's useful when using a local mock DynamoDB server, e.g.
//
//     dev := EndPoint("dev", "eu-west-1", "localhost:9091", false)
//
// Otherwise, unless Amazon upgrade their infrastructure,
// the predefined endpoints like USEast1 should suffice.
func EndPoint(name, region, host string, tls bool) endpoint {
	var url string
	if tls {
		url = "https://" + host + "/"
	} else {
		url = "http://" + host + "/"
	}
	return endpoint{
		name:   name,
		region: region,
		host:   host,
		tls:    tls,
		url:    url,
	}
}

// Current DynamoDB endpoints within Amazon's
// infrastructure.
var (
	APNorthEast1 = EndPoint("Tokyo", "ap-northeast-1", "dynamodb.ap-northeast-1.amazonaws.com", true)
	APSouthEast1 = EndPoint("Singapore", "ap-southeast-1", "dynamodb.ap-southeast-1.amazonaws.com", true)
	APSouthEast2 = EndPoint("Sydney", "ap-southeast-2", "dynamodb.ap-southeast-2.amazonaws.com", true)
	EUWest1      = EndPoint("Ireland", "eu-west-1", "dynamodb.eu-west-1.amazonaws.com", true)
	SAEast1      = EndPoint("Sao Paulo", "sa-east-1", "dynamodb.sa-east-1.amazonaws.com", true)
	USEast1      = EndPoint("N. Virginia", "us-east-1", "dynamodb.us-east-1.amazonaws.com", true)
	USWest1      = EndPoint("Oregon", "us-west-1", "dynamodb.us-west-1.amazonaws.com", true)
	USWest2      = EndPoint("Northern California", "us-west-2", "dynamodb.us-west-2.amazonaws.com", true)
)

type auth struct {
	accessKey string
	secretKey []byte
}

func Auth(accessKey, secretKey string) auth {
	return auth{
		accessKey: accessKey,
		secretKey: []byte("AWS4" + secretKey),
	}
}

// Error represents all responses to DynamoDB API calls with
// an HTTP status code other than 200.
type Error struct {
	Body       []byte
	StatusCode int
}

// Error satisfies the default error interface and
// automatically tries to parse any JSON response that
// DynamoDB may have sent in order to provide a useful error
// message.
func (e Error) Error() string {
	errtype, message := e.Info()
	if errtype == "" || message == "" {
		return fmt.Sprintf("dynamodb: error with http status code %d", e.StatusCode)
	}
	return fmt.Sprintf("dynamodb: %s: %s", errtype, message)
}

// Info tries to parse the error type and message from the
// JSON body that DynamoDB may have responded with.
func (e Error) Info() (errtype string, message string) {
	if e.Body == nil {
		return
	}
	info := map[string]string{}
	if json.Unmarshal(e.Body, &info) != nil {
		return
	}
	errtype = info["__type"]
	idx := strings.Index(errtype, "#")
	if idx > 0 {
		errtype = errtype[idx+1:]
	}
	return errtype, info["message"]
}

// Retry returns error is safe to retry
func (e Error) Retry() bool {
	errtype, _ := e.Info()
	switch errtype {
	case "InternalServerError":
		return true
	case "ProvisionedThroughputExceededException":
		return true
	case "ServiceUnavailableException":
		return true
	default:
		return false
	}
}

// Item specifies an interface for encoding and decoding a
// struct into the custom JSON format required by DynamoDB.
// The dynamodb-marshal tool, that accompanies this package
// in the cmd directory, is capable of auto-generating
// optimised code to satisfy this interface.
//
// To make use of it, put the structs you want to optimise
// in a file, e.g. model.go
//
//     package campaign
//
//     type Contribution struct {
//         Email string
//         On    time.Time
//         Tags  []string
//     }
//
// Then run the tool from the command line, e.g.
//
//    $ dynamodb-marshal model.go
//
// This will generate a model_marshal.go file which would
// contain implementations for the Encode() and Decode()
// methods that satisfy the Item interface, e.g.
//
//     package campaign
//
//     func (c *Contribution) Encode(buf *bytes.Buffer) {
//         // optimised implementation ...
//     }
//
//     func (c *Contribution) Decode(data map[string]map[string]interface{}) {
//         // optimised implementation ...
//     }
//
// You can expect the performance of the optimised version
// to be somewhere between 1.5x to 10x the reflection-based
// default implementation.
type Item interface {
	Encode(buf *bytes.Buffer)
	Decode(data ResponseItem)
}

type Key struct {
}

// Map provides a shortcut for the abstract data type used
// in all DynamoDB API calls.
type Map map[string]interface{}

type Query struct {
	table      *Table
	cursor     Key
	descending bool
	eventually bool
	index      string
	limit      int
	selector   string
}

func (q *Query) Sort(order byte) *Query {
	if order == '+' {
		q.descending = false
	} else if order == '-' {
		q.descending = true
	}
	return q
}

// func (q *Query) EventuallyConsistent() *Query {
// 	q.eventually = true
// 	return q
// }

func (q *Query) Index(name string) *Query {
	q.index = name
	return q
}

func (q *Query) Only(attrs ...string) *Query {
	return q
}

func (q *Query) Limit(n int) *Query {
	q.limit = n
	return q
}

func (q *Query) Run(consistent bool) error {
	// q.table.client.RawRequest("Query", payload)
	return nil
}

func (q *Query) Select(mechanism string) *Query {
	q.selector = mechanism
	return q
}

func (q *Query) WithCursor(key Key) *Query {
	q.cursor = key
	return q
}

// Session helps perform multiple Table operations on a Context
type Session struct {
	ctx   context.Context
	table *Table
}

func (s *Session) Get(item interface{}, consistent bool) error {
	return s.table.Get(s.ctx, item, consistent)
}

func (s *Session) Delete(item interface{}) error {
	return s.table.Delete(s.ctx, item)
}

func (s *Session) Put(item interface{}) error {
	return s.table.Put(s.ctx, item)
}

func (s *Session) PutIf(newItem, oldItem interface{}) error {
	return s.table.PutIf(s.ctx, newItem, oldItem)
}

func (s *Session) Add(item interface{}) error {
	return s.table.Add(s.ctx, item)
}

// Table operates on a named DynamoDB table
type Table struct {
	client *Client
	name   string
}

// Session creates a new Session for Context and Table
func (t *Table) Session(ctx context.Context) *Session {
	return &Session{ctx: ctx, table: t}
}

// Get fetches and populates the item.
func (t *Table) Get(
	ctx context.Context,
	item interface{},
	consistent bool,
) error {
	payload := &bytes.Buffer{}
	encodedKey := bytes.Buffer{}
	encode(item, &encodedKey, true, false)
	fmt.Fprintf(
		payload,
		`{"TableName":"%s", "Key":%s, "ConsistentRead":%t}`,
		t.name,
		encodedKey.String(),
		consistent,
	)
	resp, err := t.client.CallBytes(ctx, "GetItem", payload.Bytes())
	if err != nil {
		return err
	}
	var getData GetItem
	err = json.Unmarshal(resp, &getData)
	if getData.Item == nil {
		return errors.New("Item does not exist")
	}
	decode(item, getData.Item)
	return err
}

func (t *Table) Delete(ctx context.Context, item interface{}) error {
	payload := &bytes.Buffer{}
	encodedKey := bytes.Buffer{}
	encode(item, &encodedKey, true, false)
	fmt.Fprintf(
		payload,
		`{"TableName":"%s", "Key":%s}`,
		t.name,
		encodedKey.String(),
	)
	_, err := t.client.CallBytes(ctx, "DeleteItem", payload.Bytes())
	return err
}

// Put puts item
func (t *Table) Put(ctx context.Context, item interface{}) error {
	payload := &bytes.Buffer{}
	encodedItem := bytes.Buffer{}
	encode(item, &encodedItem, false, false)
	fmt.Fprintf(
		payload,
		`{"TableName":"%s", "Item":%s}`,
		t.name,
		encodedItem.String(),
	)
	_, err := t.client.CallBytes(ctx, "PutItem", payload.Bytes())
	return err
}

// PutIf only puts if item hasn't changed
func (t *Table) PutIf(ctx context.Context, newItem, oldItem interface{}) error {
	payload := &bytes.Buffer{}
	encodedNewItem := bytes.Buffer{}
	encodedOldItem := bytes.Buffer{}
	encode(newItem, &encodedNewItem, false, false)
	encode(oldItem, &encodedOldItem, false, true)
	fmt.Fprintf(
		payload,
		`{"TableName":"%s", "Item":%s, "Expected":%s}`,
		t.name,
		encodedNewItem.String(),
		encodedOldItem.String(),
	)
	_, err := t.client.CallBytes(ctx, "PutItem", payload.Bytes())
	return err
}

// Add puts item if the key doesn't already exist
func (t *Table) Add(ctx context.Context, item interface{}) error {
	payload := &bytes.Buffer{}
	encodedItem := bytes.Buffer{}
	encode(item, &encodedItem, false, false)
	fields, _ := getTypeInfo(item)
	var keyStrings []string
	for _, field := range fields {
		if field.keyType != "" {
			keyStrings = append(keyStrings, fmt.Sprintf(`"%s": {"Exists":false}`, field.name))
		}
	}
	fmt.Fprintf(
		payload,
		`{"TableName":"%s", "Item":%s, "Expected":{%s}}`,
		t.name,
		encodedItem.String(),
		strings.Join(keyStrings, ", "),
	)
	_, err := t.client.CallBytes(ctx, "PutItem", payload.Bytes())
	return err
}

// TODO implement me
func (t *Table) Query() *Query {
	return &Query{}
}

// TODO implement me
func (t *Table) Update(key Key) error {
	// return c.RawRequest("UpdateItem", payload)
	return nil
}

const (
	RetryDefault int = 5
	RetryForever     = -1
	RetryNever       = 0
)

// Dial creates a new Client
func Dial(region endpoint, creds auth, transport http.RoundTripper) *Client {
	if transport == nil {
		transport = &http.Transport{}
	}
	return &Client{
		Retry:     RetryDefault,
		auth:      creds,
		endpoint:  region,
		web:       &http.Client{Transport: transport},
		transport: transport,
	}
}

// Client communicates over HTTP
type Client struct {
	// Retry defines retry behavior.
	// If > 0, Client exponentially backs off and retrys to limit.
	// If 0, Client does not retry.
	// If -1, Client retries forever.
	Retry int

	auth      auth
	endpoint  endpoint
	web       *http.Client
	transport http.RoundTripper
}

// memoize tables
var tables map[string]*Table

// Table gets or initializes *Table
func (c *Client) Table(name string) *Table {
	if tables[name] != nil {
		return tables[name]
	}
	return &Table{
		client: c,
		name:   name,
	}
}

func (c *Client) CreateTable(
	ctx context.Context,
	name string,
	schemaItem interface{},
	readCapacity int,
	writeCapacity int,
	globalIndexes []GlobalIndex,
	localIndexes []Index,
) (*TableDesc, error) {
	var keys []KeyItem
	var attrDefs []AttributeDefinition
	fields, _ := getTypeInfo(schemaItem)
	for _, field := range fields {
		if field.keyType != "" {
			keys = append(
				keys,
				KeyItem{
					AttributeName: field.name,
					KeyType:       field.keyType,
				},
			)

			attrDefs = append(
				attrDefs,
				AttributeDefinition{
					AttributeName: field.name,
					AttributeType: kindMap[field.kind],
				},
			)
		}
	}

	payload, err := c.Call(
		ctx,
		"CreateTable",
		TableCreate{
			attrDefs,
			keys,
			globalIndexes,
			localIndexes,
			ProvisionedThroughput{readCapacity, writeCapacity},
			name,
		},
	)
	if err != nil {
		return nil, err
	}

	var t TableResponseWrapper
	err = json.Unmarshal(payload, &t)
	return &t.TableDescription, err
}

func (c *Client) ListTables(
	ctx context.Context,
	limit int,
	cursor string,
) (tables TablesList, err error) {
	args := Map{}
	if limit != 0 {
		args["Limit"] = limit
	}
	if cursor != "" {
		args["ExclusiveStartTable"] = cursor
	}
	payload, err := c.Call(ctx, "ListTables", args)
	if err != nil {
		return
	}
	err = json.Unmarshal(payload, &tables)
	return
}

func (c *Client) DescribeTable(
	ctx context.Context,
	name string,
) (*TableDesc, error) {
	payload, err := c.Call(ctx, "DescribeTable", Map{"TableName": name})
	if err != nil {
		return nil, err
	}
	var t TableDescWrapper
	err = json.Unmarshal(payload, &t)
	return &t.Table, err
}

func (c *Client) DeleteTable(
	ctx context.Context,
	name string,
) (*TableDesc, error) {
	payload, err := c.Call(ctx, "DeleteTable", Map{"TableName": name})
	if err != nil {
		return nil, err
	}
	var t TableResponseWrapper
	err = json.Unmarshal(payload, &t)
	return &t.TableDescription, err
}

func (c *Client) UpdateTable(
	ctx context.Context,
	name string,
	ReadCapacityUnits int,
	WriteCapacityUnits int,
	IndexUpdates []GlobalIndexUpdate,
) (*TableDesc, error) {
	tabUp := TableUpdate{
		IndexUpdates,
		ProvisionedThroughput{ReadCapacityUnits, WriteCapacityUnits},
		name,
	}
	payload, err := c.Call(ctx, "UpdateTable", tabUp)
	if err != nil {
		return nil, err
	}
	var t TableResponseWrapper
	err = json.Unmarshal(payload, &t)
	return &t.TableDescription, err
}

// Call makes request with params marshalled to JSON
func (c *Client) Call(
	ctx context.Context,
	method string,
	params interface{},
) (payload []byte, err error) {
	if params == nil {
		payload = []byte{'{', '}'}
	} else {
		payload, err = json.Marshal(params)
		if err != nil {
			return nil, err
		}
	}
	return c.CallBytes(ctx, method, payload)
}

// CallBytes makes and retries request with raw bytes
func (c *Client) CallBytes(
	ctx context.Context,
	method string,
	payload []byte,
) ([]byte, error) {
	attempt := 0
	for {
		b, err := c.callRaw(ctx, method, payload)
		if err == nil {
			return b, nil
		}

		if e, ok := err.(Error); ok {
			if e.Retry() {
				attempt++
				if attempt >= c.Retry {
					return nil, ErrRetryExhausted
				}
				c.backoff(attempt)
			} else {
				return nil, e
			}
		} else {
			return nil, err
		}
	}
}

const backoffFactor = 50 * time.Millisecond

// block exponentially where exponent is attempt
func (c *Client) backoff(attempt int) {
	<-time.After(time.Duration(math.Pow(2, float64(attempt))) * backoffFactor)
}

func (c *Client) callRaw(
	ctx context.Context,
	method string,
	payload []byte,
) ([]byte, error) {
	// new request
	req, err := c.newRequest(method, payload)
	if err != nil {
		return nil, err
	}

	// send request
	resp, err := c.do(ctx, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// decode response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		err := Error{
			Body:       body,
			StatusCode: resp.StatusCode,
		}
		log.Printf("%v", string(body))
		return nil, err
	}
	return body, nil
}

// TODO: use a buffer to reduce string allocations like bmizerany/aws4
func (c *Client) newRequest(
	method string,
	payload []byte,
) (*http.Request, error) {
	req, err := http.NewRequest("POST", c.endpoint.url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	hasher := sha256.New()
	hasher.Write(payload)
	datetime := time.Now().UTC().Format(iso8601)
	date := datetime[:8]
	method = "DynamoDB_20120810." + method
	canonicalReq := "POST\n/\n\ncontent-type:application/x-amz-json-1.0\nhost:" + c.endpoint.host + "\nx-amz-date:" + datetime + "\nx-amz-target:" + method + "\n\ncontent-type;host;x-amz-date;x-amz-target\n" + hex.EncodeToString(hasher.Sum(nil))
	hasher.Reset()
	hasher.Write([]byte(canonicalReq))
	post := "AWS4-HMAC-SHA256\n" + datetime + "\n" + date + "/" + c.endpoint.region + "/dynamodb/aws4_request\n" + hex.EncodeToString(hasher.Sum(nil))
	sig := hex.EncodeToString(doHMAC(doHMAC(doHMAC(doHMAC(doHMAC(c.auth.secretKey, date), c.endpoint.region), "dynamodb"), "aws4_request"), post))
	credential := "AWS4-HMAC-SHA256 Credential=" + c.auth.accessKey + "/" + date + "/" + c.endpoint.region + "/dynamodb/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=" + sig
	req.Header.Set("Authorization", credential)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("Host", c.endpoint.host)
	req.Header.Set("X-Amz-Date", datetime)
	req.Header.Set("X-Amz-Target", method)
	return req, nil
}

// do sends request and enforces context deadline
func (c *Client) do(
	ctx context.Context,
	req *http.Request,
) (*http.Response, error) {
	// start
	errc := make(chan error, 1)
	var resp *http.Response
	go func() {
		var err error
		resp, err = c.web.Do(req)
		errc <- err
	}()

	// pick whichever happens first
	select {
	case <-ctx.Done():
		// interface check here to accept less capable http.RoundTripper
		type canceler interface {
			CancelRequest(*http.Request)
		}
		if tr, ok := c.transport.(canceler); ok {
			tr.CancelRequest(req)
		}
		<-errc
		return nil, ctx.Err()
	case err := <-errc:
		return resp, err
	}
}

func doHMAC(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}
