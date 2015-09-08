package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
)

type Bucket struct {
	Name   string
	Prefix string
	Client *s3.S3
}

func NewBucket(bucketUrl string, region string) (*Bucket, error) {
	u, err := url.Parse(bucketUrl)

	if err != nil {
		return nil, err
	}

	if u.Scheme != "s3" {
		return nil, fmt.Errorf("Invalid S3 URL scheme: %s", u.Scheme)
	}

	if region == "" {
		req := s3.GetBucketLocationInput{
			Bucket: aws.String(u.Host),
		}

		res, err := s3.New(nil).GetBucketLocation(&req)

		if err != nil {
			return nil, fmt.Errorf("Error while retrieving bucket location: %s", err)
		}

		region = *res.LocationConstraint
	}

	client := s3.New(aws.NewConfig().WithRegion(region))

	prefix := u.Path[1:]

	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return &Bucket{
		Name:   u.Host,
		Prefix: prefix,
		Client: client,
	}, nil
}

func listBucket(bucket *Bucket, out chan string) error {
	defer close(out)

	listRequest := s3.ListObjectsInput{
		Bucket: aws.String(bucket.Name),
		Prefix: aws.String(bucket.Prefix),
	}

	callback := func(result *s3.ListObjectsOutput, lastPage bool) bool {
		for _, obj := range result.Contents {
			out <- (*obj.Key)[len(bucket.Prefix):]
		}

		return true
	}

	return bucket.Client.ListObjectsPages(&listRequest, callback)
}

func copyKeys(sourceBucket, destBucket *Bucket, keys <-chan string) error {
	const concurrency = 50
	copyErrors := make(chan error, concurrency)
	wg := sync.WaitGroup{}

	worker := func() error {
		defer wg.Done()

		for key := range keys {
			if err := copyKey(sourceBucket, destBucket, key); err != nil {
				return fmt.Errorf("Error while copying key %s: %s", key, err)
			}
		}

		return nil
	}

	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			copyErrors <- worker()
		}()
	}

	wg.Wait()

	var err error

	for i := 0; i < concurrency; i++ {
		if copyError := <-copyErrors; copyError != nil {
			err = copyError
		}
	}

	return err
}

func copyKey(sourceBucket, destBucket *Bucket, key string) error {
	sourceKey := sourceBucket.Prefix + key
	req := s3.CopyObjectInput{
		Bucket:     aws.String(destBucket.Name),
		CopySource: aws.String(sourceBucket.Name + "/" + sourceKey),
		Key:        aws.String(destBucket.Prefix + key),
	}

	log.Printf("%s → %s", sourceKey, *req.Key)

	if *dryRun {
		return nil
	}

	_, err := destBucket.Client.CopyObject(&req)

	if err != nil {
		return err
	}

	return nil
}

func deleteKeys(bucket *Bucket, keys <-chan string) error {
	buffer := make([]string, 0, 100)

	doDelete := func() error {
		objects := make([]*s3.ObjectIdentifier, len(buffer))

		for i, v := range buffer {
			objects[i] = &s3.ObjectIdentifier{
				Key: aws.String(bucket.Prefix + v),
			}
		}

		req := s3.DeleteObjectsInput{
			Bucket: &bucket.Name,
			Delete: &s3.Delete{
				Objects: objects,
			},
		}

		if _, err := bucket.Client.DeleteObjects(&req); err != nil {
			return err
		}

		return nil
	}

	for key := range keys {
		buffer = append(buffer, key)

		if len(buffer) == cap(buffer) {
			if err := doDelete(); err != nil {
				return err
			}
		}
	}

	if len(buffer) > 0 {
		if err := doDelete(); err != nil {
			return err
		}
	}

	return nil
}

func compareKeys(sourceKeys, destKeys <-chan string, toCopy, toDelete chan<- string) {
	sourceFinished := false
	destFinished := false

	var sourceCurrent *string
	var destCurrent *string

	buffer := func(from <-chan string, to **string, finished *bool) {
		if *finished || *to != nil {
			return
		}

		key, ok := <-from

		*finished = !ok

		if ok {
			*to = &key
		}
	}

	type Action int

	const (
		Copy Action = iota
		Skip
		Delete
	)

	cmp := func(a, b *string) Action {
		if a == nil && b == nil {
			return Skip
		}

		if a == nil {
			return Delete
		}

		if b == nil {
			return Copy
		}

		if *a == *b {
			return Skip
		}

		if *a > *b {
			return Delete
		}

		return Copy
	}

	for {
		buffer(sourceKeys, &sourceCurrent, &sourceFinished)
		buffer(destKeys, &destCurrent, &destFinished)

		if sourceFinished && destFinished {
			close(toCopy)
			close(toDelete)
			return
		}

		action := cmp(sourceCurrent, destCurrent)

		if action == Skip && *overwrite {
			action = Copy
		}

		switch action {
		case Copy:
			toCopy <- *sourceCurrent
			sourceCurrent = nil
		case Skip:
			sourceCurrent = nil
			destCurrent = nil
		case Delete:
			toDelete <- *destCurrent
			destCurrent = nil
		}
	}
}

func runCopy(sourceBucket, destBucket *Bucket) error {
	sourceKeys := make(chan string)
	destKeys := make(chan string)
	keysToCopy := make(chan string)
	keysToDelete := make(chan string)
	listSourceKeysError := make(chan error, 1)
	listDestKeysError := make(chan error, 1)
	copyError := make(chan error, 1)
	deleteError := make(chan error, 1)

	go func() {
		listSourceKeysError <- listBucket(sourceBucket, sourceKeys)
	}()

	go func() {
		listDestKeysError <- listBucket(destBucket, destKeys)
	}()

	go func() {
		copyError <- copyKeys(sourceBucket, destBucket, keysToCopy)
	}()

	go func() {
		deleteError <- deleteKeys(destBucket, keysToDelete)
	}()

	compareKeys(sourceKeys, destKeys, keysToCopy, keysToDelete)

	for {
		if listSourceKeysError == nil && listDestKeysError == nil && copyError == nil && deleteError == nil {
			return nil
		}

		select {
		case err := <-listSourceKeysError:
			if err != nil {
				return fmt.Errorf("Error while listing keys in source bucket: %s", err)
			}

			listSourceKeysError = nil
		case err := <-listDestKeysError:
			if err != nil {
				return fmt.Errorf("Error while listing keys in destination bucket: %s", err)
			}

			listDestKeysError = nil
		case err := <-copyError:
			if err != nil {
				return fmt.Errorf("Error while copying keys to destination bucket: %s", err)
			}

			copyError = nil
		case err := <-deleteError:
			if err != nil {
				return fmt.Errorf("Error while deleting keys from destination bucket: %s", err)
			}

			deleteError = nil
		}
	}
}

var sourceRegion = flag.String("sourceRegion", "", "Region of the source S3 bucket (auto detected if not specified)")
var destRegion = flag.String("destRegion", "", "Region of the destination S3 bucket (auto detected if not specified)")
var dryRun = flag.Bool("dryRun", false, "Don't actually do the copy")
var overwrite = flag.Bool("overwrite", false, "Force copy even if destination key already exists")

func main() {
	flag.Usage = func() {
		const usage = `Usage: %s SOURCE DEST

Mirrors a S3 folder to another one, potentially in a different bucket.

SOURCE and DEST should be S3 URLs in the form s3://bucket-name/prefix .
Objects metadata is preserved during the copy.

Additional command line options:
`

		fmt.Fprintf(os.Stderr, usage, os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() != 2 {
		flag.Usage()
		os.Exit(1)
	}

	// We only use the default client to fetch bucket location.
	defaults.DefaultConfig.Region = aws.String("us-east-1")
	defaults.DefaultConfig.S3ForcePathStyle = aws.Bool(true)

	sourceUrl := flag.Arg(0)
	destUrl := flag.Arg(1)

	sourceBucket, err := NewBucket(sourceUrl, *sourceRegion)

	if err != nil {
		log.Fatalf("Error while configuring source bucket: %s", err)
	}

	destBucket, err := NewBucket(destUrl, *destRegion)

	if err != nil {
		log.Fatalf("Error while configuring destination bucket: %s", err)
	}

	if err := runCopy(sourceBucket, destBucket); err != nil {
		log.Fatalf(err.Error())
	}
}
