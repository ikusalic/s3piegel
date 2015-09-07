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

	return &Bucket{
		Name:   u.Host,
		Prefix: u.Path[1:],
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
			out <- *obj.Key
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

func getDestinationKey(sourceBucket, destBucket *Bucket, key string) string {
	// If we mirror s3://a/prefixA/ to s3://b/prefixB, we want the key
	// s3://a/prefixA/key to be copied to s3://b/prefixB/key
	key = key[len(sourceBucket.Prefix):]

	// Avoid double / in keys
	if strings.HasSuffix(destBucket.Prefix, "/") && strings.HasPrefix(key, "/") {
		key = key[1:]
	}

	return destBucket.Prefix + key
}

func copyKey(sourceBucket, destBucket *Bucket, key string) error {
	req := s3.CopyObjectInput{
		Bucket:     aws.String(destBucket.Name),
		CopySource: aws.String(sourceBucket.Name + "/" + key),
		Key:        aws.String(getDestinationKey(sourceBucket, destBucket, key)),
	}

	log.Printf("%s → %s", key, *req.Key)

	if *dryRun {
		return nil
	}

	_, err := destBucket.Client.CopyObject(&req)

	if err != nil {
		return err
	}

	return nil
}

var sourceRegion = flag.String("sourceRegion", "", "Region of the source S3 bucket (auto detected if not specified)")
var destRegion = flag.String("destRegion", "", "Region of the destination S3 bucket (auto detected if not specified)")
var dryRun = flag.Bool("dryRun", false, "Don't actually do the copy")

func runCopy(sourceBucket, destBucket *Bucket) error {
	sourceKeys := make(chan string)
	listKeysError := make(chan error, 1)

	go func() {
		listKeysError <- listBucket(sourceBucket, sourceKeys)
	}()

	if err := copyKeys(sourceBucket, destBucket, sourceKeys); err != nil {
		return fmt.Errorf("Error while copying data: %s", err)
	}

	if err := <-listKeysError; err != nil {
		return fmt.Errorf("Error while listing keys in source bucket: %s", err)
	}

	return nil
}

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
