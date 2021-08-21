package main

import (
	//"fmt"
	"context"
	//"bufio"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

func main() {
	// get input from s3 bucket
	ctx := context.Background()
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-west-2"),
	})
	if err != nil {
		panic("Fail to create new session")
	}

	bucket, err := s3blob.OpenBucket(ctx, sess, "xxxx", nil)
	if err != nil {
		panic("Fail to open bucket")
	}
	defer bucket.Close()

	opt := blob.ListOptions{Prefix: "books/pg"}
	it := bucket.List(&opt)
	files := []string{}
	for obj, _ := it.Next(ctx); obj != nil; obj, _ = it.Next(ctx) {
		files = append(files, obj.Key)
	}

	r, err := bucket.NewReader(ctx, files[0], nil)
	if err != nil {
		panic("Fail to open reader")
	}
	defer r.Close()

	file, err := os.OpenFile("/home/ubuntu/test/test.txt", os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic("Fail to open file")
	}
	defer file.Close()

	// w := bufio.NewWriter(file)

	if _, err := io.Copy(file, r); err != nil {
		panic("Fail to write data")
	}
}
