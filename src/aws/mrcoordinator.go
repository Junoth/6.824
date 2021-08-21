package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"context"
	"time"

	"6.824/mr"
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

	bucket, err := s3blob.OpenBucket(ctx, sess, "xxxxx", nil)
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

	// pass into all input files and reduce number(fix as 10)
	m := mr.MakeCoordinator(files, 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
