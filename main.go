package main

import (
	"github.com/urfave/cli"
	"os"
	"fmt"
	"context"
	"cloud.google.com/go/storage"
	"log"
	"io/ioutil"
	"github.com/pkg/errors"
)

type Store struct {
	BucketName string
	Client *storage.Client
	Bucket *storage.BucketHandle
	Context context.Context
}

func main() {
	app := cli.NewApp()
	app.Name = "Google Cloud Storage Uploader"

	app.Flags = []cli.Flag{
		cli.BoolFlag{
			EnvVar: "DEBUG",
			Name:   "debug",
			Usage:  "Debug",
		},
		cli.StringFlag{
			EnvVar: "BUCKET_NAME",
			Name:   "bucket_name",
			Usage: "Bucket name",
		},
		cli.StringFlag{
			EnvVar: "BUCKET_FOLDER_PATH",
			Name: "bucket_folder_path",
			Usage: "upload to which bucket folder path",
			Value: "test",
		},
		cli.StringFlag{
			EnvVar: "FILE_NAME",
			Name: "file_name",
			Usage: "name of local file",
			Value: "testFile.png",
		},
	}

	app.Action = func(c *cli.Context) error {
		fmt.Printf("Flags: Debug: %t, Bucket Name: %s\n", c.Bool("debug"), c.String("bucket_name"))

		ctx := context.Background()

		store := Store{
			BucketName: c.String("bucket_name"),
			Context: ctx,
		}

		client, err := storage.NewClient(ctx)
		if err != nil {
			log.Fatalf("Failed to create client: %#v", err)
		}
		store.Client = client
		store.Bucket = client.Bucket(store.BucketName)

		b, err := ioutil.ReadFile(c.String("file_name"))
		if err != nil {
			log.Fatalf("Fail to read file: %#v", err)
		}
		if err := store.writeBytes(b, c.String("bucket_folder_path"), c.String("file_name")); err != nil {
			log.Fatalf("Error on write bytes: %#v", err)
		}
		return nil
	}
	app.Run(os.Args)
}

func (s *Store) writeBytes(b []byte, path string, fileName string) error {
	obj := s.Bucket.Object(fmt.Sprintf("%s/%s", path, fileName))
	w := obj.NewWriter(s.Context)

	_, err := w.Write(b)
	if err != nil {
		return errors.Wrap(err, "Fail to write file")
	}
	if err := w.Close(); err != nil {
		return errors.Wrap(err, "Fail to close writer")
	}

	return nil
}
