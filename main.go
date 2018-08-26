package main

import (
	"context"
	"fmt"
	"os"

	"encoding/json"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"github.com/urfave/cli"
	"go.uber.org/zap"
)

type Store struct {
	BucketName string
	Client     *storage.Client
	Bucket     *storage.BucketHandle
	Context    context.Context
}

type ReceivedFile struct {
	ID          uint64
	Name        string
	FileSize    int
	Data        []byte
	Path        string
	CallbackKey string
}

type CallbackMQ struct {
	ID       uint64
	ImageURL string
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
			Usage:  "Bucket name",
		},
		cli.StringFlag{
			EnvVar: "BUCKET_FOLDER_PATH",
			Name:   "bucket_folder_path",
			Usage:  "upload to which bucket folder path",
			Value:  "test",
		},
		cli.StringFlag{
			EnvVar: "AMQP_USER_NAME",
			Name:   "amqp_user_name",
			Value:  "guest",
		},
		cli.StringFlag{
			EnvVar: "AMQP_PASSWORD",
			Name:   "amqp_password",
			Value:  "guest",
		},
		cli.StringFlag{
			EnvVar: "AMQP_IP",
			Name:   "amqp_ip",
			Value:  "localhost",
		},
		cli.StringFlag{
			EnvVar: "AMQP_PORT",
			Name:   "amqp_port",
			Value:  "5672",
		},
		cli.StringFlag{
			EnvVar: "AMQP_LISTEN_KEY",
			Name:   "amqp_listen_key",
			Value:  "FILES",
		},
		cli.StringFlag{
			EnvVar: "AMQP_RESPONSE_KEY",
			Name:   "amqp_response_key",
			Value:  "UPLOAD_COMPLETED",
		},
	}

	app.Action = func(c *cli.Context) error {
		logger, _ := zap.NewProduction()
		defer logger.Sync()

		sugar := logger.Sugar()
		sugar.Infow("Flags",
			"Debug", c.Bool("debug"),
			"Bucket Name", c.String("bucket_name"),
			"Bucket Folder Path", c.String("bucket_folder_path"),
			"AMQP User Name", c.String("amqp_user_name"),
			"AMQP Password", c.String("amqp_password"),
			"AMQP IP", c.String("amqp_ip"),
			"AMQP Port", c.String("amqp_port"),
			"AMQP Listen Key", c.String("amqp_listen_key"),
			"AMQP Response Key", c.String("amqp_response_key"),
		)

		amqpURL := fmt.Sprintf("amqp://%s:%s@%s:%s", c.String("amqp_user_name"), c.String("amqp_password"), c.String("amqp_ip"), c.String("amqp_port"))
		sugar.Infow("AMQP connection", "AMQP URL", amqpURL)
		conn, err := amqp.Dial(amqpURL)
		if err != nil {
			sugar.Fatalw("Failed to connect AMQP", err)
		}
		defer conn.Close()

		ch, err := conn.Channel()
		if err != nil {
			sugar.Fatalw("Failed to create channel", err)
		}
		defer ch.Close()

		q, err := ch.QueueDeclare(
			c.String("amqp_listen_key"), // name
			true,  // durable
			false, // delete when usused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			sugar.Fatalw("Failed to connect AMQP", err)
		}

		responseCh, err := conn.Channel()
		if err != nil {
			sugar.Fatalw("Failed to create listen channel", err)
		}
		defer responseCh.Close()
		responseQ, err := responseCh.QueueDeclare(
			c.String("amqp_response_key"), // name
			true,  // durable
			false, // delete when usused
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		msgs, err := ch.Consume(
			q.Name, // queue
			"",     // consumer
			true,   // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		if err != nil {
			sugar.Fatalw("Failed to create consume", err)
		}
		forever := make(chan bool)
		go func() {
			for d := range msgs {
				var file ReceivedFile
				err := json.Unmarshal(d.Body, &file)
				if err != nil {
					sugar.Errorw("Failed to unmarshal JSON", err)
					continue
				}
				sugar.Infow("Received a Queue",
					"ID", file.ID,
					"File Name", file.Name,
					"File Path", file.Path,
					"Callback Key", file.CallbackKey,
				)

				if err := uploadFile(c.String("bucket_name"), file.Path, file.Name, file.Data); err != nil {
					sugar.Errorw("Failed upload file", err)
					continue
				}
				if file.CallbackKey != "" {
					callback := CallbackMQ{
						ID:       file.ID,
						ImageURL: fmt.Sprintf("https://storage.googleapis.com/%s/%s/%s", c.String("bucket_name"), file.Path, file.Name),
					}
					sugar.Infow("Callback message",
						"ID", callback.ID,
						"Message", callback.ImageURL,
					)
					body, err := json.Marshal(callback)
					if err != nil {
						sugar.Errorw("Failed to marshal JSON", err)
						continue
					}
					err = responseCh.Publish("", responseQ.Name, false, false, amqp.Publishing{
						ContentType: "application/json",
						Body:        body,
					})
					if err != nil {
						sugar.Errorw("Failed to send callback message to MQ", err)
					}
				}
			}
		}()
		sugar.Info(" [*] Waiting for messages. To exit press CTRL+C")
		<-forever

		return nil
	}
	app.Run(os.Args)
}

func uploadFile(bucketName, bucketFolderPath, fileName string, data []byte) error {
	ctx := context.Background()

	store := Store{
		BucketName: bucketName,
		Context:    ctx,
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to create new client")
	}
	store.Client = client
	store.Bucket = client.Bucket(store.BucketName)

	if err := store.writeBytes(data, bucketFolderPath, fileName); err != nil {
		return errors.Wrapf(err, "Error on write bytes: %#v", err)
	}

	obj := client.Bucket(store.BucketName).Object(fmt.Sprintf("%s/%s", bucketFolderPath, fileName))
	if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return errors.Wrapf(err, "Failed to set read only for anyone")
	}

	return nil
}

func (s *Store) writeBytes(b []byte, path, fileName string) error {
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
