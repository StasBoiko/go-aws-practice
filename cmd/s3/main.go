package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	localStackEndpoint = "http://localhost:4566"
	awsRegion          = "us-east-1"
	bucketName         = "user-avatars-bucket"
)

func main() {
	ctx := context.Background()

	// 1. Инициализация клиента S3
	s3Client, err := newLocalS3Client(ctx)
	if err != nil {
		log.Fatalf("ошибка создания клиента S3: %v", err)
	}

	// 2. Идемпотентное создание бакета
	if err := ensureBucketExists(ctx, s3Client, bucketName); err != nil {
		log.Fatalf("ошибка создания бакета: %v", err)
	}

	// 3. Прямая загрузка файла с бэкенда (PutObject)
	objectKey := "docs/hello.txt"
	content := []byte("Привет из Go-бэкенда в AWS S3!")
	if err := uploadFile(ctx, s3Client, bucketName, objectKey, content); err != nil {
		log.Fatalf("ошибка загрузки файла: %v", err)
	}

	// 4. Чтение файла (GetObject)
	downloaded, err := downloadFile(ctx, s3Client, bucketName, objectKey)
	if err != nil {
		log.Fatalf("ошибка скачивания файла: %v", err)
	}
	fmt.Printf("Скачано из S3 [%s]: %s\n\n", objectKey, string(downloaded))

	// 5. Продвинутый кейс: Presigned URL для прямой загрузки с фронтенда
	presignClient := s3.NewPresignClient(s3Client)
	uploadKey := "uploads/client_data.json"

	presignedReq, err := presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(uploadKey),
		ContentType: aws.String("application/json"),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 5 * time.Minute // Ссылка протухнет через 5 минут
	})
	if err != nil {
		log.Fatalf("ошибка генерации presigned url: %v", err)
	}

	fmt.Printf("Сгенерирован Presigned URL (действует 5 мин):\n%s\n\n", presignedReq.URL)

	// Эмулируем фронтенд: отправляем HTTP PUT напрямую по этой ссылке без AWS SDK
	if err := simulateFrontendUpload(presignedReq.URL, `{"status":"uploaded_directly"}`); err != nil {
		log.Fatalf("ошибка загрузки по presigned url: %v", err)
	}
	fmt.Println("Фронтенд успешно загрузил файл напрямую в S3!")
}

func newLocalS3Client(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(awsRegion),
		// Для LocalStack передаем фейковые креды. В реальном AWS эта строка убирается,
		// и SDK сам берет IAM-роль из окружения (ECS Task Role / EC2 Instance Profile).
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// Перенаправляем запросы в LocalStack
		o.BaseEndpoint = aws.String(localStackEndpoint)
		// Обязательно для LocalStack: путь вида localhost:4566/bucket/key вместо bucket.localhost:4566
		o.UsePathStyle = true
	})

	return client, nil
}

func ensureBucketExists(ctx context.Context, client *s3.Client, bucket string) error {
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err == nil {
		fmt.Printf("Бакет '%s' уже существует.\n", bucket)
		return nil
	}

	// Если бакета нет, создаем его
	var notFound *types.NotFound
	if errors.As(err, &notFound) || err != nil {
		_, createErr := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		})
		if createErr != nil {
			return createErr
		}
		fmt.Printf("Бакет '%s' успешно создан.\n", bucket)
	}
	return nil
}

func uploadFile(ctx context.Context, client *s3.Client, bucket, key string, data []byte) error {
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("text/plain; charset=utf-8"),
	})
	return err
}

func downloadFile(ctx context.Context, client *s3.Client, bucket, key string) ([]byte, error) {
	resp, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	// Важно: Body в S3 GetObject — это io.ReadCloser, его обязательно нужно закрывать,
	// иначе потекут HTTP-соединения в пуле.
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
}

func simulateFrontendUpload(presignedURL string, jsonPayload string) error {
	req, err := http.NewRequest(http.MethodPut, presignedURL, bytes.NewBufferString(jsonPayload))
	if err != nil {
		return err
	}
	// Content-Type должен совпадать с тем, что мы зашили при генерации Presigned URL!
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("неожиданный статус %d: %s", resp.StatusCode, string(body))
	}
	return nil
}