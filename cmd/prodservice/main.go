package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type AppConfig struct {
	Port        string
	AWSRegion   string
	AWSEndpoint string // Пусто в продакшене, "http://localstack:4566" локально
	BucketName  string
	TableName   string
}

func loadAppConfig() AppConfig {
	return AppConfig{
		Port:        getEnv("PORT", "8080"),
		AWSRegion:   getEnv("AWS_REGION", "us-east-1"),
		AWSEndpoint: os.Getenv("AWS_ENDPOINT_URL"),
		BucketName:  getEnv("S3_BUCKET", "user-avatars-bucket"),
		TableName:   getEnv("DYNAMO_TABLE", "UserOrders"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type AWSClients struct {
	S3        *s3.Client
	S3Presign *s3.PresignClient
	Dynamo    *dynamodb.Client
}

// initAWSClients одинаково работает и с LocalStack, и с реальным AWS через IAM Role
func initAWSClients(ctx context.Context, cfg AppConfig) (*AWSClients, error) {
	var optFns []func(*config.LoadOptions) error
	optFns = append(optFns, config.WithRegion(cfg.AWSRegion))

	// Если задан кастомный эндпоинт (LocalStack / MinIO), подставляем тестовые креды
	if cfg.AWSEndpoint != "" {
		optFns = append(optFns,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		)
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("load aws default config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.AWSEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.AWSEndpoint)
			o.UsePathStyle = true
		}
	})

	dynamoClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if cfg.AWSEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.AWSEndpoint)
		}
	})

	return &AWSClients{
		S3:        s3Client,
		S3Presign: s3.NewPresignClient(s3Client),
		Dynamo:    dynamoClient,
	}, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	appCfg := loadAppConfig()
	clients, err := initAWSClients(ctx, appCfg)
	if err != nil {
		slog.Error("failed to initialize AWS clients", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()

	// Healthcheck для AWS Application Load Balancer (ALB) и ECS
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// Боевой эндпоинт: создает запись о файле в DynamoDB и выдает Presigned URL в S3
	mux.HandleFunc("POST /files/upload-url", func(w http.ResponseWriter, r *http.Request) {
		key := fmt.Sprintf("uploads/%d.json", time.Now().UnixNano())

		presigned, err := clients.S3Presign.PresignPutObject(r.Context(), &s3.PutObjectInput{
			Bucket:      aws.String(appCfg.BucketName),
			Key:         aws.String(key),
			ContentType: aws.String("application/json"),
		}, func(opts *s3.PresignOptions) {
			opts.Expires = 5 * time.Minute
		})
		if err != nil {
			slog.Error("presign failed", "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		// Фиксируем метаданные в DynamoDB
		item, _ := attributevalue.MarshalMap(map[string]string{
			"PK":        "FILE#UPLOAD",
			"SK":        key,
			"Status":    "AWAITING_UPLOAD",
			"CreatedAt": time.Now().Format(time.RFC3339),
		})
		if _, err := clients.Dynamo.PutItem(r.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(appCfg.TableName),
			Item:      item,
		}); err != nil {
			slog.Error("dynamodb put failed", "error", err)
			http.Error(w, "db error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"file_key":   key,
			"upload_url": presigned.URL,
		})
	})

	srv := &http.Server{
		Addr:              ":" + appCfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("starting HTTP server", "port", appCfg.Port, "aws_endpoint", appCfg.AWSEndpoint)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	slog.Info("server stopped")
}
