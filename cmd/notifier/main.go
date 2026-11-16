package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
)

// EmailSender — интерфейс для DI, позволяет мокать AWS SES в unit-тестах
type EmailSender interface {
	SendEmail(ctx context.Context, params *ses.SendEmailInput, optFns ...func(*ses.Options)) (*ses.SendEmailOutput, error)
}

type Notifier struct {
	client EmailSender
	sender string
	env    string
}

func NewNotifier(client EmailSender, sender, env string) *Notifier {
	return &Notifier{
		client: client,
		sender: sender,
		env:    env,
	}
}

func (n *Notifier) SendNotification(ctx context.Context, recipient string) (string, error) {
	if recipient == "" {
		return "", fmt.Errorf("recipient email cannot be empty")
	}

	subject := fmt.Sprintf("[%s] Уведомление из Go + AWS SES", n.env)
	htmlBody := fmt.Sprintf("<h1>Привет!</h1><p>Окружение: <b>%s</b></p>", n.env)
	textBody := fmt.Sprintf("Привет! Окружение: %s", n.env)

	out, err := n.client.SendEmail(ctx, &ses.SendEmailInput{
		Source: aws.String(n.sender),
		Destination: &types.Destination{
			ToAddresses: []string{recipient},
		},
		Message: &types.Message{
			Subject: &types.Content{Data: aws.String(subject), Charset: aws.String("UTF-8")},
			Body: &types.Body{
				Html: &types.Content{Data: aws.String(htmlBody), Charset: aws.String("UTF-8")},
				Text: &types.Content{Data: aws.String(textBody), Charset: aws.String("UTF-8")},
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("ses send email: %w", err)
	}
	return *out.MessageId, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx := context.Background()
	env := getEnv("APP_ENV", "local")
	region := getEnv("AWS_REGION", "us-east-1")
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	sender := getEnv("SES_SENDER_EMAIL", "noreply@myapp.local")
	recipient := getEnv("SES_RECIPIENT_EMAIL", "stasboiko0@gmail.com")

	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(region))
	if env == "local" && endpoint != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		slog.Error("load aws config failed", "error", err)
		os.Exit(1)
	}

	sesClient := ses.NewFromConfig(awsCfg, func(o *ses.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})

	if env == "local" {
		_, _ = sesClient.VerifyEmailIdentity(ctx, &ses.VerifyEmailIdentityInput{
			EmailAddress: aws.String(sender),
		})
	}

	notifier := NewNotifier(sesClient, sender, env)
	msgID, err := notifier.SendNotification(ctx, recipient)
	if err != nil {
		slog.Error("notification failed", "error", err)
		os.Exit(1)
	}

	slog.Info("email sent successfully", "env", env, "message_id", msgID, "to", recipient)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
