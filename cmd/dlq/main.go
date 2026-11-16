package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	endpointURL = "http://localhost:4566"
	region      = "us-east-1"
)

type Order struct {
	ID     string  `json:"id"`
	Amount float64 `json:"amount"`
}

// Структура для сериализации RedrivePolicy в JSON (требование AWS API)
type RedrivePolicy struct {
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	MaxReceiveCount     int    `json:"maxReceiveCount"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		log.Fatalf("ошибка конфига: %v", err)
	}

	client := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
	})

	// 1. Создаем DLQ и основную очередь с RedrivePolicy (maxReceiveCount = 3)
	mainQueueURL, dlqURL := setupQueuesWithDLQ(ctx, client)

	// 2. Отправляем 1 валидный заказ и 1 "отравленный" (с отрицательной суммой)
	sendOrder(ctx, client, mainQueueURL, Order{ID: "ORD-OK", Amount: 120.50})
	sendOrder(ctx, client, mainQueueURL, Order{ID: "ORD-POISON", Amount: -50.00})

	var wg sync.WaitGroup
	wg.Add(2)

	// 3. Основной воркер: обрабатывает заказы и оставляет битые без DeleteMessage
	go runMainWorker(ctx, &wg, client, mainQueueURL)

	// 4. Монитор DLQ: слушает очередь ошибок, куда упадет ORD-POISON после 3 попыток
	go runDLQInspector(ctx, &wg, client, dlqURL)

	fmt.Printf("\nНаблюдаем за 3 попытками ретрая (займет ~6-8 сек). Нажми Ctrl+C для выхода после проверки.\n")
	<-ctx.Done()
	fmt.Println("\nОстановка воркеров...")
	wg.Wait()
	fmt.Println("Готово.")
}

func setupQueuesWithDLQ(ctx context.Context, client *sqs.Client) (string, string) {
	// Шаг А: Сначала создаем саму DLQ (это обычная очередь SQS)
	dlqOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("orders-dlq"),
	})
	if err != nil {
		log.Fatalf("ошибка создания DLQ: %v", err)
	}
	dlqURL := *dlqOut.QueueUrl

	// Получаем ARN созданной DLQ
	attrOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		log.Fatalf("ошибка получения ARN для DLQ: %v", err)
	}
	dlqARN := attrOut.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]

	// Шаг Б: Формируем JSON для RedrivePolicy
	policyBytes, err := json.Marshal(RedrivePolicy{
		DeadLetterTargetArn: dlqARN,
		MaxReceiveCount:     3, // После 3 неудачных попыток -> в DLQ
	})
	if err != nil {
		log.Fatalf("ошибка маршалинга RedrivePolicy: %v", err)
	}

	// Шаг В: Создаем основную очередь и привязываем к ней RedrivePolicy
	mainOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("orders-main-queue"),
		Attributes: map[string]string{
			// Ставим 2 секунды для быстрого теста (в проде обычно 30-60 сек)
			"VisibilityTimeout": "2",
			"RedrivePolicy":     string(policyBytes),
		},
	})
	if err != nil {
		log.Fatalf("ошибка создания основной очереди: %v", err)
	}

	fmt.Printf("Создана DLQ: %s\n", dlqARN)
	fmt.Printf("Создана основная очередь с maxReceiveCount=3: %s\n", *mainOut.QueueUrl)
	return *mainOut.QueueUrl, dlqURL
}

func sendOrder(ctx context.Context, client *sqs.Client, queueURL string, order Order) {
	body, _ := json.Marshal(order)
	_, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(body)),
	})
	if err != nil {
		log.Fatalf("ошибка отправки заказа %s: %v", order.ID, err)
	}
	fmt.Printf("[Producer] Отправлен заказ: %s (сумма: %.2f)\n", order.ID, order.Amount)
}

func runMainWorker(ctx context.Context, wg *sync.WaitGroup, client *sqs.Client, queueURL string) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			resp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(queueURL),
				MaxNumberOfMessages: 5,
				WaitTimeSeconds:     2,
				// Запрашиваем системный атрибут, чтобы видеть номер текущей попытки!
				MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{
					sqstypes.MessageSystemAttributeNameApproximateReceiveCount,
				},
			})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				time.Sleep(time.Second)
				continue
			}

			for _, msg := range resp.Messages {
				attempt := msg.Attributes[string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount)]
				var order Order
				_ = json.Unmarshal([]byte(*msg.Body), &order)

				if err := processOrder(order); err != nil {
					// ВАЖНО: Не вызываем DeleteMessage!
					// Сообщение останется в очереди и станет снова доступным через VisibilityTimeout (2 сек).
					fmt.Printf("[MainWorker] ОШИБКА обработки %s (попытка #%s): %v. Оставляем в очереди для ретрая.\n",
						order.ID, attempt, err)
					continue
				}

				// Успех -> удаляем сообщение
				_, _ = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      aws.String(queueURL),
					ReceiptHandle: msg.ReceiptHandle,
				})
				fmt.Printf("[MainWorker] УСПЕХ: заказ %s обработан и удален из очереди.\n", order.ID)
			}
		}
	}
}

func processOrder(o Order) error {
	if o.Amount <= 0 {
		return errors.New("некорректная сумма заказа (<= 0)")
	}
	return nil
}

func runDLQInspector(ctx context.Context, wg *sync.WaitGroup, client *sqs.Client, dlqURL string) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			resp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(dlqURL),
				MaxNumberOfMessages: 5,
				WaitTimeSeconds:     2,
			})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				time.Sleep(time.Second)
				continue
			}

			for _, msg := range resp.Messages {
				fmt.Printf("\n>>> [DLQ ALERT] В Dead Letter Queue перехвачено битое сообщение: %s <<<\n", *msg.Body)
				// Удаляем из DLQ после логирования/сохранения в БД инцидентов
				_, _ = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      aws.String(dlqURL),
					ReceiptHandle: msg.ReceiptHandle,
				})
			}
		}
	}
}
