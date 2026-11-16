package main

import (
	"context"
	"encoding/json"
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
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	endpointURL = "http://localhost:4566"
	region      = "us-east-1"
)

type OrderCreatedEvent struct {
	OrderID string  `json:"order_id"`
	UserID  int     `json:"user_id"`
	Amount  float64 `json:"amount"`
}

func main() {
	// Контекст с отменой по Ctrl+C (SIGINT / SIGTERM) для Graceful Shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		log.Fatalf("ошибка загрузки конфига: %v", err)
	}

	snsClient := sns.NewFromConfig(cfg, func(o *sns.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
	})
	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
	})

	// 1. Создаем инфраструктуру: 1 SNS Topic и 2 SQS Queues (Fan-out)
	topicARN, billingQueueURL, notifyQueueURL := setupPubSubInfra(ctx, snsClient, sqsClient)

	var wg sync.WaitGroup

	// 2. Запускаем два независимых воркера-консьюмера
	wg.Add(2)
	go startSQSWorker(ctx, &wg, sqsClient, "BillingService", billingQueueURL)
	go startSQSWorker(ctx, &wg, sqsClient, "NotificationService", notifyQueueURL)

	// 3. Имитируем публикацию событий в SNS (Producer)
	go func() {
		for i := 1; i <= 3; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				event := OrderCreatedEvent{
					OrderID: fmt.Sprintf("ORD-%03d", i),
					UserID:  100 + i,
					Amount:  float64(i) * 49.99,
				}
				if err := publishOrderEvent(ctx, snsClient, topicARN, event); err != nil {
					log.Printf("[Producer] ошибка публикации: %v", err)
				}
				time.Sleep(1 * time.Second)
			}
		}
		fmt.Println("\n[Producer] Все события отправлены. Нажми Ctrl+C для корректной остановки воркеров.")
	}()

	// Ждем сигнала завершения и даем воркерам закончить текущие задачи
	<-ctx.Done()
	fmt.Println("\nПолучен сигнал завершения, ждем остановки воркеров...")
	wg.Wait()
	fmt.Println("Все воркеры остановлены корректно.")
}

func setupPubSubInfra(ctx context.Context, snsClient *sns.Client, sqsClient *sqs.Client) (string, string, string) {
	// Создаем SNS топик
	topicOut, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("order-events-topic"),
	})
	if err != nil {
		log.Fatalf("не удалось создать SNS топик: %v", err)
	}
	topicARN := *topicOut.TopicArn
	fmt.Printf("Создан SNS Topic: %s\n", topicARN)

	// Создаем две очереди SQS и подписываем обе на один топик
	billingURL := createQueueAndSubscribe(ctx, snsClient, sqsClient, topicARN, "billing-orders-queue")
	notifyURL := createQueueAndSubscribe(ctx, snsClient, sqsClient, topicARN, "notifications-orders-queue")

	return topicARN, billingURL, notifyURL
}

func createQueueAndSubscribe(ctx context.Context, snsClient *sns.Client, sqsClient *sqs.Client, topicARN, queueName string) string {
	qOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			// VisibilityTimeout: если воркер упадет, через 15 сек сообщение вернется в очередь
			"VisibilityTimeout": "15",
		},
	})
	if err != nil {
		log.Fatalf("не удалось создать очередь %s: %v", queueName, err)
	}
	queueURL := *qOut.QueueUrl

	// Получаем ARN очереди (для подписки в SNS нужен именно ARN, а не HTTP URL)
	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		log.Fatalf("не удалось получить ARN очереди %s: %v", queueName, err)
	}
	queueARN := attrOut.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]

	// Подписываем SQS на SNS с включенным RawMessageDelivery (без лишней обертки SNS)
	_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
		Attributes: map[string]string{
			"RawMessageDelivery": "true",
		},
	})
	if err != nil {
		log.Fatalf("ошибка подписки %s на топик: %v", queueName, err)
	}

	fmt.Printf("Очередь '%s' создана и подписана на топик.\n", queueName)
	return queueURL
}

func publishOrderEvent(ctx context.Context, client *sns.Client, topicARN string, event OrderCreatedEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	out, err := client.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicARN),
		Message:  aws.String(string(payload)),
	})
	if err != nil {
		return err
	}

	fmt.Printf("[Producer] Опубликовано событие %s (SNS MessageId: %s)\n", event.OrderID, *out.MessageId)
	return nil
}

func startSQSWorker(ctx context.Context, wg *sync.WaitGroup, client *sqs.Client, serviceName, queueURL string) {
	defer wg.Done()
	fmt.Printf("[%s] Воркер запущен, слушаем очередь...\n", serviceName)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[%s] Завершение работы воркера.\n", serviceName)
			return
		default:
			// Long Polling: ждем сообщения до 5 секунд (в продакшене обычно ставят 20)
			resp, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(queueURL),
				MaxNumberOfMessages: 5, // Забираем батчем до 5 сообщений за раз
				WaitTimeSeconds:     5,
			})
			if err != nil {
				// Если контекст отменен во время ожидания Long Polling — штатно выходим
				if ctx.Err() != nil {
					return
				}
				log.Printf("[%s] ошибка чтения из SQS: %v", serviceName, err)
				time.Sleep(time.Second)
				continue
			}

			for _, msg := range resp.Messages {
				var event OrderCreatedEvent
				if err := json.Unmarshal([]byte(*msg.Body), &event); err != nil {
					log.Printf("[%s] битый JSON: %v", serviceName, err)
					continue
				}

				// Имитируем бизнес-логику обработки
				fmt.Printf("[%s] Обработан заказ %s на сумму $%.2f\n", serviceName, event.OrderID, event.Amount)

				// ОБЯЗАТЕЛЬНО удаляем сообщение после успешной обработки (ACK)
				_, delErr := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      aws.String(queueURL),
					ReceiptHandle: msg.ReceiptHandle,
				})
				if delErr != nil {
					log.Printf("[%s] ошибка удаления сообщения: %v", serviceName, delErr)
				}
			}
		}
	}
}
