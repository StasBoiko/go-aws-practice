package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	endpointURL = "http://localhost:4566"
	region      = "us-east-1"
	tableName   = "UserOrders"
)

// OrderItem — модель хранения в DynamoDB (Single-Table Design подход)
type OrderItem struct {
	PK        string  `dynamodbav:"PK"` // Partition Key: USER#<user_id>
	SK        string  `dynamodbav:"SK"` // Sort Key: ORDER#<order_id>
	Amount    float64 `dynamodbav:"Amount"`
	Status    string  `dynamodbav:"Status"`
	CreatedAt string  `dynamodbav:"CreatedAt"`
}

func main() {
	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		log.Fatalf("ошибка конфига: %v", err)
	}

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
	})

	// 1. Создаем таблицу с составным ключом (PK + SK)
	ensureTableExists(ctx, client, tableName)

	// 2. Записываем заказы пользователя USER#42 (идемпотентно)
	orders := []OrderItem{
		{PK: "USER#42", SK: "ORDER#2026-001", Amount: 150.00, Status: "PENDING", CreatedAt: time.Now().Format(time.RFC3339)},
		{PK: "USER#42", SK: "ORDER#2026-002", Amount: 299.99, Status: "PAID", CreatedAt: time.Now().Format(time.RFC3339)},
		{PK: "USER#99", SK: "ORDER#2026-003", Amount: 45.00, Status: "PENDING", CreatedAt: time.Now().Format(time.RFC3339)},
	}

	for _, ord := range orders {
		if err := createOrderIdempotent(ctx, client, ord); err != nil {
			log.Printf("Заказ %s не записан: %v", ord.SK, err)
		} else {
			fmt.Printf("[PutItem] Сохранен %s -> %s (сумма: %.2f, статус: %s)\n", ord.PK, ord.SK, ord.Amount, ord.Status)
		}
	}

	// Проверка защиты от дублей: пробуем повторно записать ORDER#2026-001
	fmt.Println("\nПробуем повторно создать уже существующий ORDER#2026-001...")
	if err := createOrderIdempotent(ctx, client, orders[0]); err != nil {
		fmt.Printf("[Idempotency OK] Защита сработала: %v\n", err)
	}

	// 3. Query: забираем все заказы ТОЛЬКО для USER#42 (без Scan всей таблицы)
	fmt.Println("\nВыполняем Query для USER#42:")
	userOrders, err := getUserOrders(ctx, client, "USER#42")
	if err != nil {
		log.Fatalf("ошибка Query: %v", err)
	}
	for _, o := range userOrders {
		fmt.Printf(" -> %s | Сумма: $%.2f | Статус: %s\n", o.SK, o.Amount, o.Status)
	}

	// 4. Optimistic Locking: меняем статус PENDING -> SHIPPED для ORDER#2026-001
	fmt.Println("\nОбновляем статус ORDER#2026-001 (только если текущий статус PENDING)...")
	if err := transitionOrderStatus(ctx, client, "USER#42", "ORDER#2026-001", "PENDING", "SHIPPED"); err != nil {
		log.Fatalf("ошибка обновления статуса: %v", err)
	}
	fmt.Println("[UpdateItem] Статус успешно переведен в SHIPPED!")
}

func ensureTableExists(ctx context.Context, client *dynamodb.Client, table string) {
	_, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})
	if err == nil {
		fmt.Printf("Таблица '%s' уже существует.\n", table)
		return
	}

	_, err = client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(table),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},  // Partition Key
			{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange}, // Sort Key
		},
		BillingMode: types.BillingModePayPerRequest, // On-Demand режим (без ручного задания RCU/WCU)
	})
	if err != nil {
		log.Fatalf("ошибка создания таблицы: %v", err)
	}
	fmt.Printf("Таблица '%s' создана.\n", table)
}

func createOrderIdempotent(ctx context.Context, client *dynamodb.Client, item OrderItem) error {
	av, err := attributevalue.MarshalMap(item)
	if err != nil {
		return err
	}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      av,
		// Гарантируем, что запись не перезапишет существующий заказ с тем же PK+SK
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		var condErr *types.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			return fmt.Errorf("заказ %s уже существует", item.SK)
		}
		return err
	}
	return nil
}

func getUserOrders(ctx context.Context, client *dynamodb.Client, userPK string) ([]OrderItem, error) {
	out, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :skPrefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":       &types.AttributeValueMemberS{Value: userPK},
			":skPrefix": &types.AttributeValueMemberS{Value: "ORDER#"},
		},
	})
	if err != nil {
		return nil, err
	}

	var result []OrderItem
	if err := attributevalue.UnmarshalListOfMaps(out.Items, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func transitionOrderStatus(ctx context.Context, client *dynamodb.Client, pk, sk, expectedStatus, newStatus string) error {
	_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: pk},
			"SK": &types.AttributeValueMemberS{Value: sk},
		},
		// #st используется как псевдоним, так как "Status" — зарезервированное слово в DynamoDB!
		UpdateExpression:    aws.String("SET #st = :newStatus"),
		ConditionExpression: aws.String("#st = :expectedStatus"),
		ExpressionAttributeNames: map[string]string{
			"#st": "Status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newStatus":      &types.AttributeValueMemberS{Value: newStatus},
			":expectedStatus": &types.AttributeValueMemberS{Value: expectedStatus},
		},
	})
	return err
}
