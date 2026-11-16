package main

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ses"
)

type mockSESClient struct {
	lastInput *ses.SendEmailInput
}

func (m *mockSESClient) SendEmail(ctx context.Context, params *ses.SendEmailInput, optFns ...func(*ses.Options)) (*ses.SendEmailOutput, error) {
	m.lastInput = params
	return &ses.SendEmailOutput{
		MessageId: aws.String("mock-msg-id-12345"),
	}, nil
}

func TestSendNotification_Success(t *testing.T) {
	mockClient := &mockSESClient{}
	notifier := NewNotifier(mockClient, "noreply@test.com", "dev")

	msgID, err := notifier.SendNotification(context.Background(), "stasboiko0@gmail.com")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if msgID != "mock-msg-id-12345" {
		t.Errorf("expected mock-msg-id-12345, got: %s", msgID)
	}
	if !strings.Contains(*mockClient.lastInput.Message.Subject.Data, "[dev]") {
		t.Errorf("expected subject to contain environment [dev], got: %s", *mockClient.lastInput.Message.Subject.Data)
	}
}

func TestSendNotification_EmptyRecipient(t *testing.T) {
	mockClient := &mockSESClient{}
	notifier := NewNotifier(mockClient, "noreply@test.com", "dev")

	_, err := notifier.SendNotification(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty recipient, got nil")
	}
}
