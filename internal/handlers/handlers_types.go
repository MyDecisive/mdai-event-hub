package handlers

import (
	"context"
)

type IHandlerAdapter interface {
	AddElementToSet(ctx context.Context, variableKey string, hubName string, value string, correlationID string) error
	RemoveElementFromSet(ctx context.Context, variableKey string, hubName string, value string, correlationID string) error
	AddSetMapElement(ctx context.Context, variableKey string, hubName string, field string, value string, correlationID string) error
	RemoveElementFromMap(ctx context.Context, variableKey string, hubName string, field string, correlationID string) error
	SetStringValue(ctx context.Context, variableKey string, hubName string, value string, correlationID string) error
}
