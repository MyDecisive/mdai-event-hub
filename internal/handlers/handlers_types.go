package handlers

import (
	"context"

	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	"go.uber.org/zap"
)

type IHandlerAdapter interface {
	AddElementToSet(ctx context.Context, variableKey string, hubName string, value string, correlationID string) error
	RemoveElementFromSet(ctx context.Context, variableKey string, hubName string, value string, correlationID string) error
	AddSetMapElement(ctx context.Context, variableKey string, hubName string, field string, value string, correlationID string) error
	RemoveElementFromMap(ctx context.Context, variableKey string, hubName string, field string, correlationID string) error
	SetStringValue(ctx context.Context, variableKey string, hubName string, value string, correlationID string) error
}
type MdaiInterface struct {
	Logger *zap.Logger
	Data   IHandlerAdapter
}

type HandlerName string

type HandlerFunc func(MdaiInterface, eventing.MdaiEvent, map[string]any) error
