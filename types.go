package main

import (
	"context"
	"github.com/decisiveai/mdai-event-hub/eventing"
	"go.uber.org/zap"
)

type IHandlerAdapter interface {
	AddElementToSet(ctx context.Context, variableKey string, hubName string, value string, correlationId string) error
	RemoveElementFromSet(ctx context.Context, variableKey string, hubName string, value string, correlationId string) error
	AddSetMapElement(ctx context.Context, variableKey string, hubName string, field string, value string, correlationId string) error
	RemoveElementFromMap(ctx context.Context, variableKey string, hubName string, field string, correlationId string) error
	SetStringValue(ctx context.Context, variableKey string, hubName string, value string, correlationId string) error
}

type MdaiInterface struct {
	logger *zap.Logger
	data   IHandlerAdapter
}

type HandlerName string

type HandlerFunc func(MdaiInterface, eventing.MdaiEvent, map[string]string) error

type HandlerMap map[HandlerName]HandlerFunc
