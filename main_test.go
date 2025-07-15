package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	dcoreKube "github.com/decisiveai/mdai-data-core/kube"
	"github.com/decisiveai/mdai-event-hub/eventing"
	v1 "github.com/decisiveai/mdai-operator/api/v1"
	"github.com/stretchr/testify/assert"
	valkeyMock "github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// Mock handler for testing
var testHandlerCalled bool
var testHandlerError error

func testHandler(_ MdaiInterface, _ eventing.MdaiEvent, _ map[string]string) error {
	testHandlerCalled = true
	return testHandlerError
}

func TestProcessEvent_Success(t *testing.T) {
	ctx := context.TODO()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := valkeyMock.NewClient(ctrl)
	logger := zap.NewNop()

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-hub-automation",
			Labels: map[string]string{
				dcoreKube.ConfigMapTypeLabel: dcoreKube.ManualEnvConfigMapType,
				dcoreKube.LabelMdaiHubName:   "test-hub",
			},
		},
		Data: map[string]string{
			"TestAlert.firing": "[{\"handlerRef\" :\"testHandler\", \"args\": {\"key\":\"value\"}}]",
		},
	}

	clientset := fake.NewClientset(configMap)
	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap, metav1.CreateOptions{})

	cmController, err := dcoreKube.NewConfigMapController(dcoreKube.ManualEnvConfigMapType, "", clientset, logger)
	if err != nil {
		logger.Fatal("failed to create ConfigMap controller", zap.Error(err))
	}
	if err := cmController.Run(); err != nil {
		logger.Fatal("failed to run ConfigMap controller", zap.Error(err))
	}

	testHandlerCalled = false
	testHandlerError = nil

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "TestAlert.firing",
	}
	SupportedHandlers = map[HandlerName]HandlerFunc{
		"testHandler": testHandler,
	}

	handler := ProcessEvent(ctx, mockClient, cmController, logger)
	err = handler(event)

	assert.NoError(t, err)
	assert.True(t, testHandlerCalled)
}

func TestProcessEvent_NoHubName(t *testing.T) {
	ctx := context.TODO()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := valkeyMock.NewClient(ctrl)
	logger := zap.NewNop()

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-hub-automation",
			Labels: map[string]string{
				dcoreKube.ConfigMapTypeLabel: dcoreKube.ManualEnvConfigMapType,
				dcoreKube.LabelMdaiHubName:   "test-hub",
			},
		},
		Data: map[string]string{
			"TestAlert.firing": "[{\"handlerRef\" :\"testHandler\", \"args\": {\"key\":\"value\"}}]",
		},
	}

	clientset := fake.NewClientset(configMap)
	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap, metav1.CreateOptions{})

	cmController, err := dcoreKube.NewConfigMapController(dcoreKube.ManualEnvConfigMapType, "", clientset, logger)
	if err != nil {
		logger.Fatal("failed to create ConfigMap controller", zap.Error(err))
	}
	if err := cmController.Run(); err != nil {
		logger.Fatal("failed to run ConfigMap controller", zap.Error(err))
	}
	defer cmController.Stop()

	testHandlerCalled = false
	testHandlerError = nil

	event := eventing.MdaiEvent{
		Name: "TestAlert.firing",
	}
	SupportedHandlers = map[HandlerName]HandlerFunc{
		"testHandler": testHandler,
	}

	handler := ProcessEvent(ctx, mockClient, cmController, logger)
	err = handler(event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no hub name provided")
}

func TestProcessEvent_MatchAlertNameOnly(t *testing.T) {
	ctx := context.TODO()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := valkeyMock.NewClient(ctrl)
	logger := zap.NewNop()

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-hub-automation",
			Labels: map[string]string{
				dcoreKube.ConfigMapTypeLabel: dcoreKube.ManualEnvConfigMapType,
				dcoreKube.LabelMdaiHubName:   "test-hub",
			},
		},
		Data: map[string]string{
			"TestAlert": "[{\"handlerRef\" :\"testHandler\", \"args\": {\"key\":\"value\"}}]",
		},
	}

	clientset := fake.NewClientset(configMap)
	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap, metav1.CreateOptions{})

	cmController, err := dcoreKube.NewConfigMapController(dcoreKube.ManualEnvConfigMapType, "", clientset, logger)
	if err != nil {
		logger.Fatal("failed to create ConfigMap controller", zap.Error(err))
	}
	if err := cmController.Run(); err != nil {
		logger.Fatal("failed to run ConfigMap controller", zap.Error(err))
	}
	defer cmController.Stop()

	testHandlerCalled = false
	testHandlerError = nil

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "TestAlert.firing",
	}
	SupportedHandlers = map[HandlerName]HandlerFunc{
		"testHandler": testHandler,
	}

	handler := ProcessEvent(ctx, mockClient, cmController, logger)
	err = handler(event)

	assert.NoError(t, err)
	assert.True(t, testHandlerCalled)
}

func TestProcessEvent_NoWorkflowFound(t *testing.T) {
	ctx := context.TODO()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := valkeyMock.NewClient(ctrl)
	logger := zap.NewNop()

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-hub-automation",
			Labels: map[string]string{
				dcoreKube.ConfigMapTypeLabel: dcoreKube.ManualEnvConfigMapType,
				dcoreKube.LabelMdaiHubName:   "test-hub",
			},
		},
	}

	clientset := fake.NewClientset(configMap)
	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap, metav1.CreateOptions{})

	cmController, err := dcoreKube.NewConfigMapController(dcoreKube.ManualEnvConfigMapType, "", clientset, logger)
	if err != nil {
		logger.Fatal("failed to create ConfigMap controller", zap.Error(err))
	}
	if err := cmController.Run(); err != nil {
		logger.Fatal("failed to run ConfigMap controller", zap.Error(err))
	}
	defer cmController.Stop()

	testHandlerCalled = false
	testHandlerError = nil

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "UnknownAlert.firing",
	}

	handler := ProcessEvent(ctx, mockClient, cmController, logger)
	err = handler(event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no configured automation for event: UnknownAlert.firing")
}

func TestSafePerformAutomationStep_Success(t *testing.T) {
	testHandlerCalled = false
	testHandlerError = nil

	originalHandlers := SupportedHandlers
	SupportedHandlers = map[HandlerName]HandlerFunc{
		"testHandler": testHandler,
	}
	defer func() { SupportedHandlers = originalHandlers }()

	mdai := MdaiInterface{
		logger: zap.NewNop(),
	}

	autoStep := v1.AutomationStep{
		HandlerRef: "testHandler",
		Arguments:  map[string]string{"key": "value"},
	}

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "TestAlert.firing",
	}

	err := safePerformAutomationStep(mdai, autoStep, event)

	assert.NoError(t, err)
	assert.True(t, testHandlerCalled)
}

func TestSafePerformAutomationStep_UnsupportedHandler(t *testing.T) {
	mdai := MdaiInterface{
		logger: zap.NewNop(),
	}

	autoStep := v1.AutomationStep{
		HandlerRef: "unsupportedHandler",
		Arguments:  map[string]string{"key": "value"},
	}

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "TestAlert.firing",
	}

	err := safePerformAutomationStep(mdai, autoStep, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "handler unsupportedHandler not supported")
}

func panicHandler(_ MdaiInterface, _ eventing.MdaiEvent, _ map[string]string) error {
	panic("simulated panic")
}

func TestSafePerformAutomationStep_PanicRecovery(t *testing.T) {
	SupportedHandlers = HandlerMap{
		"panicHandler": panicHandler,
	}

	logger := zap.NewNop()
	mdai := MdaiInterface{
		data:   nil,
		logger: logger,
	}

	autoStep := v1.AutomationStep{
		HandlerRef: "panicHandler",
		Arguments:  map[string]string{},
	}

	event := eventing.MdaiEvent{
		Id:      "1",
		Name:    "TestEvent",
		HubName: "test-hub",
	}

	err := safePerformAutomationStep(mdai, autoStep, event)
	assert.Error(t, err)

	expectedPrefix := fmt.Sprintf("panic in handler %s", autoStep.HandlerRef)
	assert.True(
		t,
		strings.HasPrefix(err.Error(), expectedPrefix),
		"unexpected error message:\n got: %q\n want prefix: %q",
		err.Error(), expectedPrefix,
	)
}

func TestProcessEventPayload_Success(t *testing.T) {
	validJSON := `{"key1":"value1","key2":42,"nested":{"sub":"val"}}`
	event := eventing.MdaiEvent{
		Id:      "e1",
		Name:    "test",
		HubName: "hub1",
		Payload: validJSON,
	}

	result, err := processEventPayload(event)
	assert.NoError(t, err, "expected no error for valid JSON payload")

	assert.Contains(t, result, "key1")
	assert.Contains(t, result, "key2")
	assert.Contains(t, result, "nested")

	assert.Equal(t, "value1", result["key1"])
	assert.Equal(t, float64(42), result["key2"])
	nested, ok := result["nested"].(map[string]any)
	assert.True(t, ok, "expected nested to be a map[string]any")
	assert.Equal(t, "val", nested["sub"])
}

func TestProcessEventPayload_InvalidJSON(t *testing.T) {
	invalidJSON := `{"key1":"value1", "key2":}`
	event := eventing.MdaiEvent{
		Id:      "e2",
		Name:    "test-invalid",
		HubName: "hub2",
		Payload: invalidJSON,
	}

	result, err := processEventPayload(event)
	assert.Nil(t, result, "expected result to be nil on invalid JSON")
	assert.Error(t, err, "expected an error for invalid JSON payload")
	assert.Contains(t, err.Error(), "failed to unmarshal payload")
}

func TestGetWorkflowMap(t *testing.T) {
	steps := []map[string]string{
		{"TestAlert1.firing": "[{\"handlerRef\" :\"testHandler1\", \"args\": {\"key1\":\"value1\"}}]"},
		{"TestAlert2.firing": "[{\"handlerRef\" :\"testHandler2\", \"args\": {\"key2\":\"value2\"}}]"},
	}
	expectedWorkflowMap := map[string][]v1.AutomationStep{
		"TestAlert1.firing": {
			{
				HandlerRef: "testHandler1",
				Arguments:  map[string]string{"key1": "value1"},
			},
		},
		"TestAlert2.firing": {
			{
				HandlerRef: "testHandler2",
				Arguments:  map[string]string{"key2": "value2"},
			},
		},
	}
	workflowMap := getWorkflowMap(steps)
	assert.Equal(t, expectedWorkflowMap, workflowMap)
}

func TestGetWorkflowMap_InvalidStepsJson(t *testing.T) {
	steps := []map[string]string{
		{"TestAlert1.firing": "[{\"hhhhandlerRef\" :\"testHandler1\", \"aaaargs\": {\"key1\":\"value1\"}}]"},
	}
	result := make(map[string][]v1.AutomationStep, 0)
	workflowMap := getWorkflowMap(steps)
	assert.Equal(t, workflowMap, result)
}
