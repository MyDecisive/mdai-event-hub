package main

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	dcorekube "github.com/decisiveai/mdai-data-core/kube"
	"github.com/decisiveai/mdai-event-hub/internal/handlers"
	"github.com/decisiveai/mdai-event-hub/pkg/eventing"
	operator "github.com/decisiveai/mdai-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	valkeymock "github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type testHandlerState struct {
	called bool
	err    error
}

func newTestHandler() (handlers.HandlerFunc, *testHandlerState) {
	state := &testHandlerState{}
	handler := func(_ handlers.MdaiInterface, _ eventing.MdaiEvent, _ map[string]string) error {
		state.called = true
		return state.err
	}
	return handler, state
}

func TestProcessEvent_Success(t *testing.T) {
	ctx := t.Context()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := valkeymock.NewClient(ctrl)
	logger := zap.NewNop()

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-hub-automation",
			Labels: map[string]string{
				dcorekube.ConfigMapTypeLabel: dcorekube.ManualEnvConfigMapType,
				dcorekube.LabelMdaiHubName:   "test-hub",
			},
		},
		Data: map[string]string{
			"TestAlert.firing": "[{\"handlerRef\" :\"testHandler\", \"args\": {\"key\":\"value\"}}]",
		},
	}

	clientset := fake.NewClientset(configMap)
	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap, metav1.CreateOptions{})

	cmController, err := dcorekube.NewConfigMapController(dcorekube.ManualEnvConfigMapType, "", clientset, logger)
	if err != nil {
		logger.Fatal("failed to create ConfigMap controller", zap.Error(err))
	}
	if cmControllerRunErr := cmController.Run(); cmControllerRunErr != nil {
		logger.Fatal("failed to run ConfigMap controller", zap.Error(cmControllerRunErr))
	}

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "TestAlert.firing",
	}

	testHandler, state := newTestHandler()

	handlerMap := handlers.GetSupportedHandlers(map[handlers.HandlerName]handlers.HandlerFunc{
		"testHandler": testHandler,
	})

	handler := ProcessEvent(ctx, mockClient, cmController, logger, nil, handlerMap)
	err = handler(event)

	require.NoError(t, err)
	assert.True(t, state.called)
}

func TestProcessEvent_NoHubName(t *testing.T) {
	ctx := t.Context()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := valkeymock.NewClient(ctrl)
	logger := zap.NewNop()

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-hub-automation",
			Labels: map[string]string{
				dcorekube.ConfigMapTypeLabel: dcorekube.ManualEnvConfigMapType,
				dcorekube.LabelMdaiHubName:   "test-hub",
			},
		},
		Data: map[string]string{
			"TestAlert.firing": "[{\"handlerRef\" :\"testHandler\", \"args\": {\"key\":\"value\"}}]",
		},
	}

	clientset := fake.NewClientset(configMap)
	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap, metav1.CreateOptions{})

	cmController, err := dcorekube.NewConfigMapController(dcorekube.ManualEnvConfigMapType, "", clientset, logger)
	if err != nil {
		logger.Fatal("failed to create ConfigMap controller", zap.Error(err))
	}
	if cmControllerRunErr := cmController.Run(); cmControllerRunErr != nil {
		logger.Fatal("failed to run ConfigMap controller", zap.Error(cmControllerRunErr))
	}
	defer cmController.Stop()

	event := eventing.MdaiEvent{
		Name: "TestAlert.firing",
	}

	testHandler, _ := newTestHandler()

	handlerMap := handlers.GetSupportedHandlers(map[handlers.HandlerName]handlers.HandlerFunc{
		"testHandler": testHandler,
	})

	handler := ProcessEvent(ctx, mockClient, cmController, logger, nil, handlerMap)
	err = handler(event)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no hub name provided")
}

func TestProcessEvent_MatchAlertNameOnly(t *testing.T) {
	ctx := t.Context()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := valkeymock.NewClient(ctrl)
	logger := zap.NewNop()

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-hub-automation",
			Labels: map[string]string{
				dcorekube.ConfigMapTypeLabel: dcorekube.ManualEnvConfigMapType,
				dcorekube.LabelMdaiHubName:   "test-hub",
			},
		},
		Data: map[string]string{
			"TestAlert": "[{\"handlerRef\" :\"testHandler\", \"args\": {\"key\":\"value\"}}]",
		},
	}

	clientset := fake.NewClientset(configMap)
	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap, metav1.CreateOptions{})

	cmController, err := dcorekube.NewConfigMapController(dcorekube.ManualEnvConfigMapType, "", clientset, logger)
	if err != nil {
		logger.Fatal("failed to create ConfigMap controller", zap.Error(err))
	}
	if cmControllerRunErr := cmController.Run(); cmControllerRunErr != nil {
		logger.Fatal("failed to run ConfigMap controller", zap.Error(cmControllerRunErr))
	}
	defer cmController.Stop()

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "TestAlert.firing",
	}

	testHandler, state := newTestHandler()

	handlerMap := handlers.GetSupportedHandlers(map[handlers.HandlerName]handlers.HandlerFunc{
		"testHandler": testHandler,
	})

	handler := ProcessEvent(ctx, mockClient, cmController, logger, nil, handlerMap)
	err = handler(event)

	require.NoError(t, err)
	assert.True(t, state.called)
}

func TestProcessEvent_NoWorkflowFound(t *testing.T) {
	ctx := t.Context()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := valkeymock.NewClient(ctrl)
	logger := zap.NewNop()

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-hub-automation",
			Labels: map[string]string{
				dcorekube.ConfigMapTypeLabel: dcorekube.ManualEnvConfigMapType,
				dcorekube.LabelMdaiHubName:   "test-hub",
			},
		},
	}

	clientset := fake.NewClientset(configMap)
	_, _ = clientset.CoreV1().ConfigMaps("first").Create(ctx, configMap, metav1.CreateOptions{})

	cmController, err := dcorekube.NewConfigMapController(dcorekube.ManualEnvConfigMapType, "", clientset, logger)
	if err != nil {
		logger.Fatal("failed to create ConfigMap controller", zap.Error(err))
	}
	if cmControllerRunErr := cmController.Run(); cmControllerRunErr != nil {
		logger.Fatal("failed to run ConfigMap controller", zap.Error(cmControllerRunErr))
	}
	defer cmController.Stop()

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "UnknownAlert.firing",
	}

	testHandler, _ := newTestHandler()

	handlerMap := handlers.GetSupportedHandlers(map[handlers.HandlerName]handlers.HandlerFunc{
		"testHandler": testHandler,
	})

	handler := ProcessEvent(ctx, mockClient, cmController, logger, nil, handlerMap)
	err = handler(event)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no configured automation for event: UnknownAlert.firing")
}

func TestSafePerformAutomationStep_Success(t *testing.T) {
	testHandler, state := newTestHandler()

	handlerMap := handlers.GetSupportedHandlers(map[handlers.HandlerName]handlers.HandlerFunc{
		"testHandler": testHandler,
	})

	mdai := handlers.MdaiInterface{
		Logger: zap.NewNop(),
	}

	autoStep := operator.AutomationStep{
		HandlerRef: "testHandler",
		Arguments:  map[string]string{"key": "value"},
	}

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "TestAlert.firing",
	}

	err := safePerformAutomationStep(handlerMap, mdai, autoStep, event)

	require.NoError(t, err)
	assert.True(t, state.called)
}

func TestSafePerformAutomationStep_UnsupportedHandler(t *testing.T) {
	mdai := handlers.MdaiInterface{
		Logger: zap.NewNop(),
	}

	autoStep := operator.AutomationStep{
		HandlerRef: "unsupportedHandler",
		Arguments:  map[string]string{"key": "value"},
	}

	event := eventing.MdaiEvent{
		HubName: "test-hub",
		Name:    "TestAlert.firing",
	}

	testHandler, _ := newTestHandler()

	handlerMap := handlers.GetSupportedHandlers(map[handlers.HandlerName]handlers.HandlerFunc{
		"testHandler": testHandler,
	})

	err := safePerformAutomationStep(handlerMap, mdai, autoStep, event)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "handler unsupportedHandler not supported")
}

func panicHandler(_ handlers.MdaiInterface, _ eventing.MdaiEvent, _ map[string]string) error {
	panic("simulated panic")
}

func TestSafePerformAutomationStep_PanicRecovery(t *testing.T) {
	handlerMap := handlers.GetSupportedHandlers(map[handlers.HandlerName]handlers.HandlerFunc{
		"panicHandler": panicHandler,
	})

	logger := zap.NewNop()
	mdai := handlers.MdaiInterface{
		Data:   nil,
		Logger: logger,
	}

	autoStep := operator.AutomationStep{
		HandlerRef: "panicHandler",
		Arguments:  map[string]string{},
	}

	event := eventing.MdaiEvent{
		ID:      "1",
		Name:    "TestEvent",
		HubName: "test-hub",
	}

	err := safePerformAutomationStep(handlerMap, mdai, autoStep, event)
	require.Error(t, err)

	expectedPrefix := "panic in handler " + autoStep.HandlerRef
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
		ID:      "e1",
		Name:    "test",
		HubName: "hub1",
		Payload: validJSON,
	}

	result, err := handlers.ProcessEventPayload(event)
	require.NoError(t, err, "expected no error for valid JSON payload")

	assert.Contains(t, result, "key1")
	assert.Contains(t, result, "key2")
	assert.Contains(t, result, "nested")

	assert.Equal(t, "value1", result["key1"])
	assert.InDelta(t, float64(42), result["key2"], 0.001)
	nested, ok := result["nested"].(map[string]any)
	assert.True(t, ok, "expected nested to be a map[string]any")
	assert.Equal(t, "val", nested["sub"])
}

func TestProcessEventPayload_InvalidJSON(t *testing.T) {
	invalidJSON := `{"key1":"value1", "key2":}`
	event := eventing.MdaiEvent{
		ID:      "e2",
		Name:    "test-invalid",
		HubName: "hub2",
		Payload: invalidJSON,
	}

	result, err := handlers.ProcessEventPayload(event)
	assert.Nil(t, result, "expected result to be nil on invalid JSON")
	require.Error(t, err, "expected an error for invalid JSON payload")
	assert.Contains(t, err.Error(), "failed to unmarshal payload")
}

func TestGetWorkflowMap(t *testing.T) {
	steps := []map[string]string{
		{"TestAlert1.firing": "[{\"handlerRef\" :\"testHandler1\", \"args\": {\"key1\":\"value1\"}}]"},
		{"TestAlert2.firing": "[{\"handlerRef\" :\"testHandler2\", \"args\": {\"key2\":\"value2\"}}]"},
	}
	expectedWorkflowMap := map[string][]operator.AutomationStep{
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
	workflowMap := getWorkflowMap(zap.NewNop(), steps)
	assert.Equal(t, expectedWorkflowMap, workflowMap)
}

func TestGetWorkflowMap_InvalidStepsJson(t *testing.T) {
	steps := []map[string]string{
		{"TestAlert1.firing": "[{\"hhhhandlerRef\" :\"testHandler1\", \"aaaargs\": {\"key1\":\"value1\"}}]"},
	}
	result := make(map[string][]operator.AutomationStep, 0)
	workflowMap := getWorkflowMap(zap.NewNop(), steps)
	assert.Equal(t, workflowMap, result)
}

func TestMain_CustomValkeyExpiryEnvVar(t *testing.T) {
	key := valkeyAuditStreamExpiryMSEnvVarKey
	t.Setenv(key, "86400000") // 1 day in ms

	expiryMsStr := os.Getenv(key)
	ms, err := strconv.Atoi(expiryMsStr)
	require.NoError(t, err)

	expiry := time.Duration(ms) * time.Millisecond
	assert.Equal(t, 24*time.Hour, expiry)
}

func TestCreateLogger(t *testing.T) {
	logger := createLogger()
	assert.NotNil(t, logger)
	logger.Info("test message") // No crash = pass
}
