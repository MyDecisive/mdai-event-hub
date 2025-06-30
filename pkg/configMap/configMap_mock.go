package configMap

import (
	"context"
	v1 "github.com/decisiveai/mdai-operator/api/v1"
)

type ConfigMapManagerInterface interface {
	GetConfigMapForHub(ctx context.Context, hubName string) (map[string][]v1.AutomationStep, error)
	Cleanup()
	RemoveHubFetcher(hubName string)
}

type MockConfigMapManager struct {
	GetConfigMapForHubFunc func(ctx context.Context, hubName string) (map[string][]v1.AutomationStep, error)
	CleanupFunc            func()
	RemoveHubFetcherFunc   func(hubName string)

	// Track calls for verification
	GetConfigMapCalls  []string
	CleanupCalled      bool
	RemovedHubFetchers []string

	configMapMockData map[string]map[string][]v1.AutomationStep
}

func NewMockConfigMapManager() *MockConfigMapManager {
	return &MockConfigMapManager{
		GetConfigMapForHubFunc: func(ctx context.Context, hubName string) (map[string][]v1.AutomationStep, error) {
			return make(map[string][]v1.AutomationStep), nil
		},
		CleanupFunc:          func() {},
		RemoveHubFetcherFunc: func(hubName string) {},
		GetConfigMapCalls:    make([]string, 0),
		RemovedHubFetchers:   make([]string, 0),
		configMapMockData:    make(map[string]map[string][]v1.AutomationStep),
	}
}

func (m *MockConfigMapManager) GetConfigMapForHub(ctx context.Context, hubName string) (map[string][]v1.AutomationStep, error) {
	m.GetConfigMapCalls = append(m.GetConfigMapCalls, hubName)

	if data, exists := m.configMapMockData[hubName]; exists {
		return data, nil
	}

	return m.GetConfigMapForHubFunc(ctx, hubName)
}

func (m *MockConfigMapManager) Cleanup() {
	m.CleanupCalled = true
	m.CleanupFunc()
}

func (m *MockConfigMapManager) RemoveHubFetcher(hubName string) {
	m.RemovedHubFetchers = append(m.RemovedHubFetchers, hubName)
	m.RemoveHubFetcherFunc(hubName)
}

func (m *MockConfigMapManager) SetConfig(hubName string, workflowMap map[string][]v1.AutomationStep) {
	m.configMapMockData[hubName] = workflowMap
}

// Helper methods assertions
func (m *MockConfigMapManager) GetConfigMapCallsForHub(hubName string) int {
	count := 0
	for _, call := range m.GetConfigMapCalls {
		if call == hubName {
			count++
		}
	}
	return count
}

func (m *MockConfigMapManager) WasCleanupCalled() bool {
	return m.CleanupCalled
}

func (m *MockConfigMapManager) GetRemovedHubFetchers() []string {
	return m.RemovedHubFetchers
}

func (m *MockConfigMapManager) Reset() {
	m.GetConfigMapCalls = make([]string, 0)
	m.CleanupCalled = false
	m.RemovedHubFetchers = make([]string, 0)
	m.configMapMockData = make(map[string]map[string][]v1.AutomationStep)
}
