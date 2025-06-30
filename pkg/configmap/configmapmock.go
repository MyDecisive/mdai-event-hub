package configmap

import (
	"context"
	v1 "github.com/decisiveai/mdai-operator/api/v1"
)

type ManagerInterface interface {
	GetConfigMapForHub(ctx context.Context, hubName string) (map[string][]v1.AutomationStep, error)
	Cleanup()
	RemoveHubFetcher(hubName string)
}

type ManagerMock struct {
	GetConfigMapForHubFunc func(ctx context.Context, hubName string) (map[string][]v1.AutomationStep, error)
	CleanupFunc            func()
	RemoveHubFetcherFunc   func(hubName string)

	// Track calls for verification
	GetConfigMapCalls  []string
	CleanupCalled      bool
	RemovedHubFetchers []string

	configMapMockData map[string]map[string][]v1.AutomationStep
}

func NewManagerMock() *ManagerMock {
	return &ManagerMock{
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

func (m *ManagerMock) GetConfigMapForHub(ctx context.Context, hubName string) (map[string][]v1.AutomationStep, error) {
	m.GetConfigMapCalls = append(m.GetConfigMapCalls, hubName)

	if data, exists := m.configMapMockData[hubName]; exists {
		return data, nil
	}

	return m.GetConfigMapForHubFunc(ctx, hubName)
}

func (m *ManagerMock) Cleanup() {
	m.CleanupCalled = true
	m.CleanupFunc()
}

func (m *ManagerMock) RemoveHubFetcher(hubName string) {
	m.RemovedHubFetchers = append(m.RemovedHubFetchers, hubName)
	m.RemoveHubFetcherFunc(hubName)
}

func (m *ManagerMock) SetConfig(hubName string, workflowMap map[string][]v1.AutomationStep) {
	m.configMapMockData[hubName] = workflowMap
}

// Helper methods assertions
func (m *ManagerMock) GetConfigMapCallsForHub(hubName string) int {
	count := 0
	for _, call := range m.GetConfigMapCalls {
		if call == hubName {
			count++
		}
	}
	return count
}

func (m *ManagerMock) WasCleanupCalled() bool {
	return m.CleanupCalled
}

func (m *ManagerMock) GetRemovedHubFetchers() []string {
	return m.RemovedHubFetchers
}

func (m *ManagerMock) Reset() {
	m.GetConfigMapCalls = make([]string, 0)
	m.CleanupCalled = false
	m.RemovedHubFetchers = make([]string, 0)
	m.configMapMockData = make(map[string]map[string][]v1.AutomationStep)
}
