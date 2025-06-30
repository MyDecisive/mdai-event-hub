package configMap

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	v1 "github.com/decisiveai/mdai-operator/api/v1"
	"go.uber.org/zap"
	"k8s.io/client-go/tools/clientcmd"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var _ ConfigMapManagerInterface = (*Manager)(nil)

type Manager struct {
	clientset    *kubernetes.Clientset
	namespace    string
	logger       *zap.Logger
	fetchers     map[string]*Fetcher
	fetchersLock sync.RWMutex
	suffix       string
}

type Fetcher struct {
	clientset     *kubernetes.Clientset
	namespace     string
	configMapName string
	data          map[string]string
	dataLock      sync.RWMutex
	logger        *zap.Logger
	lastUpdated   time.Time
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewConfigMapManager(logger *zap.Logger, suffix string) (*Manager, error) {
	// attempting to create inCLuster k8s client first - if fails, tries to create out of cluster client
	// so can be started locally for dev purposes
	config, err := rest.InClusterConfig()
	if err != nil {
		kubeconfig, err := os.UserHomeDir()
		if err != nil {
			logger.Error("Failed to load k8s config", zap.Error(err))
			return nil, err
		}

		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig+"/.kube/config")
		if err != nil {
			logger.Error("Failed to build k8s config", zap.Error(err))
			return nil, err
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create clientset: %v", err)
	}

	// TODO: Replace with getEnvVar helper
	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
		if err == nil {
			namespace = string(data)
		} else {
			namespace = "default"
		}
	}

	return &Manager{
		clientset: clientset,
		namespace: namespace,
		logger:    logger,
		fetchers:  make(map[string]*Fetcher),
		suffix:    suffix,
	}, nil
}

// GetConfigMapForHub returns the ConfigMap data for a specific hub, creating a new Manager if one is not found
func (m *Manager) GetConfigMapForHub(ctx context.Context, hubName string) (map[string][]v1.AutomationStep, error) {
	configMapName := hubName + m.suffix

	m.fetchersLock.RLock()
	fetcher, exists := m.fetchers[hubName]
	m.fetchersLock.RUnlock()

	if !exists {
		m.logger.Info("Creating new fetcher for hub", zap.String("HubName", hubName), zap.String("ConfigMapName", configMapName))

		fetcherCtx, cancel := context.WithCancel(context.Background())
		fetcher = &Fetcher{
			clientset:     m.clientset,
			namespace:     m.namespace,
			configMapName: configMapName,
			data:          make(map[string]string),
			logger:        m.logger,
			ctx:           fetcherCtx,
			cancel:        cancel,
		}

		go fetcher.watchConfigMap()

		m.fetchersLock.Lock()
		m.fetchers[hubName] = fetcher
		m.fetchersLock.Unlock()

		err := fetcher.fetchConfigMap(ctx)
		if err != nil {
			m.logger.Warn("Warning: Initial fetch for hub failed", zap.String("HubName", hubName), zap.Error(err))
			// We don't return error here, as the watch might succeed later
		}
	}

	fetcher.dataLock.RLock()
	defer fetcher.dataLock.RUnlock()

	result := make(map[string][]v1.AutomationStep, len(fetcher.data))

	for k, v := range fetcher.data {
		var workflow []v1.AutomationStep

		if err := json.Unmarshal([]byte(v), &workflow); err != nil {
			m.logger.Warn("could not unmarshall workflow", zap.String("key", k), zap.Error(err))
			continue
		}

		result[k] = workflow
	}

	return result, nil
}

func (m *Manager) Cleanup() {
	m.fetchersLock.Lock()
	defer m.fetchersLock.Unlock()

	for hubName, fetcher := range m.fetchers {
		m.logger.Info("Stopping watcher for hub", zap.String("hubName", hubName))
		fetcher.cancel()
	}
}

func (m *Manager) RemoveHubFetcher(hubName string) {
	m.fetchersLock.Lock()
	defer m.fetchersLock.Unlock()

	if fetcher, exists := m.fetchers[hubName]; exists {
		m.logger.Info("Removing fetcher for hub", zap.String("hubName", hubName))
		fetcher.cancel()
		delete(m.fetchers, hubName)
	}
}

func (f *Fetcher) watchConfigMap() {
	backoff := 1 * time.Second
	maxBackoff := 60 * time.Second

	for {
		select {
		case <-f.ctx.Done():
			f.logger.Info("Watcher for ConfigMap stopped", zap.String("configMapName", f.configMapName))
			return
		default:
			// Continue with watch setup
		}

		f.logger.Info("Setting up watcher for ConfigMap", zap.String("configMapName", f.configMapName))
		watcher, err := f.clientset.CoreV1().ConfigMaps(f.namespace).Watch(f.ctx, metav1.ListOptions{
			FieldSelector: fmt.Sprintf("metadata.name=%s", f.configMapName),
		})

		if err != nil {
			f.logger.Error("Failed to watch ConfigMap. Retrying.",
				zap.String("configMapName", f.configMapName), zap.Error(err), zap.Duration("Retry backoff", backoff))

			select {
			case <-time.After(backoff):
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			case <-f.ctx.Done():
				return
			}
			continue
		}

		backoff = 1 * time.Second

		f.logger.Info("Successfully set up watcher for ConfigMap", zap.String("configMapName", f.configMapName))

		for event := range watcher.ResultChan() {
			if f.ctx.Err() != nil {
				watcher.Stop()
				return
			}

			switch event.Type {
			case "ADDED", "MODIFIED":
				configMap, ok := event.Object.(*corev1.ConfigMap)
				if !ok {
					f.logger.Warn("Unexpected object type:", zap.Any("Event Object", event.Object))
					continue
				}

				f.dataLock.Lock()
				f.data = configMap.Data
				f.lastUpdated = time.Now()
				f.dataLock.Unlock()

				f.logger.Info("ConfigMap entries updated",
					zap.String("configMapName", f.configMapName), zap.String("EventType", string(event.Type)), zap.Int("Entries", len(configMap.Data)))

			case "DELETED":
				f.logger.Info("ConfigMap was deleted", zap.String("ConfigMapName", f.configMapName))
				// Keep the last known data but mark it as potentially stale
			}
		}

		f.logger.Info("Watcher for ConfigMap ended, will reconnect", zap.String("ConfigMapName", f.configMapName))
	}
}

func (f *Fetcher) fetchConfigMap(ctx context.Context) error {
	configMap, err := f.clientset.CoreV1().ConfigMaps(f.namespace).Get(ctx, f.configMapName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get ConfigMap %s: %v", f.configMapName, err)
	}

	f.dataLock.Lock()
	f.data = configMap.Data
	f.lastUpdated = time.Now()
	f.dataLock.Unlock()

	f.logger.Info("Successfully fetched ConfigMap with entries", zap.String("ConfigMapName", f.configMapName), zap.Int("Entries", len(configMap.Data)))
	return nil
}
