package eventhub

import (
	"context"
	"encoding/json"
	"k8s.io/utils/ptr"
	"testing"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/fake"
)

func TestEventHub_HandleDeployReplay(t *testing.T) {
	tests := []struct {
		name           string
		event          eventing.MdaiEvent
		command        rule.Command
		payloadData    map[string]any
		namespace      string
		wantErr        bool
		errContains    string
		validateReplay func(t *testing.T, obj *unstructured.Unstructured)
	}{
		{
			name: "successful replay deployment with basic config",
			event: eventing.MdaiEvent{
				HubName: "test-hub",
			},
			command: rule.Command{
				Inputs: mustMarshal(t, mdaiv1.DeployReplayAction{
					ReplaySpec: mdaiv1.MdaiReplaySpec{
						OpAMPEndpoint:     "http://opamp.example.com",
						StatusVariableRef: "replay-status-var",
					},
				}),
			},
			payloadData: map[string]any{
				"data": mustMarshalString(t, ReplayScalarUpdatePayloadInputs{
					ReplayName:    "test-replay",
					StartTime:     "2024-01-01T00:00:00Z",
					EndTime:       "2024-01-02T00:00:00Z",
					TelemetryType: mdaiv1.MetricsReplayTelemetryType,
				}),
			},
			namespace: "default",
			wantErr:   false,
			validateReplay: func(t *testing.T, obj *unstructured.Unstructured) {
				assert.Equal(t, "test-replay", obj.GetName())
				spec, found, err := unstructured.NestedMap(obj.Object, "spec")
				require.NoError(t, err)
				require.True(t, found)
				assert.Equal(t, "2024-01-01T00:00:00Z", spec["startTime"])
				assert.Equal(t, "2024-01-02T00:00:00Z", spec["endTime"])
				assert.Equal(t, string(mdaiv1.MetricsReplayTelemetryType), spec["telemetryType"])
				assert.Equal(t, "test-hub", spec["hubName"])
				assert.Equal(t, "http://opamp.example.com", spec["opampEndpoint"])
				assert.Equal(t, "replay-status-var", spec["statusVariableRef"])
			},
		},
		{
			name: "replay deployment with S3 source configuration",
			event: eventing.MdaiEvent{
				HubName: "prod-hub",
			},
			command: rule.Command{
				Inputs: mustMarshal(t, mdaiv1.DeployReplayAction{
					ReplaySpec: mdaiv1.MdaiReplaySpec{
						OpAMPEndpoint:     "http://opamp.example.com",
						StatusVariableRef: "replay-status",
						Source: mdaiv1.MdaiReplaySourceConfiguration{
							S3: &mdaiv1.MdaiReplayS3Configuration{
								S3Region:    "us-east-1",
								S3Bucket:    "telemetry-data",
								FilePrefix:  "metrics/",
								S3Path:      "prod/data",
								S3Partition: mdaiv1.S3ReplayHourPartition,
							},
						},
					},
				}),
			},
			payloadData: map[string]any{
				"data": mustMarshalString(t, ReplayScalarUpdatePayloadInputs{
					ReplayName:    "s3-replay",
					StartTime:     "2024-01-01T00:00:00Z",
					EndTime:       "2024-01-01T12:00:00Z",
					TelemetryType: mdaiv1.LogsReplayTelemetryType,
				}),
			},
			namespace: "default",
			wantErr:   false,
			validateReplay: func(t *testing.T, obj *unstructured.Unstructured) {
				spec, found, err := unstructured.NestedMap(obj.Object, "spec")
				require.NoError(t, err)
				require.True(t, found)

				source, found, err := unstructured.NestedMap(spec, "source")
				require.NoError(t, err)
				require.True(t, found)

				s3, found, err := unstructured.NestedMap(source, "s3")
				require.NoError(t, err)
				require.True(t, found)
				assert.Equal(t, "us-east-1", s3["s3Region"])
				assert.Equal(t, "telemetry-data", s3["s3Bucket"])
				assert.Equal(t, "metrics/", s3["filePrefix"])
				assert.Equal(t, string(mdaiv1.S3ReplayHourPartition), s3["s3Partition"])
			},
		},
		{
			name: "replay deployment with OTLP destination",
			event: eventing.MdaiEvent{
				HubName: "test-hub",
			},
			command: rule.Command{
				Inputs: mustMarshal(t, mdaiv1.DeployReplayAction{
					ReplaySpec: mdaiv1.MdaiReplaySpec{
						OpAMPEndpoint:     "http://opamp.example.com",
						StatusVariableRef: "replay-status",
						Destination: mdaiv1.MdaiReplayDestinationConfiguration{
							OtlpHttp: &mdaiv1.MdaiReplayOtlpHttpDestinationConfiguration{
								Endpoint: "http://otlp-collector:4318",
							},
						},
					},
				}),
			},
			payloadData: map[string]any{
				"data": mustMarshalString(t, ReplayScalarUpdatePayloadInputs{
					ReplayName:    "otlp-replay",
					StartTime:     "2024-01-01T00:00:00Z",
					EndTime:       "2024-01-01T01:00:00Z",
					TelemetryType: mdaiv1.TracesReplayTelemetryType,
				}),
			},
			namespace: "default",
			wantErr:   false,
			validateReplay: func(t *testing.T, obj *unstructured.Unstructured) {
				spec, found, err := unstructured.NestedMap(obj.Object, "spec")
				require.NoError(t, err)
				require.True(t, found)

				destination, found, err := unstructured.NestedMap(spec, "destination")
				require.NoError(t, err)
				require.True(t, found)

				otlpHttp, found, err := unstructured.NestedMap(destination, "otlpHttp")
				require.NoError(t, err)
				require.True(t, found)
				assert.Equal(t, "http://otlp-collector:4318", otlpHttp["endpoint"])
			},
		},
		{
			name: "replay deployment with AWS credentials and custom image",
			event: eventing.MdaiEvent{
				HubName: "secure-hub",
			},
			command: rule.Command{
				Inputs: mustMarshal(t, mdaiv1.DeployReplayAction{
					ReplaySpec: mdaiv1.MdaiReplaySpec{
						OpAMPEndpoint:      "http://opamp.example.com",
						StatusVariableRef:  "replay-status",
						IgnoreSendingQueue: true,
						Source: mdaiv1.MdaiReplaySourceConfiguration{
							AWSConfig: &mdaiv1.MdaiReplayAwsConfig{
								AWSAccessKeySecret: ptr.To("aws-creds-secret"),
							},
							S3: &mdaiv1.MdaiReplayS3Configuration{
								S3Region:    "eu-west-1",
								S3Bucket:    "secure-bucket",
								FilePrefix:  "traces/",
								S3Path:      "production",
								S3Partition: mdaiv1.S3ReplayMinutePartition,
							},
						},
						Resource: mdaiv1.MdaiReplayResourceConfiguration{
							Image: "custom-replay-image:v1.2.3",
						},
					},
				}),
			},
			payloadData: map[string]any{
				"data": mustMarshalString(t, ReplayScalarUpdatePayloadInputs{
					ReplayName:    "secure-replay",
					StartTime:     "2024-01-01T00:00:00Z",
					EndTime:       "2024-01-01T00:30:00Z",
					TelemetryType: mdaiv1.TracesReplayTelemetryType,
				}),
			},
			namespace: "secure-ns",
			wantErr:   false,
			validateReplay: func(t *testing.T, obj *unstructured.Unstructured) {
				spec, found, err := unstructured.NestedMap(obj.Object, "spec")
				require.NoError(t, err)
				require.True(t, found)

				assert.Equal(t, true, spec["ignoreSendingQueue"])

				resource, found, err := unstructured.NestedMap(spec, "resource")
				require.NoError(t, err)
				require.True(t, found)
				assert.Equal(t, "custom-replay-image:v1.2.3", resource["image"])

				source, found, err := unstructured.NestedMap(spec, "source")
				require.NoError(t, err)
				require.True(t, found)

				aws, found, err := unstructured.NestedMap(source, "aws")
				require.NoError(t, err)
				require.True(t, found)
				assert.Equal(t, "aws-creds-secret", aws["awsAccessKeySecret"])
			},
		},
		{
			name: "invalid command inputs JSON",
			event: eventing.MdaiEvent{
				HubName: "test-hub",
			},
			command: rule.Command{
				Inputs: []byte("invalid json"),
			},
			payloadData: map[string]any{
				"data": "{}",
			},
			namespace:   "default",
			wantErr:     true,
			errContains: "invalid character",
		},
		{
			name: "missing payload data field",
			event: eventing.MdaiEvent{
				HubName: "test-hub",
			},
			command: rule.Command{
				Inputs: mustMarshal(t, mdaiv1.DeployReplayAction{}),
			},
			payloadData: map[string]any{},
			namespace:   "default",
			wantErr:     true,
			errContains: "replay variable payload must contain inputs to decode",
		},
		{
			name: "payload data is not a string",
			event: eventing.MdaiEvent{
				HubName: "test-hub",
			},
			command: rule.Command{
				Inputs: mustMarshal(t, mdaiv1.DeployReplayAction{}),
			},
			payloadData: map[string]any{
				"data": 123,
			},
			namespace:   "default",
			wantErr:     true,
			errContains: "replay variable payload must contain inputs to decode",
		},
		{
			name: "invalid replay payload JSON",
			event: eventing.MdaiEvent{
				HubName: "test-hub",
			},
			command: rule.Command{
				Inputs: mustMarshal(t, mdaiv1.DeployReplayAction{}),
			},
			payloadData: map[string]any{
				"data": "invalid json",
			},
			namespace:   "default",
			wantErr:     true,
			errContains: "invalid character",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = mdaiv1.AddToScheme(scheme)

			dynamicClient := fake.NewSimpleDynamicClient(scheme)
			h := &EventHub{}
			ctx := context.Background()

			err := h.HandleDeployReplay(ctx, dynamicClient, tt.namespace, tt.event, tt.command, tt.payloadData)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)

			// Validate the created resource if validation function is provided
			if tt.validateReplay != nil {
				list, err := dynamicClient.Resource(schema.GroupVersionResource{
					Group:    mdaiv1.GroupVersion.Group,
					Version:  mdaiv1.GroupVersion.Version,
					Resource: "mdaireplays",
				}).Namespace(tt.namespace).List(ctx, metav1.ListOptions{})
				require.NoError(t, err)
				require.Len(t, list.Items, 1)
				tt.validateReplay(t, &list.Items[0])
			}
		})
	}
}

func TestEventHub_HandleReplayCleanUp(t *testing.T) {
	tests := []struct {
		name         string
		payloadData  map[string]any
		namespace    string
		setupReplay  *unstructured.Unstructured
		wantErr      bool
		errContains  string
		shouldDelete bool
	}{
		{
			name: "successful replay cleanup",
			payloadData: map[string]any{
				"data": mustMarshalString(t, ReplayCompletion{
					ReplayName:   "test-replay",
					ReplayStatus: "completed",
				}),
			},
			namespace: "default",
			setupReplay: &unstructured.Unstructured{
				Object: map[string]any{
					"apiVersion": "mdai.decisiveai.com/v1",
					"kind":       "MdaiReplay",
					"metadata": map[string]any{
						"name":      "test-replay",
						"namespace": "default",
					},
					"spec": map[string]any{},
				},
			},
			wantErr:      false,
			shouldDelete: true,
		},
		{
			name:        "missing data field in payload",
			payloadData: map[string]any{},
			namespace:   "default",
			wantErr:     true,
			errContains: "no data field was found on cleanup event payload",
		},
		{
			name: "data field is not a string",
			payloadData: map[string]any{
				"data": 123,
			},
			namespace:   "default",
			wantErr:     true,
			errContains: "no data field was found on cleanup event payload",
		},
		{
			name: "invalid completion JSON",
			payloadData: map[string]any{
				"data": "invalid json",
			},
			namespace:   "default",
			wantErr:     true,
			errContains: "invalid character",
		},
		{
			name: "replay not found (should return error from delete)",
			payloadData: map[string]any{
				"data": mustMarshalString(t, ReplayCompletion{
					ReplayName:   "nonexistent-replay",
					ReplayStatus: "completed",
				}),
			},
			namespace:   "default",
			wantErr:     true,
			errContains: "not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = mdaiv1.AddToScheme(scheme)

			var dynamicClient dynamic.Interface
			if tt.setupReplay != nil {
				dynamicClient = fake.NewSimpleDynamicClient(scheme, tt.setupReplay)
			} else {
				dynamicClient = fake.NewSimpleDynamicClient(scheme)
			}

			h := &EventHub{}
			ctx := context.Background()

			err := h.HandleReplayCleanUp(ctx, dynamicClient, tt.namespace, tt.payloadData)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				return
			}

			require.NoError(t, err)

			// Verify the replay was deleted if expected
			if tt.shouldDelete {
				list, err := dynamicClient.Resource(schema.GroupVersionResource{
					Group:    mdaiv1.GroupVersion.Group,
					Version:  mdaiv1.GroupVersion.Version,
					Resource: "mdaireplays",
				}).Namespace(tt.namespace).List(ctx, metav1.ListOptions{})
				require.NoError(t, err)
				assert.Len(t, list.Items, 0, "replay should have been deleted")
			}
		})
	}
}

func TestEventHub_buildReplayCustomResource(t *testing.T) {
	tests := []struct {
		name       string
		replayName string
		spec       mdaiv1.MdaiReplaySpec
		validate   func(t *testing.T, cr *unstructured.Unstructured)
	}{
		{
			name:       "basic replay resource",
			replayName: "test-replay",
			spec: mdaiv1.MdaiReplaySpec{
				StartTime:     "2024-01-01T00:00:00Z",
				EndTime:       "2024-01-02T00:00:00Z",
				TelemetryType: "metrics",
				HubName:       "test-hub",
			},
			validate: func(t *testing.T, cr *unstructured.Unstructured) {
				assert.Equal(t, "test-replay", cr.GetName())
				assert.Equal(t, "MdaiReplay", cr.GetKind())

				apiVersion, found, err := unstructured.NestedString(cr.Object, "apiVersion")
				require.NoError(t, err)
				require.True(t, found)
				assert.Contains(t, apiVersion, mdaiv1.GroupVersion.Group)
				assert.Contains(t, apiVersion, mdaiv1.GroupVersion.Version)

				spec, found, err := unstructured.NestedMap(cr.Object, "spec")
				require.NoError(t, err)
				require.True(t, found)
				assert.Equal(t, "2024-01-01T00:00:00Z", spec["startTime"])
				assert.Equal(t, "2024-01-02T00:00:00Z", spec["endTime"])
				assert.Equal(t, "metrics", spec["telemetryType"])
				assert.Equal(t, "test-hub", spec["hubName"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &EventHub{}
			cr := h.buildReplayCustomResource(tt.replayName, tt.spec)

			require.NotNil(t, cr)
			tt.validate(t, cr)
		})
	}
}

// Helper functions
func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	return data
}

func mustMarshalString(t *testing.T, v any) string {
	t.Helper()
	return string(mustMarshal(t, v))
}
