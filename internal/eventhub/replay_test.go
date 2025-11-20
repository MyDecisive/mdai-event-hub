package eventhub

import (
	"encoding/json"
	"testing"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
	mdaifake "github.com/decisiveai/mdai-operator/pkg/generated/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
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
		validateReplay func(t *testing.T, obj mdaiv1.MdaiReplay)
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
			validateReplay: func(t *testing.T, obj mdaiv1.MdaiReplay) {
				t.Helper()
				assert.Equal(t, "test-replay", obj.GetName())
				spec := obj.Spec
				assert.Equal(t, "2024-01-01T00:00:00Z", spec.StartTime)
				assert.Equal(t, "2024-01-02T00:00:00Z", spec.EndTime)
				assert.Equal(t, mdaiv1.MetricsReplayTelemetryType, spec.TelemetryType)
				assert.Equal(t, "test-hub", spec.HubName)
				assert.Equal(t, "http://opamp.example.com", spec.OpAMPEndpoint)
				assert.Equal(t, "replay-status-var", spec.StatusVariableRef)
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
			validateReplay: func(t *testing.T, obj mdaiv1.MdaiReplay) {
				t.Helper()
				spec := obj.Spec
				source := spec.Source
				s3 := source.S3

				assert.Equal(t, "us-east-1", s3.S3Region)
				assert.Equal(t, "telemetry-data", s3.S3Bucket)
				assert.Equal(t, "metrics/", s3.FilePrefix)
				assert.Equal(t, mdaiv1.S3ReplayHourPartition, s3.S3Partition)
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
			validateReplay: func(t *testing.T, obj mdaiv1.MdaiReplay) {
				t.Helper()
				spec := obj.Spec
				destination := spec.Destination
				otlpHTTP := destination.OtlpHttp
				assert.Equal(t, "http://otlp-collector:4318", otlpHTTP.Endpoint)
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
			validateReplay: func(t *testing.T, obj mdaiv1.MdaiReplay) {
				t.Helper()
				spec := obj.Spec

				assert.True(t, spec.IgnoreSendingQueue)

				resource := spec.Resource
				assert.Equal(t, "custom-replay-image:v1.2.3", resource.Image)

				source := spec.Source
				aws := source.AWSConfig
				assert.Equal(t, ptr.To("aws-creds-secret"), aws.AWSAccessKeySecret)
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
			ctx := t.Context()
			clientset := mdaifake.NewSimpleClientset().HubV1()

			err := HandleDeployReplay(ctx, clientset, tt.namespace, tt.event, tt.command, tt.payloadData)
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
				t.Helper()
				list, err := clientset.MdaiReplays(tt.namespace).List(ctx, metav1.ListOptions{})

				require.NoError(t, err)
				require.Len(t, list.Items, 1)
				tt.validateReplay(t, list.Items[0])
			}
		})
	}
}

func TestEventHub_HandleReplayCleanUp(t *testing.T) {
	tests := []struct {
		name         string
		payloadData  map[string]any
		namespace    string
		setupReplay  *mdaiv1.MdaiReplay
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
			setupReplay: &mdaiv1.MdaiReplay{
				TypeMeta: metav1.TypeMeta{
					Kind:       "MdaiReplay",
					APIVersion: "hub.decisiveai.com/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-replay",
					Namespace: "default",
				},
				Spec: mdaiv1.MdaiReplaySpec{},
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
			clientset := mdaifake.NewSimpleClientset().HubV1()

			ctx := t.Context()
			if tt.setupReplay != nil {
				_, err := clientset.MdaiReplays(tt.namespace).Create(ctx, tt.setupReplay, metav1.CreateOptions{})
				require.NoError(t, err)
			}

			err := HandleReplayCleanUp(ctx, clientset, tt.namespace, tt.payloadData)

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
				list, err := clientset.MdaiReplays(tt.namespace).List(ctx, metav1.ListOptions{})
				require.NoError(t, err)
				assert.Empty(t, list.Items, "replay should have been deleted")
			}
		})
	}
}

func TestEventHub_buildReplayCustomResource(t *testing.T) {
	tests := []struct {
		name       string
		replayName string
		spec       mdaiv1.MdaiReplaySpec
		validate   func(t *testing.T, replay *mdaiv1.MdaiReplay)
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
			validate: func(t *testing.T, cr *mdaiv1.MdaiReplay) {
				t.Helper()
				assert.Equal(t, "test-replay", cr.GetName())
				assert.Equal(t, "MdaiReplay", cr.Kind)

				apiVersion := cr.APIVersion
				assert.Contains(t, apiVersion, mdaiv1.GroupVersion.Group)
				assert.Contains(t, apiVersion, mdaiv1.GroupVersion.Version)

				spec := cr.Spec
				assert.Equal(t, "2024-01-01T00:00:00Z", spec.StartTime)
				assert.Equal(t, "2024-01-02T00:00:00Z", spec.EndTime)
				assert.Equal(t, mdaiv1.MdaiReplayTelemetryType("metrics"), spec.TelemetryType)
				assert.Equal(t, "test-hub", spec.HubName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := buildReplayCustomResource(tt.replayName, tt.spec)

			require.NotNil(t, cr)
			tt.validate(t, &cr)
		})
	}
}

// Helper functions.
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
