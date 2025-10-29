package eventhub

import (
	"context"
	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

func (h *EventHub) HandleStartReplay(ctx context.Context, kubeClient dynamic.Interface, namespace string, payloadData map[string]any) error {
	replayName := payloadData["replayName"].(string)
	replaySpec, err := buildReplayCustomResourceSpec(payloadData)
	if err != nil {
		return err
	}
	replayCR := buildReplayCustomResource(replayName, namespace, replaySpec)

	_, applyErr := kubeClient.Resource(schema.GroupVersionResource{
		Group:    "hub.mydecisive.ai",
		Version:  "v1",
		Resource: "mdaireplay",
	}).Apply(ctx, replayName, replayCR, metav1.ApplyOptions{})

	return applyErr
}

func (h *EventHub) HandleReplayCompletion(ctx context.Context, kubeClient dynamic.Interface, payloadData map[string]any) error {
	replayName := payloadData["replayName"].(string)

	err := kubeClient.Resource(schema.GroupVersionResource{
		Group:    "hub.mydecisive.ai",
		Version:  "v1",
		Resource: "mdaireplay",
	}).Delete(ctx, replayName, metav1.DeleteOptions{})

	return err
}

func buildReplayCustomResource(name string, namespace string, spec map[string]any) *unstructured.Unstructured {
	cr := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "hub.mydecisive.ai/v1",
			"kind":       "MdaiReplay",
			"metadata": map[string]any{
				"name":      name,
				"namespace": namespace,
			},
			"spec": spec,
		},
	}
	return cr
}

func buildReplayCustomResourceSpec(payloadData map[string]any) (map[string]any, error) {
	awsAccessKeySecret := payloadData["awsAccessKeySecret"].(string)
	awsConfig := mdaiv1.MdaiReplayAwsConfig{
		AWSAccessKeySecret: &awsAccessKeySecret,
	}
	s3Config := mdaiv1.MdaiReplayS3Configuration{
		S3Region:    payloadData["s3Region"].(string),
		S3Bucket:    payloadData["s3Bucket"].(string),
		FilePrefix:  payloadData["filePrefix"].(string),
		S3Path:      payloadData["s3Path"].(string),
		S3Partition: payloadData["s3Partition"].(mdaiv1.MdaiReplayS3Partition),
	}
	otlpHttpConfig := mdaiv1.MdaiReplayOtlpHttpDestinationConfiguration{
		Endpoint: payloadData["endpoint"].(string),
	}
	replaySpec := map[string]any{
		"opampEndpoint": payloadData["opampEndpoint"].(string),
		"hubName":       payloadData["hubName"].(string),
		"startTime":     payloadData["startTime"].(string),
		"endTime":       payloadData["endTime"].(string),
		"telemetryType": payloadData["telemetryType"].(mdaiv1.MdaiReplayTelemetryType),
		"source": mdaiv1.MdaiReplaySourceConfiguration{
			AWSConfig: &awsConfig,
			S3:        &s3Config,
		},
		"destination": mdaiv1.MdaiReplayDestinationConfiguration{
			OtlpHttp: &otlpHttpConfig,
		},
	}
	return replaySpec, nil
}
