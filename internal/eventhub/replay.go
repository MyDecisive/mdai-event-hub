package eventhub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type ReplayScalarUpdatePayloadInputs struct {
	ReplayName    string                         `json:"replayName"`
	StartTime     string                         `json:"startTime"`
	EndTime       string                         `json:"endTime"`
	TelemetryType mdaiv1.MdaiReplayTelemetryType `json:"telemetryType"`
}

func (h *EventHub) HandleDeployReplay(ctx context.Context, dynamicClient dynamic.Interface, namespace string, ev eventing.MdaiEvent, cmd rule.Command, payloadData map[string]any) error {
	hubName := ev.HubName

	var replayCmdInputs mdaiv1.DeployReplayAction
	if err := json.Unmarshal(cmd.Inputs, &replayCmdInputs); err != nil {
		return err
	}

	replayPayloadInputsStr, ok := payloadData["data"].(string)
	if !ok {
		return errors.New("replay variable payload must contain inputs to decode")
	}
	var replayPayloadInputs ReplayScalarUpdatePayloadInputs
	if err := json.Unmarshal([]byte(replayPayloadInputsStr), &replayPayloadInputs); err != nil {
		return err
	}

	replaySpec := replayCmdInputs.ReplaySpec
	replaySpec.StartTime = replayPayloadInputs.StartTime
	replaySpec.EndTime = replayPayloadInputs.EndTime
	replaySpec.TelemetryType = replayPayloadInputs.TelemetryType
	replaySpec.HubName = hubName

	replayCR := h.buildReplayCustomResource(replayPayloadInputs.ReplayName, replaySpec)

	_, applyErr := dynamicClient.Resource(schema.GroupVersionResource{
		Group:    mdaiv1.GroupVersion.Group,
		Version:  mdaiv1.GroupVersion.Version,
		Resource: "mdaireplays",
	}).Namespace(namespace).Create(ctx, replayCR, metav1.CreateOptions{})

	return applyErr
}

type ReplayCompletion struct {
	ReplayName   string `json:"replay_name"`
	ReplayStatus string `json:"replay_status"`
}

func (h *EventHub) HandleReplayCleanUp(ctx context.Context, kubeClient dynamic.Interface, namespace string, payloadData map[string]any) error {
	completionJson, ok := payloadData["data"].(string)
	if !ok {
		return errors.New("no data field was found on cleanup event payload")
	}

	var completionObj ReplayCompletion
	if err := json.Unmarshal([]byte(completionJson), &completionObj); err != nil {
		return err
	}

	err := kubeClient.Resource(schema.GroupVersionResource{
		Group:    mdaiv1.GroupVersion.Group,
		Version:  mdaiv1.GroupVersion.Version,
		Resource: "mdaireplays",
	}).Namespace(namespace).Delete(ctx, completionObj.ReplayName, metav1.DeleteOptions{})

	return err
}

func (h *EventHub) buildReplayCustomResource(name string, spec mdaiv1.MdaiReplaySpec) *unstructured.Unstructured {
	cr := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": fmt.Sprintf("%s/%s", mdaiv1.GroupVersion.Group, mdaiv1.GroupVersion.Version),
			"kind":       "MdaiReplay",
			"metadata": map[string]any{
				"name": name,
			},
			"spec": spec,
		},
	}
	return cr
}
