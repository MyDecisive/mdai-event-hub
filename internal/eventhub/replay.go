package eventhub

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	mdaiv1 "github.com/decisiveai/mdai-operator/api/v1"
	mdaiclientset "github.com/decisiveai/mdai-operator/pkg/generated/clientset/versioned/typed/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type ReplayScalarUpdatePayloadInputs struct {
	ReplayName    string                         `json:"replayName"`    // nolint:tagliatelle
	StartTime     string                         `json:"startTime"`     // nolint:tagliatelle
	EndTime       string                         `json:"endTime"`       // nolint:tagliatelle
	TelemetryType mdaiv1.MdaiReplayTelemetryType `json:"telemetryType"` // nolint:tagliatelle
}

func HandleDeployReplay(ctx context.Context, clientset mdaiclientset.HubV1Interface, namespace string, ev eventing.MdaiEvent, cmd rule.Command, payloadData map[string]any) error {
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

	replayCR := buildReplayCustomResource(replayPayloadInputs.ReplayName, replaySpec)
	_, applyErr := clientset.MdaiReplays(namespace).Create(ctx, &replayCR, metav1.CreateOptions{})

	return applyErr
}

type ReplayCompletion struct {
	ReplayName   string `json:"replay_name"`
	ReplayStatus string `json:"replay_status"`
}

func HandleReplayCleanUp(ctx context.Context, clientset mdaiclientset.HubV1Interface, namespace string, payloadData map[string]any) error {
	completionJSON, ok := payloadData["data"].(string)
	if !ok {
		return errors.New("no data field was found on cleanup event payload")
	}

	var completionObj ReplayCompletion
	if err := json.Unmarshal([]byte(completionJSON), &completionObj); err != nil {
		return err
	}

	return clientset.MdaiReplays(namespace).Delete(ctx, completionObj.ReplayName, metav1.DeleteOptions{})
}

func buildReplayCustomResource(name string, spec mdaiv1.MdaiReplaySpec) mdaiv1.MdaiReplay {
	cr := mdaiv1.MdaiReplay{
		Spec: spec,
		TypeMeta: metav1.TypeMeta{
			Kind:       "MdaiReplay",
			APIVersion: mdaiv1.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
	return cr
}
