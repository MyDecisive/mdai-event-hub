package audit

import (
	"testing"
	"time"

	"github.com/decisiveai/mdai-data-core/eventing"
	"github.com/decisiveai/mdai-data-core/eventing/rule"
	"github.com/decisiveai/mdai-event-hub/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestRecordAuditEventFromMdaiEvent(t *testing.T) {
	core, obs := observer.New(zap.InfoLevel)
	logger := zap.New(core)
	t.Cleanup(func() { _ = logger.Sync() })

	mockAudit := &mocks.MockAuditAdapter{}

	event := eventing.MdaiEvent{
		ID:            "id1",
		Name:          "event_name",
		Timestamp:     time.Date(2025, 7, 19, 12, 0, 0, 0, time.UTC),
		Payload:       "{}",
		Source:        "source",
		SourceID:      "src1",
		CorrelationID: "cid1",
		HubName:       "hub",
	}
	testRule := rule.Rule{Name: "rule1"}

	expectedMap := map[string]string{
		"id":                   "id1",
		"name":                 "event_name",
		"timestamp":            "2025-07-19T12:00:00Z",
		"payload":              "{}",
		"source":               "source",
		"source_id":            "src1",
		"correlation_id":       "cid1",
		"hub_name":             "hub",
		"automation_name":      "rule1",
		"automation_succeeded": "true",
	}

	// Expect exactly the map we build in RecordAuditEventFromMdaiEvent
	mockAudit.On("InsertAuditLogEventFromMap", t.Context(), expectedMap).Return(nil).Once()

	err := RecordAuditEventFromMdaiEvent(t.Context(), logger, mockAudit, event, &testRule, true)
	require.NoError(t, err)
	mockAudit.AssertExpectations(t)

	// Verify log
	logs := obs.All()
	require.Len(t, logs, 1)
	log := logs[0]

	assert.Equal(t, "AUDIT: MdaiEvent handled", log.Message)

	ctx := log.ContextMap()
	assert.Equal(t, "audit", ctx["mdai-logstream"])
	assert.Equal(t, true, ctx["automation_succeeded"]) // logged as zap.Bool
	assert.Equal(t, "rule1", ctx["automation_name"])   // logged as zap.String

	// We logged the event as zap.Object; just assert the field exists.
	_, ok := ctx["mdaiEvent"]
	require.True(t, ok, "mdaiEvent field should be present in log")
}
