package audit

import (
	"context"
	"strconv"
	"time"

	"github.com/mydecisive/mdai-data-core/eventing"
	"github.com/mydecisive/mdai-data-core/eventing/rule"
	"go.uber.org/zap"
)

type Inserter interface {
	InsertAuditLogEventFromMap(ctx context.Context, eventMap map[string]string) error
}

func RecordAuditEventFromMdaiEvent(ctx context.Context, logger *zap.Logger, auditAdapter Inserter, event eventing.MdaiEvent, r *rule.Rule, success bool) error {
	automationName := ""
	if r != nil {
		automationName = r.Name
	}
	eventMap := map[string]string{
		"id":                   event.ID,
		"name":                 event.Name,
		"timestamp":            event.Timestamp.UTC().Format(time.RFC3339),
		"payload":              event.Payload,
		"source":               event.Source,
		"source_id":            event.SourceID,
		"correlation_id":       event.CorrelationID,
		"hub_name":             event.HubName,
		"automation_succeeded": strconv.FormatBool(success),
		"automation_name":      automationName,
	}
	logger.Info(
		"AUDIT: MdaiEvent handled",
		zap.String("mdai-logstream", "audit"),
		zap.Object("mdaiEvent", &event),
		zap.Bool("automation_succeeded", success),
		zap.String("automation_name", automationName),
	)
	return auditAdapter.InsertAuditLogEventFromMap(ctx, eventMap)
}
