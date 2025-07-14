package celeval

import (
	"strings"
	"testing"
	"time"
)

func removeCurlyBraces(condition string) string {
	return strings.ReplaceAll(strings.ReplaceAll(condition, "{{", ""), "}}", "")
}

func BenchmarkConditionEvaluator_EvaluateCondition(b *testing.B) {
	conditionEvaluator := NewConditionEvaluator()

	event := &CommandEvent{
		Id:              "event-123",
		Source:          "alertmanager",
		Subject:         "alert.alertmanager.firing",
		DataContentType: "application/json",
		Time:            time.Now(),
		HubName:         "production",
		Data: map[string]interface{}{
			"alertname":    "logBytesOutTooHighBySvc",
			"status":       "firing",
			"service_name": "payment-service",
			"severity":     "warning",
		},
		CorrelationId: "corr-456",
		CausationId:   "cause-789",
	}

	config := map[string]interface{}{
		"severity_threshold": "warning",
		"rate_limit": map[string]interface{}{
			"max_events": 100,
		},
	}

	condition := `{{alertname}} == "logBytesOutTooHighBySvc" && {{status}} == "firing" && {{severity}} == {{config.severity_threshold}}`

	compiledCondition, err := conditionEvaluator.CompileCondition(condition, true)
	if err != nil {
		b.Fatalf("Condition compilation error: %v\n", err)
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := conditionEvaluator.EvaluateCondition(compiledCondition, event, config)
		if err != nil {
			b.Fatalf("Condition evaluation error: %v\n", err)
			return
		}
	}
}

func BenchmarkConditionEvaluator_EvaluateConditionWithEnv(b *testing.B) {
	conditionEvaluator := NewConditionEvaluator()

	event := &CommandEvent{
		Id:              "event-123",
		Source:          "alertmanager",
		Subject:         "alert.alertmanager.firing",
		DataContentType: "application/json",
		Time:            time.Now(),
		HubName:         "production",
		Data: map[string]interface{}{
			"alertname":    "logBytesOutTooHighBySvc",
			"status":       "firing",
			"service_name": "payment-service",
			"severity":     "warning",
		},
		CorrelationId: "corr-456",
		CausationId:   "cause-789",
	}

	config := map[string]interface{}{
		"severity_threshold": "warning",
		"rate_limit": map[string]interface{}{
			"max_events": 100,
		},
	}

	envExpression := `{{alertname}} == "logBytesOutTooHighBySvc" && {{status}} == "firing" && severity == config.severity_threshold`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		env := conditionEvaluator.BuildEnvironment(event, config)
		_, err := conditionEvaluator.EvaluateConditionWithEnv(removeCurlyBraces(envExpression), env)
		if err != nil {
			b.Fatalf("Env-based evaluation error: %v\n", err)
			return
		}
	}

}

//func BenchmarkCELEvaluator_EvaluateCondition(b *testing.B) {
//	celConditionEvaluator, err := NewCELConditionEvaluator()
//	if err != nil {
//		b.Fatalf("Failed to create CEL evaluator: %v", err)
//	}
//
//	event := &CommandEvent{
//		Id:              "event-123",
//		Source:          "alertmanager",
//		Subject:         "alert.alertmanager.firing",
//		DataContentType: "application/json",
//		Time:            time.Now(),
//		HubName:         "production",
//		Data: map[string]interface{}{
//			"alertname":    "logBytesOutTooHighBySvc",
//			"status":       "firing",
//			"service_name": "payment-service",
//			"severity":     "warning",
//		},
//		CorrelationId: "corr-456",
//		CausationId:   "cause-789",
//	}
//
//	config := map[string]interface{}{
//		"severity_threshold": "warning",
//		"rate_limit": map[string]interface{}{
//			"max_events": 100,
//		},
//	}
//
//	condition := `{{alertname}} == "logBytesOutTooHighBySvc" && {{status}} == "firing" && {{severity}} == {{config.severity_threshold}}`
//
//	compiledCondition, err := celConditionEvaluator.CompileCELCondition(condition, true)
//	if err != nil {
//		b.Fatalf("CEL condition compilation error: %v", err)
//	}
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		_, err := celConditionEvaluator.EvaluateCELCondition(compiledCondition, event, config)
//		if err != nil {
//			b.Fatalf("CEL condition evaluation error: %v", err)
//		}
//	}
//}
//
//func BenchmarkCELEvaluator_EvaluateConditionWithEnv(b *testing.B) {
//	celConditionEvaluator, err := NewCELConditionEvaluator()
//	if err != nil {
//		b.Fatalf("Failed to create CEL evaluator: %v", err)
//	}
//
//	event := &CommandEvent{
//		Id:              "event-123",
//		Source:          "alertmanager",
//		Subject:         "alert.alertmanager.firing",
//		DataContentType: "application/json",
//		Time:            time.Now(),
//		HubName:         "production",
//		Data: map[string]interface{}{
//			"alertname":    "logBytesOutTooHighBySvc",
//			"status":       "firing",
//			"service_name": "payment-service",
//			"severity":     "warning",
//		},
//		CorrelationId: "corr-456",
//		CausationId:   "cause-789",
//	}
//
//	config := map[string]interface{}{
//		"severity_threshold": "warning",
//		"rate_limit": map[string]interface{}{
//			"max_events": 100,
//		},
//	}
//
//	envExpression := `alertname == "logBytesOutTooHighBySvc" && status == "firing" && severity == config.severity_threshold`
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		env := celConditionEvaluator.BuildEnvironment(event, config)
//		_, err := celConditionEvaluator.EvaluateCELConditionWithEnv(envExpression, env)
//		if err != nil {
//			b.Fatalf("CEL env-based evaluation error: %v", err)
//		}
//	}
//}

func BenchmarkGovaluateEvaluator_EvaluateCondition(b *testing.B) {
	govaluateConditionEvaluator := NewGovaluateConditionEvaluator()

	event := &CommandEvent{
		Id:              "event-123",
		Source:          "alertmanager",
		Subject:         "alert.alertmanager.firing",
		DataContentType: "application/json",
		Time:            time.Now(),
		HubName:         "production",
		Data: map[string]interface{}{
			"alertname":    "logBytesOutTooHighBySvc",
			"status":       "firing",
			"service_name": "payment-service",
			"severity":     "warning",
		},
		CorrelationId: "corr-456",
		CausationId:   "cause-789",
	}

	config := map[string]interface{}{
		"severity_threshold": "warning",
		"rate_limit": map[string]interface{}{
			"max_events": 100,
		},
	}

	condition := `{{alertname}} == "logBytesOutTooHighBySvc" && {{status}} == "firing" && {{severity}} == {{config.severity_threshold}}`

	compiledCondition, err := govaluateConditionEvaluator.CompileGovaluateCondition(condition, true)
	if err != nil {
		b.Fatalf("Govaluate condition compilation error: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := govaluateConditionEvaluator.EvaluateGovaluateCondition(compiledCondition, event, config)
		if err != nil {
			b.Fatalf("Govaluate condition evaluation error: %v", err)
		}
	}
}

func BenchmarkGovaluateEvaluator_EvaluateConditionWithEnv(b *testing.B) {
	govaluateConditionEvaluator := NewGovaluateConditionEvaluator()

	event := &CommandEvent{
		Id:              "event-123",
		Source:          "alertmanager",
		Subject:         "alert.alertmanager.firing",
		DataContentType: "application/json",
		Time:            time.Now(),
		HubName:         "production",
		Data: map[string]interface{}{
			"alertname":    "logBytesOutTooHighBySvc",
			"status":       "firing",
			"service_name": "payment-service",
			"severity":     "warning",
		},
		CorrelationId: "corr-456",
		CausationId:   "cause-789",
	}

	config := map[string]interface{}{
		"severity_threshold": "warning",
		"rate_limit": map[string]interface{}{
			"max_events": 100,
		},
	}

	envExpression := `{{alertname}} == "logBytesOutTooHighBySvc" && {{status}} == "firing" && severity == config.severity_threshold`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env := govaluateConditionEvaluator.BuildEnvironment(event, config)
		_, err := govaluateConditionEvaluator.EvaluateGovaluateConditionWithEnv(removeCurlyBraces(envExpression), env)
		if err != nil {
			b.Fatalf("Govaluate env-based evaluation error: %v", err)
		}
	}
}
