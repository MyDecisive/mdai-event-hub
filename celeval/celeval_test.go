package celeval

import (
	"fmt"
	"testing"
	"time"
)

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
		fmt.Printf("Condition compilation error: %v\n", err)
		return
	}

	for i := 0; i < b.N; i++ {
		_, err := conditionEvaluator.EvaluateCondition(compiledCondition, event, config)
		if err != nil {
			fmt.Printf("Condition evaluation error: %v\n", err)
			return
		}
	}
}

func BenchmarkEvaluator_EvaluateConditionWithEnv(b *testing.B) {
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

	envExpression := `alertname == "logBytesOutTooHighBySvc" && status == "firing" && severity == config.severity_threshold`

	for i := 0; i < b.N; i++ {
		env := conditionEvaluator.BuildEnvironment(event, config)
		_, err := conditionEvaluator.EvaluateConditionWithEnv(envExpression, env)
		if err != nil {
			fmt.Printf("Env-based evaluation error: %v\n", err)
			return
		}
	}

}

//BenchmarkConditionEvaluator_EvaluateCondition-11          289825              4111 ns/op            7163 B/op         89 allocs/op
//BenchmarkConditionEvaluator_EvaluateCondition-11          286476              4122 ns/op            7163 B/op         89 allocs/op
//BenchmarkConditionEvaluator_EvaluateCondition-11          290593              4124 ns/op            7163 B/op         89 allocs/op
//BenchmarkConditionEvaluator_EvaluateCondition-11          289098              4147 ns/op            7163 B/op         89 allocs/op
//BenchmarkConditionEvaluator_EvaluateCondition-11          284684              4141 ns/op            7163 B/op         89 allocs/op
//BenchmarkConditionEvaluator_EvaluateCondition-11          287929              4132 ns/op            7163 B/op         89 allocs/op
//BenchmarkConditionEvaluator_EvaluateCondition-11          285844              4134 ns/op            7163 B/op         89 allocs/op
//BenchmarkConditionEvaluator_EvaluateCondition-11          285352              4135 ns/op            7163 B/op         89 allocs/op
//BenchmarkConditionEvaluator_EvaluateCondition-11          290018              4126 ns/op            7163 B/op         89 allocs/op
//BenchmarkConditionEvaluator_EvaluateCondition-11          289639              4126 ns/op            7163 B/op         89 allocs/op
//BenchmarkEvaluator_EvaluateConditionWithEnv-11            281259              4212 ns/op            8608 B/op        102 allocs/op
//BenchmarkEvaluator_EvaluateConditionWithEnv-11            279420              4240 ns/op            8608 B/op        102 allocs/op
//BenchmarkEvaluator_EvaluateConditionWithEnv-11            279788              4318 ns/op            8608 B/op        102 allocs/op
//BenchmarkEvaluator_EvaluateConditionWithEnv-11            281246              4514 ns/op            8608 B/op        102 allocs/op
//BenchmarkEvaluator_EvaluateConditionWithEnv-11            282474              4216 ns/op            8608 B/op        102 allocs/op
//BenchmarkEvaluator_EvaluateConditionWithEnv-11            278247              4278 ns/op            8608 B/op        102 allocs/op
//BenchmarkEvaluator_EvaluateConditionWithEnv-11            280512              4232 ns/op            8608 B/op        102 allocs/op
//BenchmarkEvaluator_EvaluateConditionWithEnv-11            279770              4231 ns/op            8608 B/op        102 allocs/op
//BenchmarkEvaluator_EvaluateConditionWithEnv-11            281528              4240 ns/op            8608 B/op        102 allocs/op
//BenchmarkEvaluator_EvaluateConditionWithEnv-11            279402              4239 ns/op            8608 B/op        102 allocs/op
