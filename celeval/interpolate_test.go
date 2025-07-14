package celeval

import (
	"testing"
)

func TestInterpolator_CompileTemplate_WithQuoting(t *testing.T) {
	interpolator := NewInterpolator()

	tests := []struct {
		name           string
		template       string
		applyQuoting   bool
		expectedOutput string
		event          *CommandEvent
		config         map[string]interface{}
	}{
		{
			name:           "condition with string comparison",
			template:       `{{alertname}} == "logBytesOutTooHighBySvc" && {{status}} == "firing"`,
			applyQuoting:   true,
			expectedOutput: `"TestAlert" == "logBytesOutTooHighBySvc" && "firing" == "firing"`,
			event: &CommandEvent{
				Data: map[string]interface{}{
					"alertname": "TestAlert",
					"status":    "firing",
				},
			},
			config: map[string]interface{}{},
		},
		{
			name:           "condition with numeric comparison",
			template:       `{{severity}} > 5 && {{count}} == 10`,
			applyQuoting:   true,
			expectedOutput: `7 > 5 && 10 == 10`,
			event: &CommandEvent{
				Data: map[string]interface{}{
					"severity": 7,
					"count":    10,
				},
			},
			config: map[string]interface{}{},
		},
		{
			name:           "condition with boolean comparison",
			template:       `{{enabled}} == true && {{active}} == false`,
			applyQuoting:   true,
			expectedOutput: `true == true && false == false`,
			event: &CommandEvent{
				Data: map[string]interface{}{
					"enabled": true,
					"active":  false,
				},
			},
			config: map[string]interface{}{},
		},
		{
			name:           "value context - no quoting",
			template:       `{{service_name}}`,
			applyQuoting:   false,
			expectedOutput: `my-service`,
			event: &CommandEvent{
				Data: map[string]interface{}{
					"service_name": "my-service",
				},
			},
			config: map[string]interface{}{},
		},
		{
			name:           "value context with number - no quoting",
			template:       `{{port}}`,
			applyQuoting:   false,
			expectedOutput: `8080`,
			event: &CommandEvent{
				Data: map[string]interface{}{
					"port": 8080,
				},
			},
			config: map[string]interface{}{},
		},
		{
			name:           "value context with boolean - no quoting",
			template:       `{{enabled}}`,
			applyQuoting:   false,
			expectedOutput: `true`,
			event: &CommandEvent{
				Data: map[string]interface{}{
					"enabled": true,
				},
			},
			config: map[string]interface{}{},
		},
		{
			name:           "top-level field in condition",
			template:       `{{subject}} == "alert.critical"`,
			applyQuoting:   true,
			expectedOutput: `"alert.high.cpu.alertmanager.prod" == "alert.critical"`,
			event: &CommandEvent{
				Subject: "alert.high.cpu.alertmanager.prod",
			},
			config: map[string]interface{}{},
		},
		{
			name:           "top-level field in value context",
			template:       `{{subject}}`,
			applyQuoting:   false,
			expectedOutput: `alert.high.cpu.alertmanager.prod`,
			event: &CommandEvent{
				Subject: "alert.high.cpu.alertmanager.prod",
			},
			config: map[string]interface{}{},
		},
		{
			name:           "config field in condition",
			template:       `{{config.environment}} == "production"`,
			applyQuoting:   true,
			expectedOutput: `"production" == "production"`,
			event:          &CommandEvent{},
			config: map[string]interface{}{
				"environment": "production",
			},
		},
		{
			name:           "config field in value context",
			template:       `{{config.environment}}`,
			applyQuoting:   false,
			expectedOutput: `production`,
			event:          &CommandEvent{},
			config: map[string]interface{}{
				"environment": "production",
			},
		},
		{
			name:           "nested data field in condition",
			template:       `{{labels.service}} == "web-server"`,
			applyQuoting:   true,
			expectedOutput: `"web-server" == "web-server"`,
			event: &CommandEvent{
				Data: map[string]interface{}{
					"labels": map[string]interface{}{
						"service": "web-server",
					},
				},
			},
			config: map[string]interface{}{},
		},
		{
			name:           "nested data field in value context",
			template:       `{{labels.service}}`,
			applyQuoting:   false,
			expectedOutput: `web-server`,
			event: &CommandEvent{
				Data: map[string]interface{}{
					"labels": map[string]interface{}{
						"service": "web-server",
					},
				},
			},
			config: map[string]interface{}{},
		},
		{
			name:           "mixed condition with strings and numbers",
			template:       `{{alertname}} == "HighCPU" && {{value}} > 80.5`,
			applyQuoting:   true,
			expectedOutput: `"HighCPU" == "HighCPU" && 85.7 > 80.5`,
			event: &CommandEvent{
				Data: map[string]interface{}{
					"alertname": "HighCPU",
					"value":     85.7,
				},
			},
			config: map[string]interface{}{},
		},
		{
			name:           "string with escaped quotes in condition",
			template:       `{{message}} == "Error: \"Connection failed\""`,
			applyQuoting:   true,
			expectedOutput: `"Error: \"Connection failed\"" == "Error: \"Connection failed\""`,
			event: &CommandEvent{
				Data: map[string]interface{}{
					"message": `Error: "Connection failed"`,
				},
			},
			config: map[string]interface{}{},
		},
		{
			name:           "string with escaped quotes in value context",
			template:       `{{message}}`,
			applyQuoting:   false,
			expectedOutput: `Error: "Connection failed"`,
			event: &CommandEvent{
				Data: map[string]interface{}{
					"message": `Error: "Connection failed"`,
				},
			},
			config: map[string]interface{}{},
		},
		{
			name:           "empty string in condition",
			template:       `{{description}} == ""`,
			applyQuoting:   true,
			expectedOutput: `"" == ""`,
			event: &CommandEvent{
				Data: map[string]interface{}{
					"description": "",
				},
			},
			config: map[string]interface{}{},
		},
		{
			name:           "nil value in condition",
			template:       `{{missing_field}} == ""`,
			applyQuoting:   true,
			expectedOutput: `"" == ""`,
			event: &CommandEvent{
				Data: map[string]interface{}{},
			},
			config: map[string]interface{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compiled, err := interpolator.CompileTemplate(tt.template, tt.applyQuoting)
			if err != nil {
				t.Fatalf("CompileTemplate failed: %v", err)
			}

			result, err := interpolator.Interpolate(compiled, tt.event, tt.config)
			if err != nil {
				t.Fatalf("Interpolate failed: %v", err)
			}

			if result != tt.expectedOutput {
				t.Errorf("Expected: %q, Got: %q", tt.expectedOutput, result)
			}
		})
	}
}

func TestInterpolator_TransformFieldPath(t *testing.T) {
	interpolator := NewInterpolator()

	tests := []struct {
		name         string
		fieldPath    string
		expectedPath string
	}{
		{
			name:         "simple event data field",
			fieldPath:    "alertname",
			expectedPath: ".Event.Data.alertname",
		},
		{
			name:         "top-level field",
			fieldPath:    "subject",
			expectedPath: ".Event.Subject",
		},
		{
			name:         "config field",
			fieldPath:    "config.environment",
			expectedPath: ".Config.environment",
		},
		{
			name:         "nested event data field",
			fieldPath:    "labels.service",
			expectedPath: ".Event.Data.labels.service",
		},
		{
			name:         "config root",
			fieldPath:    "config",
			expectedPath: ".Config",
		},
		{
			name:         "nested config field",
			fieldPath:    "config.db.host",
			expectedPath: ".Config.db.host",
		},
		{
			name:         "top-level capitalized field",
			fieldPath:    "datacontenttype",
			expectedPath: ".Event.DataContentType",
		},
		{
			name:         "correlation id field",
			fieldPath:    "correlationid",
			expectedPath: ".Event.CorrelationId",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := interpolator.transformFieldPath(tt.fieldPath)
			if result != tt.expectedPath {
				t.Errorf("Expected: %q, Got: %q", tt.expectedPath, result)
			}
		})
	}
}

func TestInterpolator_SmartQuote(t *testing.T) {
	interpolator := NewInterpolator()

	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{
			name:     "string value",
			input:    "hello world",
			expected: `"hello world"`,
		},
		{
			name:     "integer value",
			input:    42,
			expected: `42`,
		},
		{
			name:     "float value",
			input:    3.14,
			expected: `3.14`,
		},
		{
			name:     "boolean true",
			input:    true,
			expected: `true`,
		},
		{
			name:     "boolean false",
			input:    false,
			expected: `false`,
		},
		{
			name:     "nil value",
			input:    nil,
			expected: `""`,
		},
		{
			name:     "string with quotes",
			input:    `Error: "Connection failed"`,
			expected: `"Error: \"Connection failed\""`,
		},
		{
			name:     "empty string",
			input:    "",
			expected: `""`,
		},
		{
			name:     "string that looks like number",
			input:    "123",
			expected: `123`,
		},
		{
			name:     "string that looks like boolean",
			input:    "true",
			expected: `true`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := interpolator.smartQuote(tt.input)
			if result != tt.expected {
				t.Errorf("Expected: %q, Got: %q", tt.expected, result)
			}
		})
	}
}

func TestInterpolator_CapitalizeField(t *testing.T) {
	interpolator := NewInterpolator()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "datacontenttype",
			input:    "datacontenttype",
			expected: "DataContentType",
		},
		{
			name:     "hubname",
			input:    "hubname",
			expected: "HubName",
		},
		{
			name:     "correlationid",
			input:    "correlationid",
			expected: "CorrelationId",
		},
		{
			name:     "causationid",
			input:    "causationid",
			expected: "CausationId",
		},
		{
			name:     "regular field",
			input:    "subject",
			expected: "Subject",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "single character",
			input:    "a",
			expected: "A",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := interpolator.capitalizeField(tt.input)
			if result != tt.expected {
				t.Errorf("Expected: %q, Got: %q", tt.expected, result)
			}
		})
	}
}

func TestInterpolator_IsTopLevelField(t *testing.T) {
	interpolator := NewInterpolator()

	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "lowercase id",
			input:    "id",
			expected: true,
		},
		{
			name:     "capitalized Id",
			input:    "Id",
			expected: true,
		},
		{
			name:     "lowercase subject",
			input:    "subject",
			expected: true,
		},
		{
			name:     "capitalized Subject",
			input:    "Subject",
			expected: true,
		},
		{
			name:     "not a top-level field",
			input:    "alertname",
			expected: false,
		},
		{
			name:     "datacontenttype lowercase",
			input:    "datacontenttype",
			expected: true,
		},
		{
			name:     "DataContentType capitalized",
			input:    "DataContentType",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := interpolator.isTopLevelField(tt.input)
			if result != tt.expected {
				t.Errorf("Expected: %v, Got: %v", tt.expected, result)
			}
		})
	}
}
