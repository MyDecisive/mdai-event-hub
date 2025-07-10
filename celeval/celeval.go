package celeval

import (
	"bytes"
	"fmt"
	"github.com/expr-lang/expr/vm"
	"strings"
	"text/template"
	"time"

	"github.com/expr-lang/expr"
)

// CommandEvent represents the event structure transmitted via NATS
type CommandEvent struct {
	Id              string                 `json:"id"`
	Source          string                 `json:"source"`
	Subject         string                 `json:"subject"`
	DataContentType string                 `json:"datacontenttype"`
	Time            time.Time              `json:"time"`
	HubName         string                 `json:"hubname"`
	Data            map[string]interface{} `json:"data"`
	CorrelationId   string                 `json:"correlationid,omitempty"`
	CausationId     string                 `json:"causationid,omitempty"`
}

// EvaluationContext holds all data available during evaluation
type EvaluationContext struct {
	Event  *CommandEvent          `json:"event"`
	Config map[string]interface{} `json:"config"`
}

// CompiledTemplate holds the pre-compiled template for interpolation
type CompiledTemplate struct {
	template *template.Template
}

// CompiledExpression holds the pre-compiled expression for evaluation
type CompiledExpression struct {
	program *vm.Program
}

// Interpolator handles template-based field interpolation
type Interpolator struct {
	topLevelFields map[string]bool
}

// NewInterpolator creates a new interpolator instance
func NewInterpolator() *Interpolator {
	return &Interpolator{
		topLevelFields: map[string]bool{
			"Id":              true,
			"Source":          true,
			"Subject":         true,
			"DataContentType": true,
			"Time":            true,
			"HubName":         true,
			"CorrelationId":   true,
			"CausationId":     true,
		},
	}
}

// CompileTemplate pre-compiles a template at rule definition time
func (i *Interpolator) CompileTemplate(templateString string) (*CompiledTemplate, error) {
	// Transform interpolation syntax using custom scanner
	transformed := i.transformInterpolation(templateString)

	// Compile the template
	tmpl, err := template.New("template").Parse(transformed)
	if err != nil {
		return nil, fmt.Errorf("template compilation failed: %w", err)
	}

	return &CompiledTemplate{template: tmpl}, nil
}

// Interpolate executes template interpolation against context
func (i *Interpolator) Interpolate(compiled *CompiledTemplate, event *CommandEvent, config map[string]interface{}) (string, error) {
	ctx := &EvaluationContext{
		Event:  event,
		Config: config,
	}

	var buf bytes.Buffer
	if err := compiled.template.Execute(&buf, ctx); err != nil {
		return "", fmt.Errorf("template execution failed: %w", err)
	}

	return buf.String(), nil
}

// transformInterpolation converts {{field}} and {{config.field}} syntax to template paths
func (i *Interpolator) transformInterpolation(condition string) string {
	var result strings.Builder
	idx := 0

	for idx < len(condition) {
		// Look for opening {{
		if idx < len(condition)-1 && condition[idx:idx+2] == "{{" {
			// Find closing }}
			end := strings.Index(condition[idx:], "}}")
			if end != -1 {
				// Extract field path and trim whitespace
				fieldPath := strings.TrimSpace(condition[idx+2 : idx+end])

				// Transform the field path
				transformed := i.transformFieldPath(fieldPath)
				result.WriteString("{{" + transformed + "}}")

				idx += end + 2
				continue
			}
		}

		// Copy character as-is
		result.WriteByte(condition[idx])
		idx++
	}

	return result.String()
}

// transformFieldPath converts field paths to template syntax
func (i *Interpolator) transformFieldPath(fieldPath string) string {
	parts := strings.Split(fieldPath, ".")

	if len(parts) == 0 {
		return fieldPath
	}

	// Handle config paths: config.field.subfield -> .Config.field.subfield
	if parts[0] == "config" {
		if len(parts) == 1 {
			return ".Config"
		}
		// Build path: .Config.field.subfield...
		return ".Config." + strings.Join(parts[1:], ".")
	}

	// Handle single field names (event data or top-level fields)
	if len(parts) == 1 {
		field := parts[0]
		if i.isTopLevelField(field) {
			return ".Event." + i.capitalizeField(field)
		}
		return ".Event.Data." + field
	}

	// Handle nested event data paths: field.subfield -> .Event.Data.field.subfield
	return ".Event.Data." + strings.Join(parts, ".")
}

// isTopLevelField checks if a field name corresponds to a top-level CommandEvent field
func (i *Interpolator) isTopLevelField(field string) bool {
	// Check both lowercase and capitalized versions
	return i.topLevelFields[field] || i.topLevelFields[i.capitalizeField(field)]
}

// capitalizeField converts field names to match Go struct field names
func (i *Interpolator) capitalizeField(field string) string {
	if len(field) == 0 {
		return field
	}

	// Handle common field name mappings
	switch strings.ToLower(field) {
	case "datacontenttype":
		return "DataContentType"
	case "hubname":
		return "HubName"
	case "correlationid":
		return "CorrelationId"
	case "causationid":
		return "CausationId"
	default:
		// Capitalize first letter for other fields
		return strings.ToUpper(field[:1]) + field[1:]
	}
}

// Evaluator handles expression evaluation using expr
type Evaluator struct{}

// NewEvaluator creates a new evaluator instance
func NewEvaluator() *Evaluator {
	return &Evaluator{}
}

// CompileExpression pre-compiles an expression at rule definition time
func (e *Evaluator) CompileExpression(expression string) (*CompiledExpression, error) {
	program, err := expr.Compile(expression)
	if err != nil {
		return nil, fmt.Errorf("expression compilation failed: %w", err)
	}

	return &CompiledExpression{program: program}, nil
}

// EvaluateExpression evaluates a pre-compiled expression with optional environment
func (e *Evaluator) EvaluateExpression(compiled *CompiledExpression, env map[string]interface{}) (interface{}, error) {
	result, err := expr.Run(compiled.program, env)
	if err != nil {
		return nil, fmt.Errorf("expression evaluation failed: %w", err)
	}

	return result, nil
}

// EvaluateExpressionString evaluates an expression string directly with environment
func (e *Evaluator) EvaluateExpressionString(expression string, env map[string]interface{}) (interface{}, error) {
	result, err := expr.Eval(expression, env)
	if err != nil {
		return nil, fmt.Errorf("expression evaluation failed: %w", err)
	}

	return result, nil
}

// ConditionEvaluator combines interpolation and evaluation for backward compatibility
type ConditionEvaluator struct {
	interpolator *Interpolator
	evaluator    *Evaluator
}

// NewConditionEvaluator creates a new combined evaluator instance
func NewConditionEvaluator() *ConditionEvaluator {
	return &ConditionEvaluator{
		interpolator: NewInterpolator(),
		evaluator:    NewEvaluator(),
	}
}

// CompiledCondition holds both compiled template and expression for combined evaluation
type CompiledCondition struct {
	template *CompiledTemplate
	expr     *CompiledExpression
	isExpr   bool
}

// CompileCondition pre-compiles a condition (interpolation + optional expression)
func (ce *ConditionEvaluator) CompileCondition(condition string, isExpr bool) (*CompiledCondition, error) {
	// Always compile the template for interpolation
	compiledTemplate, err := ce.interpolator.CompileTemplate(condition)
	if err != nil {
		return nil, err
	}

	compiled := &CompiledCondition{
		template: compiledTemplate,
		isExpr:   isExpr,
	}

	// If it's an expression, we can't pre-compile it since we don't know the interpolated result
	// This is the trade-off with the interpolate-then-evaluate approach
	return compiled, nil
}

// EvaluateCondition evaluates using interpolation then expression evaluation
func (ce *ConditionEvaluator) EvaluateCondition(compiled *CompiledCondition, event *CommandEvent, config map[string]interface{}) (interface{}, error) {
	// Step 1: Interpolate
	interpolated, err := ce.interpolator.Interpolate(compiled.template, event, config)
	if err != nil {
		return nil, err
	}

	// Step 2: If it's just interpolation, return the string result
	if !compiled.isExpr {
		return interpolated, nil
	}

	// Step 3: Evaluate the interpolated expression
	result, err := ce.evaluator.EvaluateExpressionString(interpolated, nil)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// EvaluateConditionWithEnv evaluates using a pre-built environment (for benchmarking)
func (ce *ConditionEvaluator) EvaluateConditionWithEnv(expression string, env map[string]interface{}) (interface{}, error) {
	return ce.evaluator.EvaluateExpressionString(expression, env)
}

// BuildEnvironment creates an environment map from event and config for direct expression evaluation
func (ce *ConditionEvaluator) BuildEnvironment(event *CommandEvent, config map[string]interface{}) map[string]interface{} {
	env := make(map[string]interface{})

	// Add event fields
	if event != nil {
		env["id"] = event.Id
		env["source"] = event.Source
		env["subject"] = event.Subject
		env["datacontenttype"] = event.DataContentType
		env["time"] = event.Time
		env["hubname"] = event.HubName
		env["correlationid"] = event.CorrelationId
		env["causationid"] = event.CausationId

		// Add event data fields
		for k, v := range event.Data {
			env[k] = v
		}
	}

	// Add config with "config" prefix
	if config != nil {
		configEnv := make(map[string]interface{})
		for k, v := range config {
			configEnv[k] = v
		}
		env["config"] = configEnv
	}

	return env
}

//// Example usage and benchmarking setup
//func main() {
//	// Create components
//	interpolator := NewInterpolator()
//	evaluator := NewEvaluator()
//	conditionEvaluator := NewConditionEvaluator()
//
//	// Example data
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
//	// Example 1: Test interpolation only
//	fmt.Println("=== Interpolation Only ===")
//	templateStr := "Alert {{alertname}} is {{status}} for service {{service_name}}"
//	compiledTemplate, err := interpolator.CompileTemplate(templateStr)
//	if err != nil {
//		fmt.Printf("Template compilation error: %v\n", err)
//		return
//	}
//
//	interpolated, err := interpolator.Interpolate(compiledTemplate, event, config)
//	if err != nil {
//		fmt.Printf("Interpolation error: %v\n", err)
//		return
//	}
//	fmt.Printf("Template: %s\n", templateStr)
//	fmt.Printf("Result: %s\n", interpolated)
//
//	// Example 2: Test expression evaluation only
//	fmt.Println("\n=== Expression Only ===")
//	env := conditionEvaluator.BuildEnvironment(event, config)
//	expression := `alertname == "logBytesOutTooHighBySvc" && status == "firing"`
//
//	result, err := evaluator.EvaluateExpressionString(expression, env)
//	if err != nil {
//		fmt.Printf("Expression error: %v\n", err)
//		return
//	}
//	fmt.Printf("Expression: %s\n", expression)
//	fmt.Printf("Result: %v\n", result)
//
//	// Example 3: Test combined approach
//	fmt.Println("\n=== Combined Approach ===")
//	condition := `{{alertname}} == "logBytesOutTooHighBySvc" && {{status}} == "firing" && {{severity}} == {{config.severity_threshold}}`
//	compiledCondition, err := conditionEvaluator.CompileCondition(condition, true)
//	if err != nil {
//		fmt.Printf("Condition compilation error: %v\n", err)
//		return
//	}
//
//	conditionResult, err := conditionEvaluator.EvaluateCondition(compiledCondition, event, config)
//	if err != nil {
//		fmt.Printf("Condition evaluation error: %v\n", err)
//		return
//	}
//	fmt.Printf("Condition: %s\n", condition)
//	fmt.Printf("Result: %v\n", conditionResult)
//
//	// Example 4: Test env-based approach for comparison
//	fmt.Println("\n=== Environment-Based Approach ===")
//	envExpression := `alertname == "logBytesOutTooHighBySvc" && status == "firing" && severity == config.severity_threshold`
//	envResult, err := conditionEvaluator.EvaluateConditionWithEnv(envExpression, env)
//	if err != nil {
//		fmt.Printf("Env-based evaluation error: %v\n", err)
//		return
//	}
//	fmt.Printf("Expression: %s\n", envExpression)
//	fmt.Printf("Result: %v\n", envResult)
//}
