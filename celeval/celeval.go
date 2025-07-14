package celeval

import (
	"fmt"
	"github.com/expr-lang/expr/vm"
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

// CompiledExpression holds the pre-compiled expression for evaluation
type CompiledExpression struct {
	program *vm.Program
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
	compiledTemplate, err := ce.interpolator.CompileTemplate(condition, true)
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
