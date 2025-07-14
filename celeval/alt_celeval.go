package celeval

import (
	"fmt"
	"github.com/casbin/govaluate"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
)

// CELEvaluator handles CEL expression evaluation
type CELEvaluator struct {
	env *cel.Env
}

// CompiledCELExpression holds the pre-compiled CEL expression
type CompiledCELExpression struct {
	program cel.Program
}

// NewCELEvaluator creates a new CEL evaluator instance
func NewCELEvaluator() (*CELEvaluator, error) {
	// Create CEL environment with common types
	env, err := cel.NewEnv(
		cel.Variable("id", cel.StringType),
		cel.Variable("source", cel.StringType),
		cel.Variable("subject", cel.StringType),
		cel.Variable("datacontenttype", cel.StringType),
		cel.Variable("time", cel.TimestampType),
		cel.Variable("hubname", cel.StringType),
		cel.Variable("correlationid", cel.StringType),
		cel.Variable("causationid", cel.StringType),
		cel.Variable("alertname", cel.StringType),
		cel.Variable("status", cel.StringType),
		cel.Variable("service_name", cel.StringType),
		cel.Variable("severity", cel.StringType),
		cel.Variable("config", cel.MapType(cel.StringType, cel.DynType)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	return &CELEvaluator{env: env}, nil
}

// CompileCELExpression pre-compiles a CEL expression
func (c *CELEvaluator) CompileCELExpression(expression string) (*CompiledCELExpression, error) {
	ast, issues := c.env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("CEL compilation failed: %w", issues.Err())
	}

	program, err := c.env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("CEL program creation failed: %w", err)
	}

	return &CompiledCELExpression{program: program}, nil
}

// EvaluateCELExpression evaluates a pre-compiled CEL expression
func (c *CELEvaluator) EvaluateCELExpression(compiled *CompiledCELExpression, env map[string]interface{}) (interface{}, error) {
	// Convert map to CEL-compatible format
	celEnv := make(map[string]interface{})
	for k, v := range env {
		celEnv[k] = types.DefaultTypeAdapter.NativeToValue(v)
	}

	result, _, err := compiled.program.Eval(celEnv)
	if err != nil {
		return nil, fmt.Errorf("CEL evaluation failed: %w", err)
	}

	return result.Value(), nil
}

// EvaluateCELExpressionString evaluates a CEL expression string directly
func (c *CELEvaluator) EvaluateCELExpressionString(expression string, env map[string]interface{}) (interface{}, error) {
	compiled, err := c.CompileCELExpression(expression)
	if err != nil {
		return nil, err
	}

	return c.EvaluateCELExpression(compiled, env)
}

// GovaluateEvaluator handles Govaluate expression evaluation
type GovaluateEvaluator struct{}

// CompiledGovaluateExpression holds the pre-compiled Govaluate expression
type CompiledGovaluateExpression struct {
	expression *govaluate.EvaluableExpression
}

// NewGovaluateEvaluator creates a new Govaluate evaluator instance
func NewGovaluateEvaluator() *GovaluateEvaluator {
	return &GovaluateEvaluator{}
}

// CompileGovaluateExpression pre-compiles a Govaluate expression
func (g *GovaluateEvaluator) CompileGovaluateExpression(expression string) (*CompiledGovaluateExpression, error) {
	expr, err := govaluate.NewEvaluableExpression(expression)
	if err != nil {
		return nil, fmt.Errorf("Govaluate compilation failed: %w", err)
	}

	return &CompiledGovaluateExpression{expression: expr}, nil
}

// EvaluateGovaluateExpression evaluates a pre-compiled Govaluate expression
func (g *GovaluateEvaluator) EvaluateGovaluateExpression(compiled *CompiledGovaluateExpression, env map[string]interface{}) (interface{}, error) {
	result, err := compiled.expression.Evaluate(env)
	if err != nil {
		return nil, fmt.Errorf("Govaluate evaluation failed: %w", err)
	}

	return result, nil
}

// EvaluateGovaluateExpressionString evaluates a Govaluate expression string directly
func (g *GovaluateEvaluator) EvaluateGovaluateExpressionString(expression string, env map[string]interface{}) (interface{}, error) {
	compiled, err := g.CompileGovaluateExpression(expression)
	if err != nil {
		return nil, err
	}

	return g.EvaluateGovaluateExpression(compiled, env)
}

// CELConditionEvaluator combines interpolation and CEL evaluation
type CELConditionEvaluator struct {
	interpolator *Interpolator
	celEvaluator *CELEvaluator
}

// NewCELConditionEvaluator creates a new CEL-based condition evaluator
func NewCELConditionEvaluator() (*CELConditionEvaluator, error) {
	celEvaluator, err := NewCELEvaluator()
	if err != nil {
		return nil, err
	}

	return &CELConditionEvaluator{
		interpolator: NewInterpolator(),
		celEvaluator: celEvaluator,
	}, nil
}

// CompiledCELCondition holds both compiled template and CEL expression
type CompiledCELCondition struct {
	template *CompiledTemplate
	expr     *CompiledCELExpression
	isExpr   bool
}

// CompileCELCondition pre-compiles a condition for CEL evaluation
func (ce *CELConditionEvaluator) CompileCELCondition(condition string, isExpr bool) (*CompiledCELCondition, error) {
	// Always compile the template for interpolation
	compiledTemplate, err := ce.interpolator.CompileTemplate(condition, true)
	if err != nil {
		return nil, err
	}

	compiled := &CompiledCELCondition{
		template: compiledTemplate,
		isExpr:   isExpr,
	}

	return compiled, nil
}

// EvaluateCELCondition evaluates using interpolation then CEL evaluation
func (ce *CELConditionEvaluator) EvaluateCELCondition(compiled *CompiledCELCondition, event *CommandEvent, config map[string]interface{}) (interface{}, error) {
	// Step 1: Interpolate
	interpolated, err := ce.interpolator.Interpolate(compiled.template, event, config)
	if err != nil {
		return nil, err
	}

	// Step 2: If it's just interpolation, return the string result
	if !compiled.isExpr {
		return interpolated, nil
	}

	// Step 3: Evaluate the interpolated expression with CEL
	env := ce.BuildEnvironment(event, config)
	result, err := ce.celEvaluator.EvaluateCELExpressionString(interpolated, env)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// EvaluateCELConditionWithEnv evaluates using a pre-built environment
func (ce *CELConditionEvaluator) EvaluateCELConditionWithEnv(expression string, env map[string]interface{}) (interface{}, error) {
	return ce.celEvaluator.EvaluateCELExpressionString(expression, env)
}

// BuildEnvironment creates an environment map from event and config for CEL evaluation
func (ce *CELConditionEvaluator) BuildEnvironment(event *CommandEvent, config map[string]interface{}) map[string]interface{} {
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

// GovaluateConditionEvaluator combines interpolation and Govaluate evaluation
type GovaluateConditionEvaluator struct {
	interpolator       *Interpolator
	govaluateEvaluator *GovaluateEvaluator
}

// NewGovaluateConditionEvaluator creates a new Govaluate-based condition evaluator
func NewGovaluateConditionEvaluator() *GovaluateConditionEvaluator {
	return &GovaluateConditionEvaluator{
		interpolator:       NewInterpolator(),
		govaluateEvaluator: NewGovaluateEvaluator(),
	}
}

// CompiledGovaluateCondition holds both compiled template and Govaluate expression
type CompiledGovaluateCondition struct {
	template *CompiledTemplate
	expr     *CompiledGovaluateExpression
	isExpr   bool
}

// CompileGovaluateCondition pre-compiles a condition for Govaluate evaluation
func (ce *GovaluateConditionEvaluator) CompileGovaluateCondition(condition string, isExpr bool) (*CompiledGovaluateCondition, error) {
	// Always compile the template for interpolation
	compiledTemplate, err := ce.interpolator.CompileTemplate(condition, true)
	if err != nil {
		return nil, err
	}

	compiled := &CompiledGovaluateCondition{
		template: compiledTemplate,
		isExpr:   isExpr,
	}

	return compiled, nil
}

// EvaluateGovaluateCondition evaluates using interpolation then Govaluate evaluation
func (ce *GovaluateConditionEvaluator) EvaluateGovaluateCondition(compiled *CompiledGovaluateCondition, event *CommandEvent, config map[string]interface{}) (interface{}, error) {
	// Step 1: Interpolate
	interpolated, err := ce.interpolator.Interpolate(compiled.template, event, config)
	if err != nil {
		return nil, err
	}

	// Step 2: If it's just interpolation, return the string result
	if !compiled.isExpr {
		return interpolated, nil
	}

	// Step 3: Evaluate the interpolated expression with Govaluate
	env := ce.BuildEnvironment(event, config)
	result, err := ce.govaluateEvaluator.EvaluateGovaluateExpressionString(interpolated, env)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// EvaluateGovaluateConditionWithEnv evaluates using a pre-built environment
func (ce *GovaluateConditionEvaluator) EvaluateGovaluateConditionWithEnv(expression string, env map[string]interface{}) (interface{}, error) {
	return ce.govaluateEvaluator.EvaluateGovaluateExpressionString(expression, env)
}

// BuildEnvironment creates an environment map from event and config for Govaluate evaluation
func (ce *GovaluateConditionEvaluator) BuildEnvironment(event *CommandEvent, config map[string]interface{}) map[string]interface{} {
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
