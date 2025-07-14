package celeval

import (
	"fmt"
	"github.com/expr-lang/expr/vm"
	"time"

	"github.com/expr-lang/expr"
)

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

type CompiledExpression struct {
	program *vm.Program
}

type Evaluator struct{}

func NewEvaluator() *Evaluator {
	return &Evaluator{}
}

func (e *Evaluator) CompileExpression(expression string) (*CompiledExpression, error) {
	program, err := expr.Compile(expression)
	if err != nil {
		return nil, fmt.Errorf("expression compilation failed: %w", err)
	}

	return &CompiledExpression{program: program}, nil
}

func (e *Evaluator) EvaluateExpression(compiled *CompiledExpression, env map[string]interface{}) (interface{}, error) {
	result, err := expr.Run(compiled.program, env)
	if err != nil {
		return nil, fmt.Errorf("expression evaluation failed: %w", err)
	}

	return result, nil
}

func (e *Evaluator) EvaluateExpressionString(expression string, env map[string]interface{}) (interface{}, error) {
	result, err := expr.Eval(expression, env)
	if err != nil {
		return nil, fmt.Errorf("expression evaluation failed: %w", err)
	}

	return result, nil
}

type ConditionEvaluator struct {
	interpolator *Interpolator
	evaluator    *Evaluator
}

func NewConditionEvaluator() *ConditionEvaluator {
	return &ConditionEvaluator{
		interpolator: NewInterpolator(),
		evaluator:    NewEvaluator(),
	}
}

type CompiledCondition struct {
	template *CompiledTemplate
	expr     *CompiledExpression
	isExpr   bool
}

func (ce *ConditionEvaluator) CompileCondition(condition string, isExpr bool) (*CompiledCondition, error) {
	compiledTemplate, err := ce.interpolator.CompileTemplate(condition, true)
	if err != nil {
		return nil, err
	}

	compiled := &CompiledCondition{
		template: compiledTemplate,
		isExpr:   isExpr,
	}

	return compiled, nil
}

func (ce *ConditionEvaluator) EvaluateCondition(compiled *CompiledCondition, event *CommandEvent, config map[string]interface{}) (interface{}, error) {
	interpolated, err := ce.interpolator.Interpolate(compiled.template, event, config)
	if err != nil {
		return nil, err
	}

	if !compiled.isExpr {
		return interpolated, nil
	}

	result, err := ce.evaluator.EvaluateExpressionString(interpolated, nil)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (ce *ConditionEvaluator) EvaluateConditionWithEnv(expression string, env map[string]interface{}) (interface{}, error) {
	return ce.evaluator.EvaluateExpressionString(expression, env)
}

func (ce *ConditionEvaluator) BuildEnvironment(event *CommandEvent, config map[string]interface{}) map[string]interface{} {
	env := make(map[string]interface{})

	if event != nil {
		env["id"] = event.Id
		env["source"] = event.Source
		env["subject"] = event.Subject
		env["datacontenttype"] = event.DataContentType
		env["time"] = event.Time
		env["hubname"] = event.HubName
		env["correlationid"] = event.CorrelationId
		env["causationid"] = event.CausationId

		for k, v := range event.Data {
			env[k] = v
		}
	}

	if config != nil {
		configEnv := make(map[string]interface{})
		for k, v := range config {
			configEnv[k] = v
		}
		env["config"] = configEnv
	}

	return env
}
