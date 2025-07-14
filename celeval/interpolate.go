package celeval

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/template"
)

type Interpolator struct {
	topLevelFields map[string]bool
}

type CompiledTemplate struct {
	template *template.Template
}

type EvaluationContext struct {
	Event  *CommandEvent
	Config map[string]interface{}
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
func (i *Interpolator) CompileTemplate(templateString string, applyQuoting bool) (*CompiledTemplate, error) {
	// Transform interpolation syntax using custom scanner
	transformed := i.transformInterpolation(templateString, applyQuoting)

	// Compile the template with custom functions
	tmpl, err := template.New("template").Funcs(template.FuncMap{
		"smartQuote": i.smartQuote,
	}).Parse(transformed)
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

// transformInterpolation converts {{field}} syntax to template paths with smart quoting
func (i *Interpolator) transformInterpolation(condition string, applyQuoting bool) string {
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

				// Transform the field path with smart quoting
				transformed := i.transformFieldPath(fieldPath)
				if applyQuoting {
					result.WriteString("{{smartQuote " + transformed + "}}")
				} else {
					result.WriteString("{{" + transformed + "}}")
				}

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

// smartQuote is a template function that quotes string values but leaves numbers unquoted
func (i *Interpolator) smartQuote(value interface{}) string {
	if value == nil {
		return `""`
	}

	// Convert to string first
	str := fmt.Sprintf("%v", value)

	// Check if it's a number
	if _, err := strconv.ParseFloat(str, 64); err == nil {
		return str // Don't quote numbers
	}

	// Check if it's a boolean
	if str == "true" || str == "false" {
		return str // Don't quote booleans
	}

	// Quote the string, escaping any internal quotes
	escaped := strings.ReplaceAll(str, `"`, `\"`)
	return `"` + escaped + `"`
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
