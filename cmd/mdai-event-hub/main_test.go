package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateLogger(t *testing.T) {
	logger := initLogger()
	assert.NotNil(t, logger)
	logger.Info("test message") // No crash = pass
}
