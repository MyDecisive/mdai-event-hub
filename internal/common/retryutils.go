package common

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v5"

	"go.uber.org/zap"
)

// RetryInitializer is a generic function to handle initialization with retry logic
// T is the resource type being created
func RetryInitializer[T any](
	ctx context.Context,
	logger *zap.Logger,
	resourceName string,
	initializer func() (T, error),
	maxElapsedTime time.Duration,
	initialInterval time.Duration,
) (T, error) {
	var (
		result     T
		retryCount int
	)

	operation := func() (string, error) {
		var err error
		result, err = initializer()
		if err != nil {
			retryCount++
			return "", err
		}
		logger.Info(fmt.Sprintf("Successfully initialized %s", resourceName))
		return "", nil
	}

	notifyFunc := func(err error, duration time.Duration) {
		logger.Error(fmt.Sprintf("failed to initialize %s. retrying...", resourceName),
			zap.Int("retry_count", retryCount),
			zap.Duration("duration", duration))
	}

	exponentialBackoff := backoff.NewExponentialBackOff()
	exponentialBackoff.InitialInterval = initialInterval

	if _, err := backoff.Retry(ctx, operation,
		backoff.WithBackOff(exponentialBackoff),
		backoff.WithMaxElapsedTime(maxElapsedTime),
		backoff.WithNotify(notifyFunc)); err != nil {
		logger.Fatal(fmt.Sprintf("failed to initialize %s", resourceName), zap.Error(err))
		var zero T
		return zero, err
	}

	return result, nil
}
