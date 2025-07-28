package valkey

import (
	"context"
	"time"

	"github.com/decisiveai/mdai-event-hub/internal/env"
	"github.com/decisiveai/mdai-event-hub/internal/utils"
	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
)

const (
	valkeyEndpointEnvVarKey = "VALKEY_ENDPOINT"
	valkeyPasswordEnvVarKey = "VALKEY_PASSWORD"
	maxElapsedTime          = 3 * time.Minute
	intialInterval          = 5 * time.Second
)

//nolint:ireturn
func Init(ctx context.Context, logger *zap.Logger) (valkey.Client, error) {
	valKeyEndpoint := env.GetVariableWithDefault(valkeyEndpointEnvVarKey, "")
	valkeyPassword := env.GetVariableWithDefault(valkeyPasswordEnvVarKey, "")

	logger.Info("Initializing valkey client with endpoint " + valKeyEndpoint)

	initializer := func() (valkey.Client, error) {
		return valkey.NewClient(valkey.ClientOption{
			InitAddress: []string{valKeyEndpoint},
			Password:    valkeyPassword,
		})
	}

	return utils.RetryInitializer(
		ctx,
		logger,
		"valkey client",
		initializer,
		maxElapsedTime,
		intialInterval,
	)
}
