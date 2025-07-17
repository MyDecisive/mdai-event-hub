package valkey

import (
	"context"
	"time"

	"github.com/decisiveai/mdai-event-hub/internal/common"
	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
)

const (
	valkeyEndpointEnvVarKey = "VALKEY_ENDPOINT"
	valkeyPasswordEnvVarKey = "VALKEY_PASSWORD"
)

func Init(ctx context.Context, logger *zap.Logger) (valkey.Client, error) {
	valKeyEndpoint := common.GetEnvVariableWithDefault(valkeyEndpointEnvVarKey, "")
	valkeyPassword := common.GetEnvVariableWithDefault(valkeyPasswordEnvVarKey, "")

	logger.Info("Initializing valkey client with endpoint " + valKeyEndpoint)

	initializer := func() (valkey.Client, error) {
		return valkey.NewClient(valkey.ClientOption{
			InitAddress: []string{valKeyEndpoint},
			Password:    valkeyPassword,
		})
	}

	return common.RetryInitializer(
		ctx,
		logger,
		"valkey client",
		initializer,
		3*time.Minute,
		5*time.Second,
	)
}
