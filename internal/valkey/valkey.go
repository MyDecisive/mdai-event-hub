package valkey

import (
	"context"
	"fmt"
	"github.com/decisiveai/mdai-event-hub/internal/common"
	"github.com/valkey-io/valkey-go"
	"go.uber.org/zap"
	"time"
)

const (
	valkeyEndpointEnvVarKey = "VALKEY_ENDPOINT"
	valkeyPasswordEnvVarKey = "VALKEY_PASSWORD"
)

func Init(ctx context.Context, logger *zap.Logger) (valkey.Client, error) {
	valKeyEndpoint := common.GetEnvVariableWithDefault(valkeyEndpointEnvVarKey, "")
	valkeyPassword := common.GetEnvVariableWithDefault(valkeyPasswordEnvVarKey, "")

	logger.Info(fmt.Sprintf("Initializing valkey client with endpoint %s", valKeyEndpoint))

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
