package main

import (
	"context"
	"crypto/tls"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/aws/lambdaworker"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	defaultTaskQueueName  = "worker-versioning-sample"
	defaultDeploymentName = "test"
	defaultBuildID        = "v1"
)

func main() {
	deploymentName := getEnvDefault("DEPLOYMENT_NAME", defaultDeploymentName)
	buildID := getEnvDefault("BUILD_ID", defaultBuildID)

	lambdaworker.RunWorker(
		worker.WorkerDeploymentVersion{
			DeploymentName: deploymentName,
			BuildID:        buildID,
		},
		func(ctx *lambdaworker.Options) error {
			taskQueueName := getEnvDefault("TQ_NAME", defaultTaskQueueName)
			ctx.TaskQueue = taskQueueName

			enableTLS := os.Getenv("ENABLE_TLS")
			tlsKey := os.Getenv("TLS_KEY")
			tlsCert := os.Getenv("TLS_CERT")
			apiKey := os.Getenv("API_KEY")

			localCtx := context.Background()

			var tlsConfig *tls.Config
			var credentials client.Credentials

			if enableTLS != "" {
				tlsConfig = &tls.Config{InsecureSkipVerify: true}

				config, err := config.LoadDefaultConfig(localCtx)
				if err != nil {
					return err
				}
				svc := secretsmanager.NewFromConfig(config)

				if tlsKey != "" && tlsCert != "" {
					clientCert, err := svc.GetSecretValue(localCtx, &secretsmanager.GetSecretValueInput{SecretId: &tlsCert})
					if err != nil {
						return err
					}
					clientKey, err := svc.GetSecretValue(localCtx, &secretsmanager.GetSecretValueInput{SecretId: &tlsKey})
					if err != nil {
						return err
					}

					cert, err := tls.X509KeyPair([]byte(*clientCert.SecretString), []byte(*clientKey.SecretString))
					if err != nil {
						return err
					}
					tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
				}

				if apiKey != "" {
					apiKeyValue, err := svc.GetSecretValue(localCtx, &secretsmanager.GetSecretValueInput{SecretId: &apiKey})
					if err != nil {
						return err
					}
					credentials = client.NewAPIKeyStaticCredentials(*apiKeyValue.SecretString)
				}
			}
			ctx.ClientOptions.ConnectionOptions.TLS = tlsConfig
			ctx.ClientOptions.Credentials = credentials
			ctx.WorkerOptions.DeploymentOptions.DefaultVersioningBehavior = workflow.VersioningBehaviorPinned

			ctx.RegisterDynamicWorkflow(noopWorkflow, workflow.DynamicRegisterOptions{})
			ctx.RegisterDynamicActivity(noopActivity, activity.DynamicRegisterOptions{})

			return nil
		})
}

func noopWorkflow(ctx workflow.Context, args converter.EncodedValues) (string, error) {
	logger := workflow.GetLogger(ctx)
	wfInfo := workflow.GetInfo(ctx)

	logger.Info("Running workflow", "workflow_name", wfInfo.WorkflowType.Name)

	if err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 1 * time.Minute}),
		wfInfo.WorkflowType.Name,
	).Get(ctx, nil); err != nil {
		return "", err
	}

	return "success", nil
}

func noopActivity(ctx context.Context, args converter.EncodedValues) (string, error) {
	logger := activity.GetLogger(ctx)
	actInfo := activity.GetInfo(ctx)
	logger.Info("Running activity", "activity_name", actInfo.ActivityType.Name)

	time.Sleep(15 * time.Second)

	return "Success", nil
}

func getEnvDefault(env, defaultVal string) string {
	envValue := os.Getenv(env)
	if envValue == "" {
		return defaultVal
	} else {
		return envValue
	}
}
