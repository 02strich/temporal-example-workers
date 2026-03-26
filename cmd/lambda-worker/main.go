package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
)

const (
	defaultHostPort       = "127.0.0.1:7233"
	defaultNamespace      = "default"
	defaultTaskQueueName  = "worker-versioning-sample"
	defaultDeploymentName = "test"
	defaultBuildID        = "v1"
)

func main() {
	lambda.Start(handleRequest)
}

func handleRequest(ctx context.Context, event json.RawMessage) error {
	logger := MakeLogger()
	defer func() { _ = logger.Sync() }()

	hostPort := getEnvDefault("HOST_PORT", defaultHostPort)
	namespace := getEnvDefault("NAMESPACE", defaultNamespace)
	taskQueueName := getEnvDefault("TQ_NAME", defaultTaskQueueName)
	deploymentName := getEnvDefault("DEPLOYMENT_NAME", defaultDeploymentName)
	buildID := getEnvDefault("BUILD_ID", defaultBuildID)
	enableTLS := os.Getenv("ENABLE_TLS")
	tlsKey := os.Getenv("TLS_KEY")
	tlsCert := os.Getenv("TLS_CERT")
	apiKey := os.Getenv("API_KEY")

	var tlsConfig *tls.Config
	var credentials client.Credentials

	if enableTLS != "" {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}

		config, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return err
		}
		svc := secretsmanager.NewFromConfig(config)

		if tlsKey != "" && tlsCert != "" {
			clientCert, err := svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &tlsCert})
			if err != nil {
				return err
			}
			clientKey, err := svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &tlsKey})
			if err != nil {
				return err
			}

			cert, err := tls.X509KeyPair([]byte(*clientCert.SecretString), []byte(*clientKey.SecretString))
			if err != nil {
				return err
			}
			tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
			logger.Info("Configured client cert", zap.String("cert", fmt.Sprintf("%#v", cert)))
		}

		if apiKey != "" {
			apiKeyValue, err := svc.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &apiKey})
			if err != nil {
				return err
			}
			credentials = client.NewAPIKeyStaticCredentials(*apiKeyValue.SecretString)
		}
	}

	c, err := client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: namespace,
		Logger:    newZapSDKLogger(logger),
		ConnectionOptions: client.ConnectionOptions{
			TLS: tlsConfig,
		},
		Credentials: credentials,
	})
	if err != nil {
		return err
	}
	defer c.Close()

	wOpts := worker.Options{
		DisableEagerActivities: true,
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning: true,
			Version: worker.WorkerDeploymentVersion{
				DeploymentName: deploymentName,
				BuildID:        buildID,
			},
			DefaultVersioningBehavior: workflow.VersioningBehaviorPinned,
		},

		MaxConcurrentActivityExecutionSize:     2,
		MaxConcurrentWorkflowTaskExecutionSize: 2,
		MaxConcurrentNexusTaskExecutionSize:    2,
	}

	w := worker.New(c, taskQueueName, wOpts)
	w.RegisterDynamicWorkflow(noopWorkflow, workflow.DynamicRegisterOptions{})
	w.RegisterDynamicActivity(noopActivity, activity.DynamicRegisterOptions{})

	// end the worker on SIGTERM or after 1 minute
	osSignalChannel := make(chan os.Signal, 1)
	signal.Notify(osSignalChannel, os.Interrupt, syscall.SIGTERM)
	ret := make(chan any, 1)
	go func() {
		s := <-osSignalChannel
		ret <- s
		close(ret)
	}()
	time.AfterFunc(1*time.Minute, func() { ret <- "timed out" })

	logger.Info("subprocess worker starting",
		zap.String("hostPort", hostPort),
		zap.String("namespace", namespace),
		zap.String("taskQueue", taskQueueName),
	)
	if err := w.Run(ret); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	return nil
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
