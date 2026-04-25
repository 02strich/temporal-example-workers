package main

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"time"

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
	logger := MakeLogger()
	defer func() { _ = logger.Sync() }()

	hostPort := getEnvDefault("HOST_PORT", defaultHostPort)
	namespace := getEnvDefault("NAMESPACE", defaultNamespace)
	taskQueueName := getEnvDefault("TQ_NAME", defaultTaskQueueName)
	deploymentName := getEnvDefault("DEPLOYMENT_NAME", defaultDeploymentName)
	buildID := getEnvDefault("BUILD_ID", defaultBuildID)
	enableTLS := os.Getenv("ENABLE_TLS")

	var tlsConfig *tls.Config
	if enableTLS != "" {
		tlsConfig = &tls.Config{}
	}

	c, err := client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: namespace,
		Logger:    newZapSDKLogger(logger),
		ConnectionOptions: client.ConnectionOptions{
			TLS: tlsConfig,
		},
	})
	if err != nil {
		logger.Fatal("failed to create Temporal client", zap.Error(err))
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

	logger.Info("container worker starting",
		zap.String("hostPort", hostPort),
		zap.String("namespace", namespace),
		zap.String("taskQueue", taskQueueName),
	)
	if err := w.Run(worker.InterruptCh()); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatal("worker stopped with error", zap.Error(err))
	}
}

func getEnvDefault(env, defaultVal string) string {
	envValue := os.Getenv(env)
	if envValue == "" {
		return defaultVal
	} else {
		return envValue
	}
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
