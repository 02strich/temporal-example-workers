package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
)

const (
	hostPort      = "127.0.0.1:7233"
	namespace     = "default"
	taskQueueName = "worker-versioning-sample"
)

func main() {
	logger := MakeLogger()
	defer func() { _ = logger.Sync() }()

	c, err := client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: namespace,
		Logger:    newZapSDKLogger(logger),
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
				DeploymentName: "test",
				BuildID:        "v1",
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
	time.AfterFunc(30*time.Second, func() { ret <- "timed out" })

	logger.Info("subprocess worker starting",
		zap.String("hostPort", hostPort),
		zap.String("namespace", namespace),
		zap.String("taskQueue", taskQueueName),
	)
	if err := w.Run(ret); err != nil && !errors.Is(err, context.Canceled) {
		logger.Fatal("worker stopped with error", zap.Error(err))
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
