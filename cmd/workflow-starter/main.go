package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/client"
	"go.uber.org/zap"
)

func main() {
	host := flag.String("host", "127.0.0.1:7233", "Temporal frontend gRPC address")
	namespace := flag.String("namespace", "default", "Temporal namespace")
	taskQueue := flag.String("task-queue", "worker-versioning-sample", "Task queue to submit workflows to")
	workflowType := flag.String("workflow-type", "noop", "Workflow type name to start")
	count := flag.Int("count", 1, "Number of workflow executions to start")
	jitter := flag.Duration("jitter", 0, "Max random delay before each workflow start (e.g. 5s, 500ms); 0 = no jitter")
	flag.Parse()

	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = logger.Sync() }()

	c, err := client.Dial(client.Options{
		HostPort:  *host,
		Namespace: *namespace,
	})
	if err != nil {
		logger.Fatal("failed to create Temporal client", zap.Error(err))
	}
	defer c.Close()

	var wg sync.WaitGroup
	var errCount atomic.Int64

	for i := range *count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if *jitter > 0 {
				time.Sleep(time.Duration(rand.Int63n(int64(*jitter))))
			}
			ctx := context.Background()
			_, startErr := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
				ID:        fmt.Sprintf("workflow-starter-%s-%d", *workflowType, i),
				TaskQueue: *taskQueue,
			}, *workflowType)
			if startErr != nil {
				logger.Error("failed to start workflow", zap.Int("index", i), zap.Error(startErr))
				errCount.Add(1)
			}
		}()
	}

	wg.Wait()

	errors := errCount.Load()
	started := int64(*count) - errors
	fmt.Printf("started %d workflows (%d errors)\n", started, errors)

	if errors > 0 {
		os.Exit(1)
	}
}
