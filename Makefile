############################# Main targets #############################
# Rebuild binaries (used by Dockerfile).
bins: subprocess-worker workflow-starter

# Install all tools, run all possible checks and tests (long but comprehensive).
all: clean bins

# Delete all build artifacts
clean: clean-bins
########################################################################

.PHONY: bins clean

##### Variables ######

COLOR := "\e[1;36m%s\e[0m\n"
RED :=   "\e[1;31m%s\e[0m\n"

ALL_SRC         := $(shell find . -name "*.go")
ALL_SRC         += go.mod

##### Binaries #####
clean-bins:
	@printf $(COLOR) "Delete old binaries..."
	@rm -f subprocess-worker
	@rm -f workflow-starter

subprocess-worker: $(ALL_SRC)
	@printf $(COLOR) "Build subprocess-worker with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o subprocess-worker ./cmd/subprocess-worker

lambda-worker: $(ALL_SRC)
	@printf $(COLOR) "Build lambda-worker with CGO_ENABLED=$(CGO_ENABLED)..."
	CGO_ENABLED=$(CGO_ENABLED) GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -o bootstrap ./cmd/lambda-worker
	zip lambda-worker.zip bootstrap


workflow-starter: $(ALL_SRC)
	@printf $(COLOR) "Build workflow-starter with CGO_ENABLED=$(CGO_ENABLED) for $(GOOS)/$(GOARCH)..."
	CGO_ENABLED=$(CGO_ENABLED) go build $(BUILD_TAG_FLAG) -o workflow-starter ./cmd/workflow-starter
