# start project configuration
name := ftdc
buildDir := build
packages := $(name) events hdrhist metrics util
srcFiles := makefile $(shell find . -name "*.go" -not -path "./$(buildDir)/*" -not -name "*_test.go" -not -path "./scripts/*" -not -path "*\#*")
testSrcFiles := makefile $(shell find . -name "*.go" -not -path "./$(buildDir)/*" -not -path "*\#*")
orgPath := github.com/mongodb
projectPath := $(orgPath)/$(name)
# end project configuration

# start environment setup
gobin := go
ifneq (,$(GOROOT))
gobin := $(GOROOT)/bin/go
endif

goCache := $(GOCACHE)
ifeq (,$(goCache))
goCache := $(abspath $(buildDir)/.cache)
endif
goModCache := $(GOMODCACHE)
ifeq (,$(goModCache))
goModCache := $(abspath $(buildDir)/.mod-cache)
endif
lintCache := $(GOLANGCI_LINT_CACHE)
ifeq (,$(lintCache))
lintCache := $(abspath $(buildDir)/.lint-cache)
endif

ifeq ($(OS),Windows_NT)
gobin := $(shell cygpath $(gobin))
goCache := $(shell cygpath -m $(goCache))
goModCache := $(shell cygpath -m $(goModCache))
lintCache := $(shell cygpath -m $(lintCache))
export GOROOT := $(shell cygpath -m $(GOROOT))
endif

ifneq ($(goCache),$(GOCACHE))
export GOCACHE := $(goCache)
endif
ifneq ($(goModCache),$(GOMODCACHE))
export GOMODCACHE := $(goModCache)
endif
ifneq ($(lintCache),$(GOLANGCI_LINT_CACHE))
export GOLANGCI_LINT_CACHE := $(lintCache)
endif

ifneq (,$(RACE_DETECTOR))
# cgo is required for using the race detector.
export CGO_ENABLED := 1
else
export CGO_ENABLED := 0
endif
# end environment setup

# Ensure the build directory exists, since most targets require it.
$(shell mkdir -p $(buildDir))

.DEFAULT_GOAL := compile

# start test data download targets
metrics.ftdc:
	curl -LO "https://ftdc-test-files.s3.amazonaws.com/metrics.ftdc"
perf_metrics.ftdc:
	curl -LO "https://ftdc-test-files.s3.amazonaws.com/perf_metrics.ftdc"
genny_metrics.ftdc:
	curl -LO "https://ftdc-test-files.s3.amazonaws.com/genny_metrics.ftdc"
$(buildDir)/output.ftdc.test: perf_metrics.ftdc metrics.ftdc genny_metrics.ftdc
# end test data file download targets

# start lint setup targets
$(buildDir)/golangci-lint:
	@curl --retry 10 --retry-max-time 60 -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(buildDir) v1.64.5 >/dev/null 2>&1
$(buildDir)/run-linter: cmd/run-linter/run-linter.go $(buildDir)/golangci-lint
	$(gobin) build -o $@ $<
# end lint setup targets

# start output files
testOutput := $(foreach target,$(packages),$(buildDir)/output.$(target).test)
lintOutput := $(foreach target,$(packages),$(buildDir)/output.$(target).lint)
coverageOutput := $(foreach target,$(packages),$(buildDir)/output.$(target).coverage)
htmlCoverageOutput := $(foreach target,$(packages),$(buildDir)/output.$(target).coverage.html)
.PRECIOUS: $(testOutput) $(lintOutput) $(coverageOutput) $(htmlCoverageOutput)
# end output files

# start basic development operations
compile: $(srcFiles)
	$(gobin) build $(subst $(name),,$(subst -,/,$(foreach pkg,$(packages),./$(pkg))))
test: $(testOutput)
lint: $(lintOutput)
coverage: $(coverageOutput)
html-coverage: $(htmlCoverageOutput)
phony += compile lint test coverage html-coverage

# start convenience targets for running tests and coverage tasks on a
# specific package.
test-%: $(buildDir)/output.%.test
	
coverage-%: $(buildDir)/output.%.coverage
	
html-coverage-%: $(buildDir)/output.%.coverage $(buildDir)/output.%.coverage.html
	@grep -s -q -e "^PASS" $(subst coverage,test,$<)
lint-%: $(buildDir)/output.%.lint
	@grep -v -s -q "^--- FAIL" $<
# end convenience targets
# end basic development operations

# start test and coverage artifacts
testArgs := -v
ifneq (,$(RUN_TEST))
testArgs += -run='$(RUN_TEST)'
endif
ifneq (,$(RUN_COUNT))
testArgs += -count=$(RUN_COUNT)
endif
ifneq (,$(SKIP_LONG))
testArgs += -short
endif
ifneq (,$(RACE_DETECTOR))
testArgs += -race
endif
ifneq (,$(TEST_TIMEOUT))
testArgs += -timeout=$(TEST_TIMEOUT)
else
testArgs += -timeout=30m
endif
$(buildDir)/output.%.test: .FORCE
	$(gobin) test $(testArgs) ./$(if $(subst $(name),,$*),$(subst -,/,$*),) | tee $@
	@grep -s -q -e "^PASS" $@
$(buildDir)/output.%.coverage: .FORCE
	$(gobin) test $(testArgs) ./$(if $(subst $(name),,$*),$(subst -,/,$*),) -covermode=count -coverprofile $@ | tee $(buildDir)/output.$*.test
	@-[ -f $@ ] && $(gobin) tool cover -func=$@ | sed 's%$(projectPath)/%%' | column -t
	@grep -s -q -e "^PASS" $(subst coverage,test,$@)
$(buildDir)/output.%.coverage.html: $(buildDir)/output.%.coverage
	$(gobin) tool cover -html=$< -o $@

ifneq (go,$(gobin))
# We have to handle the PATH specially for linting in CI, because if the PATH has a different version of the Go
# binary in it, the linter won't work properly.
lintEnvVars := PATH="$(shell dirname $(gobin)):$(PATH)"
endif
$(buildDir)/output.%.lint: $(buildDir)/run-linter .FORCE
	@$(lintEnvVars) ./$< --output=$@ --lintBin=$(buildDir)/golangci-lint --packages='$*'
# end test and coverage artifacts

# start module management targets
mod-tidy:
	$(gobin) mod tidy
# Check if go.mod and go.sum are clean. If they're clean, then mod tidy should not produce a different result.
verify-mod-tidy:
	$(gobin) run cmd/verify-mod-tidy/verify-mod-tidy.go -goBin="$(gobin)"
phony += mod-tidy verify-mod-tidy
# end module management targets

# start cleanup targets
clean:
	rm -rf $(buildDir)
clean-results:
	rm -rf $(buildDir)/output.*
phony += clean clean-results
# end cleanup targets

# configure phony targets
.FORCE:
.PHONY: $(phony) .FORCE
