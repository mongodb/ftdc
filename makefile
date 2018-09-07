buildDir := build
srcFiles := $(shell find . -name "*.go" -not -path "./$(buildDir)/*" -not -name "*_test.go" -not -path "*\#*")
testFiles := $(shell find . -name "*.go" -not -path "./$(buildDir)/*" -not -path "*\#*")

_testPackages := ./

testArgs := -v
ifneq (,$(RUN_TEST))
testArgs += -run='$(RUN_TEST)'
endif
ifneq (,$(RUN_COUNT))
testArgs += -count='$(RUN_COUNT)'
endif
ifneq (,$(SKIP_LONG))
testArgs += -short
endif
ifneq (,$(DISABLE_COVERAGE))
testArgs += -cover
endif 
ifneq (,$(RACE_DETECTOR))
testArgs += -race
endif

compile:
	go build $(_testPackages)
test:metrics.ftdc
	@mkdir -p $(buildDir)
	go test $(testArgs) $(_testPackages) | tee $(buildDir)/test.ftdc.out
	@grep -s -q -e "^PASS" $(buildDir)/test.ftdc.out
coverage:$(buildDir)/cover.out
	@go tool cover -func=$< | sed -E 's%github.com/.*/ftdc/%%' | column -t
coverage-html:$(buildDir)/cover.html

$(buildDir):$(srcFiles) compile
	@mkdir -p $@
$(buildDir)/cover.out:$(buildDir) $(testFiles) .FORCE
	go test $(testArgs) -covermode=count -coverprofile $@ -cover $(_testPackages)
$(buildDir)/cover.html:$(buildDir)/cover.out
	go tool cover -html=$< -o $@
.FORCE:


metrics.ftdc:
	wget "https://whatfox.net/metrics.ftdc"
