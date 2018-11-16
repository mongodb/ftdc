buildDir := build
srcFiles := $(shell find . -name "*.go" -not -path "./$(buildDir)/*" -not -name "*_test.go" -not -path "*\#*" -path "./bsonx*")
testFiles := $(shell find . -name "*.go" -not -path "./$(buildDir)/*" -not -path "*\#*")
bsonxFiles := $(shell find ./bsonx -name "*.go" -not -path "./$(buildDir)/*" -not -path "*\#*")

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


tools:$(buildDir)/sysinfo-collector $(buildDir)/ftdcdump

$(buildDir)/sysinfo-collector:cmd/sysinfo-collector/sysinfo-collector.go $(srcFiles)
	go build -o $@ $<
$(buildDir)/ftdcdump:cmd/ftdcdump/ftdcdump.go $(srcFiles)
	go build -o $@ $<

compile:
	go build $(_testPackages)
test:metrics.ftdc perf_metrics.ftdc
	@mkdir -p $(buildDir)
	go test $(testArgs) $(_testPackages) | tee $(buildDir)/test.ftdc.out
	@grep -s -q -e "^PASS" $(buildDir)/test.ftdc.out
coverage:$(buildDir)/cover.out
	@go tool cover -func=$< | sed -E 's%github.com/.*/ftdc/%%' | column -t
coverage-html:$(buildDir)/cover.html $(buildDir)/cover.bsonx.html

benchmark:
	go test -v -benchmem -bench=. -run="Benchmark.*" -timeout=20m


$(buildDir):$(srcFiles) compile
	@mkdir -p $@
$(buildDir)/cover.out:$(buildDir) $(testFiles) .FORCE
	go test $(testArgs) -covermode=count -coverprofile $@ -cover ./
$(buildDir)/cover.html:$(buildDir)/cover.out
	go tool cover -html=$< -o $@

test-bsonx:
	@mkdir -p $(buildDir)
	go test $(testArgs) ./bsonx | tee $(buildDir)/test.bsonx.out
	@grep -s -q -e "^PASS" $(buildDir)/test.bsonx.out
coverage-bsonx:$(buildDir)/cover.bsonx.out
	@go tool cover -func=$< | sed -E 's%github.com/.*/ftdc/%%' | column -t
coverage-bsonx-html:$(buildDir)/cover.bsonx.html
$(buildDir)/cover.bsonx.out:$(buildDir) $(bsonxFiles) .FORCE
	go test $(testArgs) -covermode=count -coverprofile $@ -cover ./bsonx
$(buildDir)/cover.bsonx.html:$(buildDir)/cover.bsonx.out
	go tool cover -html=$< -o $@

.FORCE:



metrics.ftdc:
	wget "https://whatfox.net/metrics.ftdc"
perf_metrics.ftdc:
	wget "https://whatfox.net/perf_metrics.ftdc"

vendor-clean:
	rm -rf vendor/github.com/mongodb/mongo-go-driver/vendor/github.com/davecgh/
	rm -rf vendor/github.com/mongodb/mongo-go-driver/vendor/github.com/stretchr/testify/
	rm -rf vendor/github.com/mongodb/mongo-go-driver/data/
	rm -rf vendor/github.com/mongodb/mongo-go-driver/vendor/github.com/pmezard/
	rm -rf vendor/github.com/mongodb/mongo-go-driver/vendor/github.com/google/go-cmp/
	rm -rf vendor/github.com/mongodb/mongo-go-driver/vendor/github.com/kr/
	sed -ri 's/bson:"(.*),omitempty"/bson:"\1"/' `find vendor/github.com/mongodb/grip/vendor/github.com/shirou/gopsutil/ -name "*go"` || true
