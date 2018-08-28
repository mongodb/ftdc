buildDir := build
srcFiles := $(shell find . -name "*.go" -not -path "./$(buildDir)/*" -not -name "*_test.go" -not -path "*\#*")
testFiles := $(shell find . -name "*.go" -not -path "./$(buildDir)/*" -not -path "*\#*")

_testPackages := ./

compile:
	go build $(_testPackages)
race:
	@mkdir -p $(buildDir)
	go test -count 1 -v -race $(_testPackages) | tee $(buildDir)/race.ftdc.out
	@grep -s -q -e "^PASS" $(buildDir)/race.sink.out && ! grep -s -q "^WARNING: DATA RACE" $(buildDir)/race.ftdc.out
test:metrics.ftdc
	@mkdir -p $(buildDir)
	go test -v -cover $(_testPackages) | tee $(buildDir)/test.ftdc.out
	@grep -s -q -e "^PASS" $(buildDir)/test.ftdc.out
coverage:$(buildDir)/cover.out
	@go tool cover -func=$< | sed -E 's%github.com/.*/ftdc/%%' | column -t
coverage-html:$(buildDir)/cover.html

$(buildDir):$(srcFiles) compile
	@mkdir -p $@
$(buildDir)/cover.out:$(buildDir) $(testFiles) .FORCE
	go test -v -coverprofile $@ -cover $(_testPackages)
$(buildDir)/cover.html:$(buildDir)/cover.out
	go tool cover -html=$< -o $@
.FORCE:


metrics.ftdc:
	wget "https://whatfox.net/metrics.ftdc"
