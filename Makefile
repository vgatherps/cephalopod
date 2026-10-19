LINUX = scripts/test-linux.sh

.PHONY: test test-release e2e bench clippy check-x86

test:            ## Linux tests in Docker (debug)
	$(LINUX) test --workspace

test-release:    ## Linux tests in Docker (release)
	$(LINUX) test --workspace --release

e2e:             ## one sequencer process, four player processes
	$(LINUX) test -p ceph-fifo --test e2e --release

bench:           ## push benchmark
	$(LINUX) run --release -p ceph-ring --example push_bench

clippy:          ## clippy for the Linux target, warnings are errors
	cargo clippy --workspace --tests --examples --target aarch64-unknown-linux-gnu -- -D warnings

check-x86:       ## compile the tests for x86_64 under the amd64 image
	PLATFORM=linux/amd64 $(LINUX) test --no-run --release --workspace
