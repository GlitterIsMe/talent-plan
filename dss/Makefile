#export RUSTFLAGS=-Dwarnings
export RUST_TEST_THREADS=1
export RUST_BACKTRACE=1

LOG_LEVEL ?= labs6824=info

check:
	cargo fmt --all
	cargo clippy --all --tests -- -A clippy::all

test: test_others test_2 test_3

test_2: test_2a test_2b test_2c

test_2a: cargo_test_2a

test_2b: cargo_test_2b

test_2c: cargo_test_2c

test_3: test_3a test_3b

test_3a: cargo_test_3a

test_3b: cargo_test_3b

cargo_test_%:
	RUST_LOG=${LOG_LEVEL} cargo test --lib -- --nocapture --test $*

test_others:
	RUST_LOG=${LOG_LEVEL} cargo test -p labrpc -p labcodec -- --nocapture

test_percolator:
	RUST_LOG=${LOG_LEVEL} cargo test -p percolator -- --nocapture
