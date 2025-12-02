.DEFAULT_GOAL := help

# SHELL := /bin/bash

################################### Build and Test

all: test

.PHONY: build
build: ## cmake generate and build shell
	@mkdir -p build
	cd build && \
		cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake && \
		cmake --build .

.PHONY: test
test: build ## Run codecrafters cloud tests
	codecrafters test

################################### Static Analysis
CPPCHECK_CACHE_DIR ?= cppcheck-cache

CLANG_FORMAT_CMD = find . -iname "*.hpp" -o -iname "*.cpp" | xargs clang-format -style=file -i
.PHONY: check-format
check-format: ## Check clang format errors
	$(CLANG_FORMAT_CMD) --dry-run -Werror

format: ## Apply clang format to code
	$(CLANG_FORMAT_CMD)

.PHONY: check
check: ## Run static analysis checks using cppcheck
	@mkdir -p $(CPPCHECK_CACHE_DIR)
	@cppcheck --version
	cppcheck --quiet \
			 --enable=all \
			 --std=c++23 \
			 --cppcheck-build-dir=$(CPPCHECK_CACHE_DIR) \
			 --inline-suppr \
			 --check-level=exhaustive \
			 --error-exitcode=1 \
			 --suppressions-list=config/cppcheck-suppressions.txt \
			 --checkers-report=cppcheck_report.txt \
			 ./src

################################### Other targets

help: ## Show this help message
	@grep '##' $(MAKEFILE_LIST) | grep -v grep | awk 'BEGIN {FS = ": .*##"}; {printf "\033[36m%-38s\033[0m %s\n", $$1, $$2}'
