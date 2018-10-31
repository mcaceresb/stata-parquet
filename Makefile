# ---------------------------------------------------------------------
# OS parsing

ifeq ($(OS),Windows_NT)
	OSFLAGS = -shared
	# GCC = x86_64-w64-mingw32-g++.exe
	GCC = g++.exe
	OUT = parquet_windows.plugin
else
	UNAME_S := $(shell uname -s)
	ifeq ($(UNAME_S),Linux)
		OSFLAGS = -shared -fPIC -DSYSTEM=OPUNIX
		OUT = parquet_unix.plugin
	endif
	ifeq ($(UNAME_S),Darwin)
		OSFLAGS = -bundle -DSYSTEM=APPLEMAC
		OUT = parquet_macosx.plugin
	endif
	GCC = g++
endif

# ---------------------------------------------------------------------
# Flags

SPI = 2.0
UFLAGS =
CFLAGS = -Wall -O3 $(OSFLAGS) $(UFLAGS)
INCLUDE = /usr/local/include
LIBS = /usr/local/lib64
PARQUET = -I$(INCLUDE) -L$(LIBS) -larrow -lparquet
STATA = ${HOME}/.local/stata13/stata
STATARUN = LD_LIBRARY_PATH=$(LIBS) ${STATA}

# ---------------------------------------------------------------------
# Rules

## Build plugin
all: clean links parquet copy

## Symlinks to plugin libraries
links:
	rm -f  src/plugin/lib
	rm -f  src/plugin/spi
	ln -sf ../../lib 	  src/plugin/lib
	ln -sf lib/spi-$(SPI) src/plugin/spi

## Build parquet plugin
parquet: src/plugin/parquet.cpp src/plugin/spi/stplugin.cpp
	mkdir -p ./build
	$(GCC) $(CFLAGS) -o build/$(OUT) src/plugin/spi/stplugin.cpp src/plugin/parquet.cpp $(PARQUET)
	cp build/*plugin lib/plugin/

## Copy Stata package files to ./build
copy:
	cp src/parquet.pkg     ./build/
	cp src/stata.toc       ./build/
	cp src/ado/parquet.ado ./build/
	cp docs/parquet.sthlp  ./build/
	cp ./src/test/parquet_tests.do ./build/

## Install the Stata package (replace if necessary)
replace:
	sed -i.bak 's/parquet_os.plugin/$(OUT)/' build/parquet.pkg
	cd build/ && $(STATARUN) -b "cap noi net uninstall parquet"
	cd build/ && $(STATARUN) -b "net install parquet, from(\`\"${PWD}/build\"')"

## Run tests
test:
	cp ./src/test/parquet_tests.do ./build/
	cd build/ && $(STATARUN) -b do parquet_tests.do

.PHONY: clean
clean:
	rm -rf build

#######################################################################
#                                                                     #
#                    Self-Documenting Foo (Ignore)                    #
#                                                                     #
#######################################################################

.DEFAULT_GOAL := show-help

.PHONY: show-help
show-help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
