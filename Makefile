# ---------------------------------------------------------------------
# OS parsing

ifeq ($(OS),Windows_NT)
else
	UNAME_S := $(shell uname -s)
	ifeq ($(UNAME_S),Linux)
		OSFLAGS = -shared -fPIC -DSYSTEM=OPUNIX
		OUT = build/parquet_unix.plugin
	endif
	ifeq ($(UNAME_S),Darwin)
	endif
	GCC = g++
endif

# ---------------------------------------------------------------------
# Flags

SPI = 2.0
CFLAGS = -Wall -O3 $(OSFLAGS)
OPENMP = -fopenmp -DGMULTI=1
PTHREADS = -lpthread -DGMULTI=1
# PARQUET = -I/usr/local/include -L/usr/local/lib64 -l:libarrow.a -l:libparquet.a
PARQUET = -I/usr/local/include -L/usr/local/lib64 -larrow -lparquet

all: clean links parquet

# ---------------------------------------------------------------------
# Rules

links:
	rm -f  src/plugin/lib
	rm -f  src/plugin/spi
	ln -sf ../../lib 	  src/plugin/lib
	ln -sf lib/spi-$(SPI) src/plugin/spi

parquet: src/plugin/parquet.cpp src/plugin/spi/stplugin.cpp
	mkdir -p ./build
	$(GCC) $(CFLAGS) -o $(OUT) src/plugin/spi/stplugin.cpp src/plugin/parquet.cpp $(PARQUET)
	cp build/*plugin lib/plugin/

.PHONY: clean
clean:
	rm -f $(OUT)
