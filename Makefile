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
UFLAGS =
CFLAGS = -Wall -O3 $(OSFLAGS) $(UFLAGS)
OPENMP = -fopenmp -DGMULTI=1
PTHREADS = -lpthread -DGMULTI=1
INCLUDE = -I/usr/local/include
LIBS = -L/usr/local/lib64
# PARQUET = $(INCLUDE) $(LIBS) -l:libarrow.a -l:libparquet.a
PARQUET = $(INCLUDE) $(LIBS) -larrow -lparquet

all: clean links parquet

# ---------------------------------------------------------------------
# Rules

links:
	rm -f  src/plugin/lib
	rm -f  src/plugin/spi
	ln -sf ../../lib 	  src/plugin/lib
	ln -sf lib/spi-$(SPI) src/plugin/spi

parquet: src/plugin/parquet.cpp src/plugin/spi/stplugin.cpp
	mkdir -p ./build/parquet
	$(GCC) $(CFLAGS) -o $(OUT) src/plugin/spi/stplugin.cpp src/plugin/parquet.cpp $(PARQUET)
	cp build/*plugin lib/plugin/

copy:
	cp changelog.md        ./build/parquet/
	cp src/parquet.pkg     ./build/parquet/
	cp src/stata.toc       ./build/parquet/
	cp src/ado/parquet.ado ./build/parquet/
	cp docs/parquet.sthlp  ./build/parquet/
	cp build/*plugin       ./build/parquet/
	cp ./src/test/parquet_tests.do ./build/
	cp ./build/parquet/* ./build/

zip:
	cd build && ls parquet/*.{ado,sthlp,plugin} | zip -@ parquet-ssc.zip
	cd build && ls parquet/* | zip -@ parquet-latest.zip
	mv build/parquet/* build/
	rm -rf build/parquet/

.PHONY: clean
clean:
	rm -f $(OUT)
	rm -rf build/parquet
