CC ?= cc
AR ?= ar
CFLAGS ?= -O3 -fPIC -std=c11 -Wall -Wextra -Werror
LDFLAGS ?=
LDLIBS ?=
COMPAT_IMAGE ?= quay.io/pypa/manylinux2014_x86_64
COMPAT_IMAGE_RHEL6 ?= quay.io/pypa/manylinux2010_x86_64

all: clean libcdx1.so libcdx1.a query_cli

libcdx1.so: index.o
	$(CC) -shared $(LDFLAGS) -o $@ $^

libcdx1.a: index.o
	$(AR) rcs $@ $^

index.o: index.c index.h
	$(CC) $(CFLAGS) -c index.c -o $@

query_cli: query_cli.o libcdx1.a
	$(CC) $(CFLAGS) $(LDFLAGS) query_cli.o libcdx1.a $(LDLIBS) -o $@

query_cli.o: query_cli.c index.h
	$(CC) $(CFLAGS) -c query_cli.c -o $@

clean:
	rm -f *.o *.a *.so query_cli

compat-build:
	docker run --rm -v $(PWD):/work -w /work $(COMPAT_IMAGE) /bin/bash -lc "CC=gcc AR=ar CFLAGS='-O2 -fPIC -std=c11 -Wall -Wextra -Werror' LDLIBS='-lrt' make clean all"

compat-build-rhel6:
	docker run --rm -v $(PWD):/work -w /work $(COMPAT_IMAGE_RHEL6) /bin/bash -lc "CC=gcc AR=ar CFLAGS='-O2 -fPIC -std=c11 -Wall -Wextra -Werror' LDLIBS='-lrt' make clean all"

compat-check:
	objdump -T ./query_cli | grep -o 'GLIBC_[0-9]\+\.[0-9]\+' | sort -V | uniq

.PHONY: all clean compat-build compat-build-rhel6 compat-check
