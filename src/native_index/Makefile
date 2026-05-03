CC ?= cc
AR ?= ar
CFLAGS ?= -O3 -fPIC -std=c11 -Wall -Wextra -Werror
LDFLAGS ?=

all: libcdx1.so libcdx1.a cdx1_query

libcdx1.so: index.o
	$(CC) -shared $(LDFLAGS) -o $@ $^

libcdx1.a: index.o
	$(AR) rcs $@ $^

index.o: index.c index.h
	$(CC) $(CFLAGS) -c index.c -o $@

cdx1_query: query_cli.o libcdx1.a
	$(CC) $(CFLAGS) $(LDFLAGS) query_cli.o libcdx1.a -o $@

query_cli.o: query_cli.c index.h
	$(CC) $(CFLAGS) -c query_cli.c -o $@

clean:
	rm -f *.o *.a *.so cdx1_query
