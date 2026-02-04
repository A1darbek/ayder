CC=gcc
CPPFLAGS=-I./include -Ideps/picohttpparser -DHTTP_SERVER_FAST -DNDEBUG
CFLAGS=-O3 -g -pipe -flto -march=native -ffunction-sections -Wall -Wextra -Wshadow -Wdouble-promotion
LDFLAGS=-flto
LDLIBS=-luv -levent -lz -luring -lm -lcurl -lssl -lcrypto

SRC=$(wildcard src/*.c) deps/picohttpparser/picohttpparser.c
OBJ=$(SRC:.c=.o)
EXEC=ayder

%.o: %.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(EXEC): $(OBJ)
	$(CC) $(OBJ) -o $@ $(LDFLAGS) $(LDLIBS)

debug: $(EXEC)
	gdb ./$(EXEC)

clean:
	rm -f $(OBJ) $(EXEC)
