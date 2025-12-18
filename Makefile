CC=gcc
CFLAGS=-O3 -g -I./include -Ideps/picohttpparser -luv -lz -pipe -flto -march=native \
          -ffunction-sections \
          -DNDEBUG -DHTTP_SERVER_FAST \
          -Wall -Wextra -Wshadow -Wdouble-promotion -luring -lm -lcurl -lssl -lcrypto

SRC=$(wildcard src/*.c) deps/picohttpparser/picohttpparser.c
OBJ=$(SRC:.c=.o)
EXEC=ayder

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

$(EXEC): $(OBJ)
	$(CC) $(OBJ) -o $@ $(CFLAGS)

debug: $(EXEC)
	gdb ./$(EXEC)

clean:
	rm -f $(OBJ) $(EXEC)
