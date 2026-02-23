#ifndef RESPONSE_H
#define RESPONSE_H

#define RESPONSE_BUFFER_SIZE 32768

typedef struct {
  char buffer[RESPONSE_BUFFER_SIZE];
  //.
} Response;

#endif // RESPONSE_H
