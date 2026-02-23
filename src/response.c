// response.c
#include "response.h"
#include <stdarg.h>
#include <stdio.h>

void response_json(Response *res, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(res->buffer, RESPONSE_BUFFER_SIZE, fmt, ap);
  va_end(ap);
  if (n < 0)
    res->buffer[0] = '\0';
  else if (n >= (int)RESPONSE_BUFFER_SIZE)
    res->buffer[RESPONSE_BUFFER_SIZE - 1] = '\0';
}
