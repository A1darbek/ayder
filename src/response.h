#ifndef RESPONSE_H
#define RESPONSE_H

#define RESPONSE_BUFFER_SIZE 32768

typedef struct {
    char buffer[RESPONSE_BUFFER_SIZE];

} Response;

// Format JSON (or any text) into res->buffer
void response_json(Response *res, const char *fmt, ...);

#endif // RESPONSE_H
