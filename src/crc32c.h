#pragma once
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif
uint32_t crc32c(uint32_t crc, const void *buf, size_t len);
#ifdef __cplusplus
}
#endif
