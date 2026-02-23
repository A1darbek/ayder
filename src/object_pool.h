#ifndef OBJECT_POOL_H
#define OBJECT_POOL_H

typedef void *(*object_factory_t)(void);
typedef void (*object_dtor_t)(void *);

typedef struct {
  void **items;
  int capacity;
  int count;
  object_factory_t factory;
  object_dtor_t dtor;
} object_pool_t;

object_pool_t *object_pool_create(int capacity, object_factory_t factory, object_dtor_t dtor);
void object_pool_destroy(object_pool_t *pool);
void *object_pool_get(object_pool_t *pool);
void object_pool_release(object_pool_t *pool, void *item);

#endif // OBJECT_POOL_H
