#ifndef MALLOC_H
#define MALLOC_H
#include <stdlib.h>
#include <string.h>

static int malloc_cnt;
static int free_cnt;

static void log_memory( const char *file, const char* func, int line, const char* format, ... );

#define LOG_MALLOC(size) \
    ({ \
     void* mm = malloc(size);\
     int cnt; \
    if (mm) { \
        cnt = __sync_add_and_fetch(&malloc_cnt, 1); \
    } \
    log_memory(__FILE__, __PRETTY_FUNCTION__, __LINE__, "LOG_MALLOC %p(size:%d), malloc count:%d", mm, (size), cnt); mm; \
    })

#define LOG_CALLOC(n, unit) \
    ({ \
     int size = (n) * (unit); \
     void* mm = malloc(size);\
     int cnt; \
    if (mm) { \
       cnt = __sync_add_and_fetch(&malloc_cnt, 1); \
       memset(mm, 0, size); \
    } \
    log_memory(__FILE__, __PRETTY_FUNCTION__, __LINE__, "LOG_MALLOC %p(size:%d), malloc count:%d", mm, size, cnt); mm; \
    })

#define LOG_FREE(ptr) \
    ({ \
     void* mm = (ptr); \
     int cnt; \
    if (mm) { \
        cnt = __sync_add_and_fetch(&free_cnt, 1); \
    } \
    log_memory(__FILE__, __PRETTY_FUNCTION__, __LINE__, "LOG_FREE %p, free count:%d", mm, cnt); free(ptr); \
    })

static void log_memory( const char *file, const char* func, int line, const char* format, ... )
{    
    va_list args;
    char buf[256] = {0};

    va_start( args, format );
    vsnprintf( buf, 256, format, args );
    fprintf( stdout, "[%s:%s:%d] %s\n", file, func, line, buf );
    va_end( args );
    fflush( stdout );
}
#endif
