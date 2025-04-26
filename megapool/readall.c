#include <stdlib.h>  // malloc, realloc, free
#include <unistd.h>  // read
#include <errno.h>   // errno

#ifndef  READALL_CHUNK
#define  READALL_CHUNK  262144
#endif

#define  READALL_OK          0  /* Success */
#define  READALL_INVALID    -1  /* Invalid parameters */
#define  READALL_ERROR      -2  /* Stream error */
#define  READALL_TOOMUCH    -3  /* Too much input */
#define  READALL_NOMEM      -4  /* Out of memory */

/* Read all data from a file descriptor into a heap-allocated buffer */
int readall(int fd, char **dataptr, size_t *sizeptr) {
    char *data = NULL, *temp;
    size_t size = 0;
    size_t used = 0;
    ssize_t n;

    if (fd < 0 || dataptr == NULL || sizeptr == NULL)
        return READALL_INVALID;

    while (1) {
        if (used + READALL_CHUNK + 1 > size) {
            size = used + READALL_CHUNK + 1;

            if (size <= used) {
                free(data);
                return READALL_TOOMUCH;
            }

            temp = realloc(data, size);
            if (temp == NULL) {
                free(data);
                return READALL_NOMEM;
            }
            data = temp;
        }

        n = read(fd, data + used, READALL_CHUNK);
        if (n < 0) {
            if (errno == EINTR)
                continue;  // interrupted syscall, retry
            free(data);
            return READALL_ERROR;
        }
        if (n == 0)
            break;  // EOF

        used += n;
    }

    temp = realloc(data, used + 1);
    if (temp == NULL) {
        free(data);
        return READALL_NOMEM;
    }
    data = temp;
    data[used] = '\0';  // null terminator for safety

    *dataptr = data;
    *sizeptr = used;

    return READALL_OK;
}