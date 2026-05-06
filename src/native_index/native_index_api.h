#ifndef NATIVE_INDEX_H
#define NATIVE_INDEX_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define NATIVE_INDEX_MAGIC 0x31495843u /* "CXI1" little endian */

typedef struct {
    const char *data;
    size_t len;
} native_index_str;

typedef struct {
    uint32_t doc_id;
    uint64_t size;
    uint32_t gid;
    uint32_t sid;
    uint32_t eid;
    uint32_t uid;
} native_index_doc_ref;

typedef struct {
    uint32_t key_id;
    uint64_t values_offset;
    uint32_t values_count;
} native_index_posting_entry;

typedef struct {
    int fd;
    size_t file_size;
    const uint8_t *base;

    uint32_t doc_count;
    uint32_t token_count;
    uint32_t ext_count;
    uint32_t user_count;

    const native_index_doc_ref *docs;

    const native_index_posting_entry *token_entries;
    const uint32_t *token_values;

    const native_index_posting_entry *ext_entries;
    const uint32_t *ext_values;

    const native_index_posting_entry *user_entries;
    const uint32_t *user_values;
} native_index_index;

typedef struct {
    uint32_t *doc_ids;
    size_t count;
} native_index_docset;

typedef struct {
    const uint32_t *token_ids;
    size_t token_count;
    const uint32_t *ext_ids;
    size_t ext_count;
    const uint32_t *user_ids;
    size_t user_count;
    uint64_t size_min;
    uint64_t size_max;
    int has_size_min;
    int has_size_max;
} native_index_query;

int native_index_open(const char *path, native_index_index *out);
void native_index_close(native_index_index *index);

int native_index_query_docs(const native_index_index *index, const native_index_query *query, native_index_docset *out);
void native_index_free_docset(native_index_docset *set);

#ifdef __cplusplus
}
#endif

#endif
