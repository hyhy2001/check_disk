#ifndef CDX1_INDEX_H
#define CDX1_INDEX_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CDX1_MAGIC 0x31495843u /* "CXI1" little endian */

typedef struct {
    const char *data;
    size_t len;
} cdx1_str;

typedef struct {
    uint32_t doc_id;
    uint64_t size;
    uint32_t gid;
    uint32_t sid;
    uint32_t eid;
    uint32_t uid;
} cdx1_doc_ref;

typedef struct {
    uint32_t key_id;
    uint64_t values_offset;
    uint32_t values_count;
} cdx1_posting_entry;

typedef struct {
    int fd;
    size_t file_size;
    const uint8_t *base;

    uint32_t doc_count;
    uint32_t token_count;
    uint32_t ext_count;
    uint32_t user_count;

    const cdx1_doc_ref *docs;

    const cdx1_posting_entry *token_entries;
    const uint32_t *token_values;

    const cdx1_posting_entry *ext_entries;
    const uint32_t *ext_values;

    const cdx1_posting_entry *user_entries;
    const uint32_t *user_values;
} cdx1_index;

typedef struct {
    uint32_t *doc_ids;
    size_t count;
} cdx1_docset;

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
} cdx1_query;

int cdx1_open(const char *path, cdx1_index *out);
void cdx1_close(cdx1_index *index);

int cdx1_query_docs(const cdx1_index *index, const cdx1_query *query, cdx1_docset *out);
void cdx1_free_docset(cdx1_docset *set);

#ifdef __cplusplus
}
#endif

#endif
