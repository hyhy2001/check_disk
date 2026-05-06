#define _GNU_SOURCE
#include "native_index_api.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdio.h>
#include <unistd.h>

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t doc_count;
    uint32_t token_count;
    uint32_t ext_count;
    uint32_t user_count;
    uint64_t docs_offset;
    uint64_t token_entries_offset;
    uint64_t token_values_offset;
    uint64_t ext_entries_offset;
    uint64_t ext_values_offset;
    uint64_t user_entries_offset;
    uint64_t user_values_offset;
} native_index_header;

static int bounds_ok(size_t file_size, uint64_t off, uint64_t bytes) {
    if (off > file_size) {
        return 0;
    }
    if (bytes > file_size - off) {
        return 0;
    }
    return 1;
}

int native_index_open(const char *path, native_index_index *out) {
    if (!path || !out) {
        return EINVAL;
    }
    memset(out, 0, sizeof(*out));

    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        return errno;
    }

    struct stat st;
    if (fstat(fd, &st) != 0) {
        int err = errno;
        close(fd);
        return err;
    }
    if (st.st_size < (off_t)sizeof(native_index_header)) {
        close(fd);
        return EPROTO;
    }

    void *mapped = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        int err = errno;
        close(fd);
        return err;
    }

    madvise(mapped, (size_t)st.st_size, MADV_SEQUENTIAL);

    const uint8_t *base = (const uint8_t *)mapped;
    const native_index_header *hdr = (const native_index_header *)base;
    if (hdr->magic != NATIVE_INDEX_MAGIC || hdr->version != 1) {
        munmap(mapped, (size_t)st.st_size);
        close(fd);
        return EPROTO;
    }

    uint64_t docs_bytes = (uint64_t)hdr->doc_count * sizeof(native_index_doc_ref);
    uint64_t token_entries_bytes = (uint64_t)hdr->token_count * sizeof(native_index_posting_entry);
    uint64_t ext_entries_bytes = (uint64_t)hdr->ext_count * sizeof(native_index_posting_entry);
    uint64_t user_entries_bytes = (uint64_t)hdr->user_count * sizeof(native_index_posting_entry);

    if (!bounds_ok((size_t)st.st_size, hdr->docs_offset, docs_bytes) ||
        !bounds_ok((size_t)st.st_size, hdr->token_entries_offset, token_entries_bytes) ||
        !bounds_ok((size_t)st.st_size, hdr->ext_entries_offset, ext_entries_bytes) ||
        !bounds_ok((size_t)st.st_size, hdr->user_entries_offset, user_entries_bytes) ||
        hdr->token_values_offset > (uint64_t)st.st_size ||
        hdr->ext_values_offset > (uint64_t)st.st_size ||
        hdr->user_values_offset > (uint64_t)st.st_size) {
        munmap(mapped, (size_t)st.st_size);
        close(fd);
        return EPROTO;
    }

    out->fd = fd;
    out->file_size = (size_t)st.st_size;
    out->base = base;
    out->doc_count = hdr->doc_count;
    out->token_count = hdr->token_count;
    out->ext_count = hdr->ext_count;
    out->user_count = hdr->user_count;

    out->docs = (const native_index_doc_ref *)(base + hdr->docs_offset);
    out->token_entries = (const native_index_posting_entry *)(base + hdr->token_entries_offset);
    out->token_values = (const uint32_t *)(base + hdr->token_values_offset);
    out->ext_entries = (const native_index_posting_entry *)(base + hdr->ext_entries_offset);
    out->ext_values = (const uint32_t *)(base + hdr->ext_values_offset);
    out->user_entries = (const native_index_posting_entry *)(base + hdr->user_entries_offset);
    out->user_values = (const uint32_t *)(base + hdr->user_values_offset);

    return 0;
}

void native_index_close(native_index_index *index) {
    if (!index) {
        return;
    }
    if (index->base && index->file_size > 0) {
        munmap((void *)index->base, index->file_size);
    }
    if (index->fd > 0) {
        close(index->fd);
    }
    memset(index, 0, sizeof(*index));
}

static int posting_lookup(const native_index_posting_entry *entries,
                         const uint32_t *values,
                         uint32_t entry_count,
                         uint32_t key_id,
                         const uint32_t **out_values,
                         uint32_t *out_count) {
    uint32_t lo = 0;
    uint32_t hi = entry_count;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        uint32_t key = entries[mid].key_id;
        if (key < key_id) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo >= entry_count || entries[lo].key_id != key_id) {
        *out_values = NULL;
        *out_count = 0;
        return 0;
    }
    *out_values = values + entries[lo].values_offset;
    *out_count = entries[lo].values_count;
    return 1;
}

static uint32_t *intersect_sorted(const uint32_t *a,
                                  uint32_t a_count,
                                  const uint32_t *b,
                                  uint32_t b_count,
                                  uint32_t *out_count) {
    uint32_t max_out = a_count < b_count ? a_count : b_count;
    uint32_t *out = (uint32_t *)malloc((size_t)max_out * sizeof(uint32_t));
    if (!out) {
        *out_count = 0;
        return NULL;
    }
    uint32_t i = 0;
    uint32_t j = 0;
    uint32_t k = 0;
    while (i < a_count && j < b_count) {
        if (a[i] == b[j]) {
            out[k++] = a[i];
            i++;
            j++;
        } else if (a[i] < b[j]) {
            i++;
        } else {
            j++;
        }
    }
    *out_count = k;
    return out;
}

static int apply_filter_ids_or(const native_index_posting_entry *entries,
                            const uint32_t *values,
                            uint32_t entry_count,
                            const uint32_t *ids,
                            size_t id_count,
                            uint32_t **candidate,
                            uint32_t *candidate_count) {
    if (id_count == 0) {
        return 0;
    }

    size_t total_capacity = 0;
    for (size_t i = 0; i < id_count; i++) {
        const uint32_t *vals = NULL;
        uint32_t count = 0;
        if (!posting_lookup(entries, values, entry_count, ids[i], &vals, &count) || count == 0) {
            continue;
        }
        total_capacity += count;
    }

    if (total_capacity == 0) {
        free(*candidate);
        *candidate = NULL;
        *candidate_count = 0;
        return 0;
    }

    uint32_t *scratch = (uint32_t *)malloc(total_capacity * sizeof(uint32_t));
    uint32_t *current = (uint32_t *)malloc(total_capacity * sizeof(uint32_t));
    if (!scratch || !current) {
        free(scratch);
        free(current);
        return ENOMEM;
    }

    uint32_t current_count = 0;
    for (size_t i = 0; i < id_count; i++) {
        const uint32_t *vals = NULL;
        uint32_t count = 0;
        if (!posting_lookup(entries, values, entry_count, ids[i], &vals, &count) || count == 0) {
            continue;
        }
        if (current_count == 0) {
            memcpy(current, vals, (size_t)count * sizeof(uint32_t));
            current_count = count;
            continue;
        }

        uint32_t a = 0, b = 0, k = 0;
        while (a < current_count && b < count) {
            uint32_t va = current[a], vb = vals[b];
            if (va == vb) { scratch[k++] = va; a++; b++; }
            else if (va < vb) { scratch[k++] = va; a++; }
            else { scratch[k++] = vb; b++; }
        }
        while (a < current_count) scratch[k++] = current[a++];
        while (b < count) scratch[k++] = vals[b++];

        memcpy(current, scratch, (size_t)k * sizeof(uint32_t));
        current_count = k;
    }

    free(scratch);

    if (*candidate == NULL) {
        uint32_t *out = (uint32_t *)malloc((size_t)current_count * sizeof(uint32_t));
        if (!out) {
            free(current);
            return ENOMEM;
        }
        memcpy(out, current, (size_t)current_count * sizeof(uint32_t));
        free(current);
        *candidate = out;
        *candidate_count = current_count;
        return 0;
    }

    uint32_t out_count = 0;
    uint32_t *merged = intersect_sorted(*candidate, *candidate_count, current, current_count, &out_count);
    free(*candidate);
    free(current);
    if (!merged && out_count > 0) {
        return ENOMEM;
    }
    *candidate = merged;
    *candidate_count = out_count;
    return 0;
}

int native_index_query_docs(const native_index_index *index, const native_index_query *query, native_index_docset *out) {
    if (!index || !query || !out) {
        return EINVAL;
    }
    memset(out, 0, sizeof(*out));

    uint32_t *candidate = NULL;
    uint32_t candidate_count = 0;

    int err = apply_filter_ids_or(index->token_entries, index->token_values, index->token_count,
                               query->token_ids, query->token_count,
                               &candidate, &candidate_count);
    if (err != 0) {
        return err;
    }
    err = apply_filter_ids_or(index->ext_entries, index->ext_values, index->ext_count,
                           query->ext_ids, query->ext_count,
                           &candidate, &candidate_count);
    if (err != 0) {
        free(candidate);
        return err;
    }
    err = apply_filter_ids_or(index->user_entries, index->user_values, index->user_count,
                           query->user_ids, query->user_count,
                           &candidate, &candidate_count);
    if (err != 0) {
        free(candidate);
        return err;
    }

    if (candidate == NULL) {
        candidate_count = index->doc_count;
        candidate = (uint32_t *)malloc((size_t)candidate_count * sizeof(uint32_t));
        if (!candidate) {
            return ENOMEM;
        }
        for (uint32_t i = 0; i < candidate_count; i++) {
            candidate[i] = i;
        }
    }

    if (!query->has_size_min && !query->has_size_max) {
        out->doc_ids = candidate;
        out->count = candidate_count;
        return 0;
    }

    uint32_t *filtered = (uint32_t *)malloc((size_t)candidate_count * sizeof(uint32_t));
    if (!filtered) {
        free(candidate);
        return ENOMEM;
    }

    uint64_t min_size = query->has_size_min ? query->size_min : 0;
    uint64_t max_size = query->has_size_max ? query->size_max : ULLONG_MAX;

    size_t keep = 0;
    for (uint32_t i = 0; i < candidate_count; i++) {
        uint32_t doc_id = candidate[i];
        if (doc_id >= index->doc_count) {
            continue;
        }
        uint64_t sz = index->docs[doc_id].size;
        if (sz >= min_size && sz <= max_size) {
            filtered[keep++] = doc_id;
        }
    }

    free(candidate);
    out->doc_ids = filtered;
    out->count = keep;
    return 0;
}

void native_index_free_docset(native_index_docset *set) {
    if (!set) {
        return;
    }
    free(set->doc_ids);
    set->doc_ids = NULL;
    set->count = 0;
}
