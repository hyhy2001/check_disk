#define _GNU_SOURCE
#include "index.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE (8 * 1024 * 1024)

/* Field bitmask for --fields */
#define F_DOC_ID  0x001U
#define F_GID     0x002U
#define F_UID     0x004U
#define F_SIZE    0x008U
#define F_EID     0x010U
#define F_SID     0x020U
#define F_PATH    0x040U
#define F_EXT     0x080U
#define F_USER    0x100U
#define F_ALL     0x1FFU

enum sort_mode { SORT_NONE, SORT_SIZE_ASC, SORT_SIZE_DESC, SORT_PATH_ASC };

typedef struct { uint32_t idx; uint32_t gid; uint64_t size; } doc_key;


typedef struct {
    char **items;
    size_t count;
} str_list;

typedef struct {
    uint32_t gid;
    char *path;
} gid_path;

static gid_path *g_sort_path_map = NULL;
static size_t g_sort_path_map_n = 0;

static void free_gid_map(gid_path *map, size_t count);

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s <index_dir|index.mmi>\n"
            "  [--kw a,b] [--ext .txt,.log] [--user u1,u2]\n"
            "  [--min n] [--max n] [--limit n] [--offset n]\n"
            "  [--sort size_asc|size_desc|path_asc]\n"
            "  [--json] [--docs] [--fields doc_id,gid,path,user,...]\n",
            prog);
}

static char *dupstr(const char *s) {
    size_t n = strlen(s);
    char *p = (char *)malloc(n + 1);
    if (!p) return NULL;
    memcpy(p, s, n + 1);
    return p;
}

static void free_str_list(str_list *lst) {
    if (!lst || !lst->items) return;
    for (size_t i = 0; i < lst->count; i++) free(lst->items[i]);
    free(lst->items);
    lst->items = NULL;
    lst->count = 0;
}

static int split_csv(const char *s, str_list *out) {
    memset(out, 0, sizeof(*out));
    if (!s || !*s) return 0;
    char *buf = dupstr(s);
    if (!buf) return 1;
    size_t cap = 8;
    out->items = (char **)calloc(cap, sizeof(char *));
    if (!out->items) { free(buf); return 1; }
    char *tok = strtok(buf, ",");
    while (tok) {
        while (*tok == ' ') tok++;
        size_t len = strlen(tok);
        while (len > 0 && tok[len - 1] == ' ') tok[--len] = '\0';
        if (len > 0) {
            if (out->count == cap) {
                cap *= 2;
                char **next = (char **)realloc(out->items, cap * sizeof(char *));
                if (!next) { free(buf); return 1; }
                out->items = next;
            }
            out->items[out->count] = dupstr(tok);
            if (!out->items[out->count]) { free(buf); return 1; }
            out->count++;
        }
        tok = strtok(NULL, ",");
    }
    free(buf);
    return 0;
}

static char *build_path(const char *index_path, const char *name) {
    size_t base_len = strlen(index_path);
    if (base_len >= 10 && strcmp(index_path + base_len - 10, "/index.mmi") == 0) base_len -= 10;
    size_t n = base_len + 1 + strlen(name) + 1;
    char *out = (char *)malloc(n);
    if (!out) return NULL;
    memcpy(out, index_path, base_len);
    out[base_len] = '/';
    strcpy(out + base_len + 1, name);
    return out;
}

static int parse_json_string_array(const char *path, str_list *out) {
    memset(out, 0, sizeof(*out));
    FILE *f = fopen(path, "rb");
    if (!f) return 1;
    char *buf = (char *)malloc(MAX_LINE);
    if (!buf) { fclose(f); return 1; }
    size_t n = fread(buf, 1, MAX_LINE - 1, f);
    fclose(f);
    buf[n] = '\0';
    size_t cap = 64;
    out->items = (char **)calloc(cap, sizeof(char *));
    if (!out->items) { free(buf); return 1; }
    const char *p = buf;
    while (*p && *p != '[') p++;
    if (*p != '[') { free(buf); return 1; }
    p++;
    while (*p) {
        while (*p && (isspace((unsigned char)*p) || *p == ',')) p++;
        if (*p == ']') break;
        if (*p != '"') { free(buf); return 1; }
        p++;
        const char *start = p;
        while (*p && *p != '"') {
            if (*p == '\\' && *(p + 1)) p++;
            p++;
        }
        if (*p != '"') { free(buf); return 1; }
        size_t len = (size_t)(p - start);
        char *s = (char *)malloc(len + 1);
        if (!s) { free(buf); return 1; }
        memcpy(s, start, len);
        s[len] = '\0';
        if (out->count == cap) {
            cap *= 2;
            char **next = (char **)realloc(out->items, cap * sizeof(char *));
            if (!next) { free(s); free(buf); return 1; }
            out->items = next;
        }
        out->items[out->count++] = s;
        p++;
    }
    free(buf);
    return 0;
}

static int find_id(const str_list *dict, const char *key, uint32_t *out) {
    for (size_t i = 0; i < dict->count; i++) {
        if (strcmp(dict->items[i], key) == 0) { *out = (uint32_t)i; return 1; }
    }
    return 0;
}

static int contains_u32(uint32_t *arr, size_t n, uint32_t v) {
    for (size_t i = 0; i < n; i++) if (arr[i] == v) return 1;
    return 0;
}

static const char *normalize_ext_token(const char *ext) {
    if (!ext) return "";
    while (*ext == ' ') ext++;
    if (*ext == '.') ext++;
    return ext;
}

static int resolve_ext_id(const str_list *ext_dict, const char *raw_ext, uint32_t *out_eid) {
    const char *norm = normalize_ext_token(raw_ext);
    if (find_id(ext_dict, norm, out_eid)) return 1;

    size_t n = strlen(norm);
    char *lower = (char *)malloc(n + 1);
    if (!lower) return 0;
    for (size_t i = 0; i < n; i++) lower[i] = (char)tolower((unsigned char)norm[i]);
    lower[n] = '\0';
    int ok = find_id(ext_dict, lower, out_eid);
    free(lower);
    return ok;
}


static void tokenize_text(const char *text, str_list *out_tokens) {
    memset(out_tokens, 0, sizeof(*out_tokens));
    size_t cap = 8;
    out_tokens->items = (char **)calloc(cap, sizeof(char *));
    if (!out_tokens->items) return;
    char cur[256];
    size_t n = 0;
    for (const char *p = text;; p++) {
        int c = (unsigned char)*p;
        if (isalnum(c)) { if (n + 1 < sizeof(cur)) cur[n++] = (char)tolower(c); }
        else {
            if (n > 0) {
                cur[n] = '\0';
                if (out_tokens->count == cap) {
                    cap *= 2;
                    char **next = (char **)realloc(out_tokens->items, cap * sizeof(char *));
                    if (!next) return;
                    out_tokens->items = next;
                }
                out_tokens->items[out_tokens->count++] = dupstr(cur);
                n = 0;
            }
            if (c == '\0') break;
        }
    }
}

static __attribute__((unused)) char *shell_quote_single(const char *s) {
    size_t n = 2;
    for (const char *p = s; *p; p++) { if (*p == '\'') n += 4; else n += 1; }
    char *out = (char *)malloc(n + 1);
    if (!out) return NULL;
    char *w = out;
    *w++ = '\'';
    for (const char *p = s; *p; p++) {
        if (*p == '\'') { *w++ = '\''; *w++ = '\\'; *w++ = '\''; *w++ = '\''; }
        else *w++ = *p;
    }
    *w++ = '\'';
    *w = '\0';
    return out;
}

static int cmp_u32(const void *a, const void *b) {
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return (x < y) ? -1 : (x > y) ? 1 : 0;
}

static int cmp_gid_path_key(const void *key, const void *elem) {
    uint32_t g = *(const uint32_t *)key;
    const gid_path *e = (const gid_path *)elem;
    return (g < e->gid) ? -1 : (g > e->gid) ? 1 : 0;
}

static __attribute__((unused)) char *read_json_string_from_stream(FILE *fp, int *ok) {
    size_t cap = 128, len = 0;
    char *buf = (char *)malloc(cap);
    if (!buf) { *ok = 0; return NULL; }
    int c;
    while ((c = fgetc(fp)) != EOF) {
        if (c == '"') { buf[len] = '\0'; *ok = 1; return buf; }
        if (c == '\\') {
            int e = fgetc(fp);
            if (e == EOF) break;
            char decoded;
            switch (e) { case '"': decoded = '"'; break; case '\\': decoded = '\\'; break;
            case '/': decoded = '/'; break; case 'b': decoded = '\b'; break;
            case 'f': decoded = '\f'; break; case 'n': decoded = '\n'; break;
            case 'r': decoded = '\r'; break; case 't': decoded = '\t'; break;
            default: decoded = (char)e; break; }
            if (len + 2 > cap) { cap *= 2; char *next = (char *)realloc(buf, cap); if (!next) { free(buf); *ok = 0; return NULL; } buf = next; }
            buf[len++] = decoded;
            continue;
        }
        if (len + 2 > cap) { cap *= 2; char *next = (char *)realloc(buf, cap); if (!next) { free(buf); *ok = 0; return NULL; } buf = next; }
        buf[len++] = (char)c;
    }
    free(buf);
    *ok = 0;
    return NULL;
}

static int load_paths_for_gids(const char *paths_bin, const uint32_t *gids,
                               size_t gids_count, gid_path **out_map, size_t *out_count) {
    *out_map = NULL;
    *out_count = 0;
    if (gids_count == 0) return 0;

    uint32_t *work = (uint32_t *)malloc(gids_count * sizeof(uint32_t));
    if (!work) return 1;
    size_t work_n = 0;
    for (size_t i = 0; i < gids_count; i++) work[work_n++] = gids[i];
    qsort(work, work_n, sizeof(uint32_t), cmp_u32);

    size_t uniq_n = 0;
    for (size_t i = 0; i < work_n; i++) {
        if (i == 0 || work[i] != work[i - 1]) work[uniq_n++] = work[i];
    }

    gid_path *map = (gid_path *)calloc(uniq_n, sizeof(gid_path));
    if (!map) { free(work); return 1; }
    for (size_t i = 0; i < uniq_n; i++) map[i].gid = work[i];

    FILE *fp = fopen(paths_bin, "rb");
    if (!fp) { free(work); free(map); return 1; }

    unsigned char header[12];
    if (fread(header, 1, 12, fp) != 12) {
        fclose(fp); free(work); free(map); return 1;
    }

    uint32_t count = (uint32_t)header[8] |
                     ((uint32_t)header[9] << 8) |
                     ((uint32_t)header[10] << 16) |
                     ((uint32_t)header[11] << 24);
    size_t want = 0;

    if (header[0] == 'P' && header[1] == 'T' && header[2] == 'H' && header[3] == '1') {
        for (uint32_t idx = 0; idx < count && want < uniq_n; idx++) {
            unsigned char lenb[4];
            if (fread(lenb, 1, 4, fp) != 4) break;
            uint32_t len = (uint32_t)lenb[0] |
                           ((uint32_t)lenb[1] << 8) |
                           ((uint32_t)lenb[2] << 16) |
                           ((uint32_t)lenb[3] << 24);

            while (want < uniq_n && map[want].gid < idx) want++;
            if (want < uniq_n && map[want].gid == idx) {
                char *value = (char *)malloc((size_t)len + 1);
                if (!value) { fclose(fp); free(work); free_gid_map(map, uniq_n); return 1; }
                if (fread(value, 1, len, fp) != len) {
                    free(value); fclose(fp); free(work); free_gid_map(map, uniq_n); return 1;
                }
                value[len] = '\0';
                map[want].path = value;
                want++;
            } else {
                if (fseek(fp, (long)len, SEEK_CUR) != 0) {
                    fclose(fp); free(work); free_gid_map(map, uniq_n); return 1;
                }
            }
        }
    } else if (header[0] == 'H' && header[1] == 'T' && header[2] == 'A' && header[3] == 'P') {
        unsigned char extra[12];
        if (fread(extra, 1, 12, fp) != 12) {
            fclose(fp); free(work); free_gid_map(map, uniq_n); return 1;
        }

        uint64_t *offsets = (uint64_t *)calloc((size_t)count + 1, sizeof(uint64_t));
        if (!offsets) {
            fclose(fp); free(work); free_gid_map(map, uniq_n); return 1;
        }
        for (uint32_t i = 0; i <= count; i++) {
            unsigned char offb[8];
            if (fread(offb, 1, 8, fp) != 8) {
                free(offsets); fclose(fp); free(work); free_gid_map(map, uniq_n); return 1;
            }
            offsets[i] = (uint64_t)offb[0]
                       | ((uint64_t)offb[1] << 8)
                       | ((uint64_t)offb[2] << 16)
                       | ((uint64_t)offb[3] << 24)
                       | ((uint64_t)offb[4] << 32)
                       | ((uint64_t)offb[5] << 40)
                       | ((uint64_t)offb[6] << 48)
                       | ((uint64_t)offb[7] << 56);
        }

        uint64_t blob_size_u64 = offsets[count];
        if (blob_size_u64 > SIZE_MAX) {
            free(offsets); fclose(fp); free(work); free_gid_map(map, uniq_n); return 1;
        }
        size_t blob_size = (size_t)blob_size_u64;
        unsigned char *blob = (unsigned char *)malloc(blob_size ? blob_size : 1);
        if (!blob) {
            free(offsets); fclose(fp); free(work); free_gid_map(map, uniq_n); return 1;
        }
        if (blob_size > 0 && fread(blob, 1, blob_size, fp) != blob_size) {
            free(blob); free(offsets); fclose(fp); free(work); free_gid_map(map, uniq_n); return 1;
        }

        for (uint32_t idx = 0; idx < count && want < uniq_n; idx++) {
            while (want < uniq_n && map[want].gid < idx) want++;
            if (want >= uniq_n) break;
            if (map[want].gid != idx) continue;

            uint64_t start = offsets[idx];
            uint64_t end = offsets[idx + 1];
            if (start > end || end > blob_size_u64) {
                free(blob); free(offsets); fclose(fp); free(work); free_gid_map(map, uniq_n); return 1;
            }

            size_t len = (size_t)(end - start);
            if (len > 0 && blob[end - 1] == '\0') len--;
            char *value = (char *)malloc(len + 1);
            if (!value) {
                free(blob); free(offsets); fclose(fp); free(work); free_gid_map(map, uniq_n); return 1;
            }
            memcpy(value, blob + start, len);
            value[len] = '\0';
            map[want].path = value;
            want++;
        }

        free(blob);
        free(offsets);
    } else {
        fclose(fp); free(work); free(map); return 1;
    }

    fclose(fp);
    free(work);
    *out_map = map;
    *out_count = uniq_n;
    return 0;
}


static void free_gid_map(gid_path *map, size_t count) {
    if (!map) return;
    for (size_t i = 0; i < count; i++) free(map[i].path);
    free(map);
}

static const char *find_path_in_map(gid_path *map, size_t count, uint32_t gid) {
    if (!map || count == 0) return NULL;
    gid_path *hit = (gid_path *)bsearch(&gid, map, count, sizeof(gid_path), cmp_gid_path_key);
    return hit ? hit->path : NULL;
}

static void print_json_escaped(const char *s) {
    putchar('"');
    if (s) {
        for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
            switch (*p) {
                case '"': fputs("\\\"", stdout); break;
                case '\\': fputs("\\\\", stdout); break;
                case '\b': fputs("\\b", stdout); break;
                case '\f': fputs("\\f", stdout); break;
                case '\n': fputs("\\n", stdout); break;
                case '\r': fputs("\\r", stdout); break;
                case '\t': fputs("\\t", stdout); break;
                default:
                    if (*p < 0x20) printf("\\u%04x", *p);
                    else putchar(*p);
                    break;
            }
        }
    }
    putchar('"');
}

static enum sort_mode parse_sort_mode(const char *s) {
    if (!s || !*s) return SORT_NONE;
    if (strcmp(s, "size_asc")  == 0) return SORT_SIZE_ASC;
    if (strcmp(s, "size_desc") == 0) return SORT_SIZE_DESC;
    if (strcmp(s, "path_asc")  == 0) return SORT_PATH_ASC;
    return SORT_NONE;
}

static int cmp_doc_key_size_asc(const void *a, const void *b) {
    const doc_key *x = (const doc_key *)a, *y = (const doc_key *)b;
    if (x->size < y->size) return -1;
    if (x->size > y->size) return  1;
    return (x->idx < y->idx) ? -1 : (x->idx > y->idx) ? 1 : 0;
}
static int cmp_doc_key_size_desc(const void *a, const void *b) {
    return cmp_doc_key_size_asc(b, a);
}
static const char *find_path_in_map(gid_path *map, size_t count, uint32_t gid);
static int cmp_doc_key_path_asc(const void *a, const void *b) {
    const doc_key *x = (const doc_key *)a, *y = (const doc_key *)b;
    const char *px = find_path_in_map(g_sort_path_map, g_sort_path_map_n, x->gid);
    const char *py = find_path_in_map(g_sort_path_map, g_sort_path_map_n, y->gid);
    if (px && py) {
        int cmp = strcmp(px, py);
        if (cmp != 0) return cmp;
    } else if (px && !py) {
        return -1;
    } else if (!px && py) {
        return 1;
    }
    return (x->idx < y->idx) ? -1 : (x->idx > y->idx) ? 1 : 0;
}

static unsigned int parse_fields_mask(const char *s) {
    unsigned int mask = 0;
    if (!s || !*s) return F_ALL;
    char *buf = dupstr(s);
    if (!buf) return F_ALL;
    char *tok = strtok(buf, ",");
    while (tok) {
        while (*tok == ' ') tok++;
        size_t len = strlen(tok);
        while (len > 0 && tok[len - 1] == ' ') tok[--len] = '\0';
        if      (strcmp(tok, "doc_id") == 0) mask |= F_DOC_ID;
        else if (strcmp(tok, "gid") == 0)    mask |= F_GID;
        else if (strcmp(tok, "uid") == 0)    mask |= F_UID;
        else if (strcmp(tok, "size") == 0)    mask |= F_SIZE;
        else if (strcmp(tok, "eid") == 0)     mask |= F_EID;
        else if (strcmp(tok, "sid") == 0)     mask |= F_SID;
        else if (strcmp(tok, "path") == 0)    mask |= F_PATH;
        else if (strcmp(tok, "ext") == 0)     mask |= F_EXT;
        else if (strcmp(tok, "user") == 0)    mask |= F_USER;
        tok = strtok(NULL, ",");
    }
    free(buf);
    return mask ? mask : F_ALL;
}

/* Print a single doc JSON object, only selected fields, no trailing comma */
static void print_doc(const cdx1_doc_ref *d, unsigned int mask,
                      gid_path *path_map, size_t path_map_n,
                      const str_list *ext_dict, const str_list *user_dict) {
    putchar('{');
    int first = 1;
    if (mask & F_DOC_ID) {
        printf("%s\"doc_id\":%u", first ? "" : ",", d->doc_id);
        first = 0;
    }
    if (mask & F_GID) {
        printf("%s\"gid\":%u", first ? "" : ",", d->gid);
        first = 0;
    }
    if (mask & F_UID) {
        printf("%s\"uid\":%u", first ? "" : ",", d->uid);
        first = 0;
    }
    if (mask & F_SIZE) {
        printf("%s\"size\":%llu", first ? "" : ",", (unsigned long long)d->size);
        first = 0;
    }
    if (mask & F_EID) {
        printf("%s\"eid\":%u", first ? "" : ",", d->eid);
        first = 0;
    }
    if (mask & F_SID) {
        printf("%s\"sid\":%u", first ? "" : ",", d->sid);
        first = 0;
    }
    if (mask & F_PATH) {
        printf("%s\"path\":", first ? "" : ",");
        const char *path = find_path_in_map(path_map, path_map_n, d->gid);
        if (path) print_json_escaped(path);
        else printf("null");
        first = 0;
    }
    if (mask & F_EXT) {
        printf("%s\"ext\":", first ? "" : ",");
        const char *ext_name = (d->eid < ext_dict->count) ? ext_dict->items[d->eid] : NULL;
        if (ext_name) print_json_escaped(ext_name);
        else printf("null");
        first = 0;
    }
    if (mask & F_USER) {
        printf("%s\"user\":", first ? "" : ",");
        const char *user_name = (d->uid < user_dict->count) ? user_dict->items[d->uid] : NULL;
        if (user_name) print_json_escaped(user_name);
        else printf("null");
        first = 0;
    }
    putchar('}');
}

int main(int argc, char **argv) {
    if (argc < 2) { usage(argv[0]); return 2; }

    const char *index_input = argv[1];
    const char *kw_csv = NULL, *ext_csv = NULL, *user_csv = NULL;
    uint64_t size_min = 0, size_max = 0;
    int has_min = 0, has_max = 0;
    size_t limit = 0, offset = 0;
    int output_json = 0, output_docs = 0;
    unsigned int fields_mask = 0;
    enum sort_mode sort_mode = SORT_NONE;

    for (int i = 2; i < argc; i++) {
        if      (strcmp(argv[i], "--kw") == 0 && i + 1 < argc)    kw_csv = argv[++i];
        else if (strcmp(argv[i], "--ext") == 0 && i + 1 < argc)    ext_csv = argv[++i];
        else if (strcmp(argv[i], "--user") == 0 && i + 1 < argc)   user_csv = argv[++i];
        else if (strcmp(argv[i], "--min") == 0 && i + 1 < argc)    { size_min = strtoull(argv[++i], NULL, 10); has_min = 1; }
        else if (strcmp(argv[i], "--max") == 0 && i + 1 < argc)    { size_max = strtoull(argv[++i], NULL, 10); has_max = 1; }
        else if (strcmp(argv[i], "--limit") == 0 && i + 1 < argc)  limit = (size_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--json") == 0)                   output_json = 1;
        else if (strcmp(argv[i], "--docs") == 0)                  output_docs = 1;
        else if (strcmp(argv[i], "--fields") == 0 && i + 1 < argc) fields_mask = parse_fields_mask(argv[++i]);
        else if (strcmp(argv[i], "--offset") == 0 && i + 1 < argc) offset = (size_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--sort") == 0 && i + 1 < argc) sort_mode = parse_sort_mode(argv[++i]);
        else { usage(argv[0]); return 2; }
    }

    char *mmi_path = NULL;
    if (strlen(index_input) >= 10 && strcmp(index_input + strlen(index_input) - 10, "/index.mmi") == 0) mmi_path = dupstr(index_input);
    else mmi_path = build_path(index_input, "index.mmi");
    if (!mmi_path) return 1;

    char *tokens_path = build_path(index_input, "tokens.json");
    char *exts_path = build_path(index_input, "exts.json");
    char *users_path = build_path(index_input, "users.json");
    char *paths_bin_path = build_path(index_input, "../index_seed/paths.bin");
    if (!tokens_path || !exts_path || !users_path || !paths_bin_path) return 1;

    str_list token_dict, ext_dict, user_dict;
    if (parse_json_string_array(tokens_path, &token_dict) != 0 ||
        parse_json_string_array(exts_path, &ext_dict) != 0 ||
        parse_json_string_array(users_path, &user_dict) != 0) {
        fprintf(stderr, "failed to parse dictionaries in index dir\n");
        return 1;
    }

    str_list kws, exts, users;
    if (split_csv(kw_csv ? kw_csv : "", &kws) != 0 ||
        split_csv(ext_csv ? ext_csv : "", &exts) != 0 ||
        split_csv(user_csv ? user_csv : "", &users) != 0) {
        fprintf(stderr, "failed to parse filters\n"); return 1;
    }

    uint32_t *token_ids = (uint32_t *)calloc(kws.count * 4 + 1, sizeof(uint32_t));
    uint32_t *ext_ids   = (uint32_t *)calloc(exts.count + 1, sizeof(uint32_t));
    uint32_t *user_ids  = (uint32_t *)calloc(users.count + 1, sizeof(uint32_t));
    if (!token_ids || !ext_ids || !user_ids) return 1;
    size_t token_n = 0, ext_n = 0, user_n = 0;

    for (size_t i = 0; i < kws.count; i++) {
        str_list toks; tokenize_text(kws.items[i], &toks);
        for (size_t j = 0; j < toks.count; j++) {
            uint32_t tid = 0;
            if (find_id(&token_dict, toks.items[j], &tid) && !contains_u32(token_ids, token_n, tid)) token_ids[token_n++] = tid;
        }
        free_str_list(&toks);
    }
    for (size_t i = 0; i < exts.count; i++) {
        uint32_t eid = 0;
        if (resolve_ext_id(&ext_dict, exts.items[i], &eid) && !contains_u32(ext_ids, ext_n, eid)) ext_ids[ext_n++] = eid;
    }
    for (size_t i = 0; i < users.count; i++) {
        uint32_t uid = 0;
        if (find_id(&user_dict, users.items[i], &uid) && !contains_u32(user_ids, user_n, uid)) user_ids[user_n++] = uid;
    }

    cdx1_index index;
    int err = cdx1_open(mmi_path, &index);
    if (err != 0) { fprintf(stderr, "open index failed: %d\n", err); return 1; }

    cdx1_query query;
    memset(&query, 0, sizeof(query));
    query.token_ids = token_n ? token_ids : NULL; query.token_count = token_n;
    query.ext_ids   = ext_n   ? ext_ids   : NULL; query.ext_count   = ext_n;
    query.user_ids  = user_n  ? user_ids  : NULL; query.user_count  = user_n;
    query.size_min = size_min; query.size_max = size_max;
    query.has_size_min = has_min; query.has_size_max = has_max;

    cdx1_docset out;
    err = cdx1_query_docs(&index, &query, &out);
    if (err != 0) { fprintf(stderr, "query failed: %d\n", err); cdx1_close(&index); return 1; }

    size_t matched = out.count;
    doc_key *keys = NULL;
    if (matched > 0) {
        keys = (doc_key *)calloc(matched, sizeof(doc_key));
        if (!keys) { cdx1_free_docset(&out); cdx1_close(&index); return 1; }
        for (size_t i = 0; i < matched; i++) {
            uint32_t id = out.doc_ids[i];
            keys[i].idx = id;
            if (id < index.doc_count) {
                keys[i].gid = index.docs[id].gid;
                keys[i].size = index.docs[id].size;
            }
        }
        if (sort_mode == SORT_SIZE_ASC) qsort(keys, matched, sizeof(doc_key), cmp_doc_key_size_asc);
        else if (sort_mode == SORT_SIZE_DESC) qsort(keys, matched, sizeof(doc_key), cmp_doc_key_size_desc);
    }

    size_t start = (offset < matched) ? offset : matched;
    size_t remain = (start < matched) ? (matched - start) : 0;
    size_t emit = (limit > 0 && remain > limit) ? limit : remain;

    /* Load paths when needed: docs.path output OR path_asc sorting */
    gid_path *path_map = NULL;
    size_t path_map_n = 0;
    unsigned int effective_mask = output_docs ? (fields_mask ? fields_mask : F_ALL) : 0;
    int need_path_for_docs = output_json && output_docs && (effective_mask & F_PATH) && emit > 0;
    int need_path_for_sort = (sort_mode == SORT_PATH_ASC && matched > 0);
    if (need_path_for_docs || need_path_for_sort) {
        size_t gid_n = need_path_for_sort ? matched : emit;
        uint32_t *gids = (uint32_t *)calloc(gid_n, sizeof(uint32_t));
        if (gids) {
            if (need_path_for_sort) {
                for (size_t i = 0; i < matched; i++) gids[i] = keys[i].gid;
            } else {
                for (size_t i = 0; i < emit; i++) gids[i] = keys[start + i].gid;
            }
            load_paths_for_gids(paths_bin_path, gids, gid_n, &path_map, &path_map_n);
            free(gids);
        }
    }

    if (sort_mode == SORT_PATH_ASC && matched > 0 && path_map_n > 0) {
        g_sort_path_map = path_map;
        g_sort_path_map_n = path_map_n;
        qsort(keys, matched, sizeof(doc_key), cmp_doc_key_path_asc);
        g_sort_path_map = NULL;
        g_sort_path_map_n = 0;
        start = (offset < matched) ? offset : matched;
        remain = (start < matched) ? (matched - start) : 0;
        emit = (limit > 0 && remain > limit) ? limit : remain;
    }

    if (output_json) {
        printf("{\"matched\":%zu,\"returned\":%zu,\"doc_ids\":[", matched, emit);
        for (size_t i = 0; i < emit; i++) { if (i) putchar(','); printf("%u", keys[start + i].idx); }
        putchar(']');

        if (output_docs && emit > 0) {
            printf(",\"docs\":[");
            for (size_t i = 0; i < emit; i++) {
                uint32_t id = keys[start + i].idx;
                if (i) putchar(',');
                if (id < index.doc_count) print_doc(&index.docs[id], effective_mask, path_map, path_map_n, &ext_dict, &user_dict);
                else { printf("{\"doc_id\":%u}", id); }
            }
            putchar(']');
        }
        puts("}");
    } else {
        printf("matched docs: %zu\n", matched);
        for (size_t i = 0; i < emit; i++) printf("%u\n", keys[start + i].idx);
    }

    cdx1_free_docset(&out);
    cdx1_close(&index);
    free_gid_map(path_map, path_map_n);
    free(keys);
    free_str_list(&token_dict); free_str_list(&ext_dict); free_str_list(&user_dict);
    free_str_list(&kws); free_str_list(&exts); free_str_list(&users);
    free(token_ids); free(ext_ids); free(user_ids);
    free(mmi_path); free(tokens_path); free(exts_path); free(users_path); free(paths_bin_path);
    return 0;
}
