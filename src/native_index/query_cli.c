#define _GNU_SOURCE

#include <ctype.h>
#include <dirent.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define F_DOC_ID 0x001U
#define F_GID 0x002U
#define F_UID 0x004U
#define F_SIZE 0x008U
#define F_EID 0x010U
#define F_SID 0x020U
#define F_PATH 0x040U
#define F_EXT 0x080U
#define F_USER 0x100U
#define F_ALL 0x1FFU

enum sort_mode { SORT_NONE, SORT_SIZE_ASC, SORT_SIZE_DESC, SORT_PATH_ASC };

typedef struct {
    char **items;
    size_t count;
} str_list;

typedef struct {
    uint32_t doc_id;
    uint32_t gid;
    uint32_t uid;
    uint64_t size;
    uint32_t eid;
    uint32_t sid;
} doc_ref;

typedef struct {
    doc_ref *items;
    size_t count;
    size_t cap;
} doc_list;

typedef struct {
    char *id;
    uint32_t gid;
    int has_gid;
    char *pid;
    char *name;
    char *owner;
    char *path;
    char *type;
    uint64_t value;
    int has_children;
} treemap_row;

typedef struct {
    treemap_row *items;
    size_t count;
    size_t cap;
} treemap_rows;

static char **g_paths = NULL;
static size_t g_paths_n = 0;
static int g_treemap_paths_loaded = 0;

static const char *treemap_row_path(const treemap_row *r);
static void clear_global_paths(void);
static int load_treemap_path_dict_seek(const char *data_dir);
static int load_detail_path_dict(const char *detail_root);
static void clear_treemap_runtime_state(void);

static char *json_get_str(const char *line, const char *key);
static void free_str_arr(char **arr, size_t count);
static int doc_list_push(doc_list *lst, const doc_ref *d);
static int json_get_bool(const char *line, const char *key, int *out);
static int treemap_rows_push(treemap_rows *lst, const treemap_row *r);
static void free_treemap_rows(treemap_rows *lst);
static int load_treemap_rows(const char *detail_root, treemap_rows *rows);
static char *parent_path_dup(const char *path);

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s <detail_users|detail_users/index>\n"
            "  [--kw a,b] [--ext .txt,.log] [--user u1,u2] [--type dir|file]\n"
            "  [--min n] [--max n] [--limit n] [--offset n] [--page n]\n"
            "  [--sort size_asc|size_desc|path_asc]\n"
            "  [--json] [--docs] [--fields doc_id,gid,path,user,...] [--stats]\n",
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
    if (!out->items) {
        free(buf);
        return 1;
    }
    char *tok = strtok(buf, ",");
    while (tok) {
        while (*tok == ' ') tok++;
        size_t len = strlen(tok);
        while (len > 0 && tok[len - 1] == ' ') tok[--len] = '\0';
        if (len > 0) {
            if (out->count == cap) {
                cap *= 2;
                char **next = (char **)realloc(out->items, cap * sizeof(char *));
                if (!next) {
                    free(buf);
                    return 1;
                }
                out->items = next;
            }
            out->items[out->count] = dupstr(tok);
            if (!out->items[out->count]) {
                free(buf);
                return 1;
            }
            out->count++;
        }
        tok = strtok(NULL, ",");
    }
    free(buf);
    return 0;
}

static int split_pipe_terms(const char *s, str_list *out) {
    memset(out, 0, sizeof(*out));
    if (!s || !*s) return 0;
    char *buf = dupstr(s);
    if (!buf) return 1;
    size_t cap = 8;
    out->items = (char **)calloc(cap, sizeof(char *));
    if (!out->items) {
        free(buf);
        return 1;
    }
    char *tok = strtok(buf, "|");
    while (tok) {
        while (*tok == ' ') tok++;
        size_t len = strlen(tok);
        while (len > 0 && tok[len - 1] == ' ') tok[--len] = '\0';
        if (len > 0) {
            if (out->count == cap) {
                cap *= 2;
                char **next = (char **)realloc(out->items, cap * sizeof(char *));
                if (!next) {
                    free(buf);
                    return 1;
                }
                out->items = next;
            }
            out->items[out->count] = dupstr(tok);
            if (!out->items[out->count]) {
                free(buf);
                return 1;
            }
            out->count++;
        }
        tok = strtok(NULL, "|");
    }
    free(buf);
    return 0;
}

static char *build_path2(const char *base, const char *name) {
    size_t n = strlen(base) + 1 + strlen(name) + 1;
    char *out = (char *)malloc(n);
    if (!out) return NULL;
    snprintf(out, n, "%s/%s", base, name);
    return out;
}

static char *resolve_detail_root(const char *input) {
    size_t n = strlen(input);
    if (n >= 6 && strcmp(input + n - 6, "/index") == 0) {
        char *out = (char *)malloc(n - 6 + 1);
        if (!out) return NULL;
        memcpy(out, input, n - 6);
        out[n - 6] = '\0';
        return out;
    }
    if (n >= 10 && strcmp(input + n - 10, "/index.mmi") == 0) {
        char *out = (char *)malloc(n - 10 + 1);
        if (!out) return NULL;
        memcpy(out, input, n - 10);
        out[n - 10] = '\0';
        return out;
    }
    return dupstr(input);
}

static char *read_file_all(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        return NULL;
    }
    long sz = ftell(f);
    if (sz < 0) {
        fclose(f);
        return NULL;
    }
    if (fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        return NULL;
    }
    char *buf = (char *)malloc((size_t)sz + 1);
    if (!buf) {
        fclose(f);
        return NULL;
    }
    size_t n = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[n] = '\0';
    return buf;
}

static int is_text_layout(const char *detail_root) {
    char *manifest = build_path2(detail_root, "manifest.json");
    if (!manifest) return 0;
    char *buf = read_file_all(manifest);
    free(manifest);
    if (!buf) return 0;
    int ok = strstr(buf, "check-disk-detail\"") != NULL;
    free(buf);
    return ok;
}

static int is_treemap_layout(const char *detail_root) {
    char *manifest = build_path2(detail_root, "manifest.json");
    if (manifest) {
        char *buf = read_file_all(manifest);
        free(manifest);
        if (buf) {
            int ok = strstr(buf, "check-disk-detail-treemap") != NULL;
            free(buf);
            if (ok) return 1;
        }
    }
    /* Fallback: check tree_map_data/manifest.json alongside current root */
    size_t tm_n = strlen(detail_root) + strlen("/tree_map_data/manifest.json") + 1;
    char *tm_manifest = (char *)malloc(tm_n);
    if (!tm_manifest) return 0;
    snprintf(tm_manifest, tm_n, "%s/tree_map_data/manifest.json", detail_root);
    char *buf = read_file_all(tm_manifest);
    free(tm_manifest);
    if (!buf) return 0;
    int ok = strstr(buf, "check-disk-detail-treemap") != NULL;
    free(buf);
    return ok;
}

static char *safe_user_dir(const char *user) {
    size_t n = strlen(user);
    char *out = (char *)malloc(n + 1);
    if (!out) return NULL;
    for (size_t i = 0; i < n; i++) {
        unsigned char ch = (unsigned char)user[i];
        out[i] = (isalnum(ch) || ch == '-' || ch == '_' || ch == '.') ? (char)ch : '_';
    }
    out[n] = '\0';
    return out;
}

static int ensure_str_slot(char ***arr, size_t *cap, size_t id) {
    if (id < *cap) return 1;
    size_t old = *cap;
    size_t next_cap = *cap ? *cap : 16;
    while (id >= next_cap) next_cap = next_cap * 2 + 16;
    char **next = (char **)realloc(*arr, next_cap * sizeof(char *));
    if (!next) return 0;
    memset(next + old, 0, (next_cap - old) * sizeof(char *));
    *arr = next;
    *cap = next_cap;
    return 1;
}

static __attribute__((unused)) int ensure_dict_id(char ***arr, size_t *count, size_t *cap, const char *value, uint32_t *out_id) {
    for (size_t i = 0; i < *count; i++) {
        if ((*arr)[i] && strcmp((*arr)[i], value) == 0) {
            *out_id = (uint32_t)i;
            return 1;
        }
    }
    if (!ensure_str_slot(arr, cap, *count)) return 0;
    (*arr)[*count] = dupstr(value);
    if (!(*arr)[*count]) return 0;
    *out_id = (uint32_t)(*count);
    (*count)++;
    return 1;
}

static __attribute__((unused)) const char *skip_ws(const char *p) {
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    return p;
}

static int append_filter_doc(doc_list *docs, uint32_t doc_id, uint32_t gid, uint32_t uid, uint32_t eid, uint64_t size) {
    doc_ref d;
    memset(&d, 0, sizeof(d));
    d.doc_id = doc_id;
    d.gid = gid;
    d.uid = uid;
    d.eid = eid;
    d.size = size;
    d.sid = 0;
    return doc_list_push(docs, &d);
}

static int load_user_file_parts(const char *user_dir, char ***parts_out, size_t *parts_n) {
    *parts_out = NULL;
    *parts_n = 0;
    char *manifest_path = build_path2(user_dir, "manifest.json");
    if (!manifest_path) return 0;
    char *buf = read_file_all(manifest_path);
    free(manifest_path);
    if (!buf) return 0;

    char *files_sec = strstr(buf, "\"files\"");
    if (!files_sec) {
        free(buf);
        return 1;
    }
    char *parts_sec = strstr(files_sec, "\"parts\"");
    if (!parts_sec) {
        free(buf);
        return 1;
    }
    char *arr = strchr(parts_sec, '[');
    char *arr_end = arr ? strchr(arr, ']') : NULL;
    if (!arr || !arr_end) {
        free(buf);
        return 1;
    }

    size_t cap = 4;
    char **parts = (char **)calloc(cap, sizeof(char *));
    if (!parts) {
        free(buf);
        return 0;
    }
    const char *p = arr;
    while ((p = strstr(p, "\"path\"")) && p < arr_end) {
        char *path = json_get_str(p, "path");
        if (!path) {
            p += 6;
            continue;
        }
        if (*parts_n == cap) {
            cap *= 2;
            char **next = (char **)realloc(parts, cap * sizeof(char *));
            if (!next) {
                free(path);
                free_str_arr(parts, *parts_n);
                free(buf);
                return 0;
            }
            parts = next;
        }
        parts[*parts_n] = path;
        (*parts_n)++;
        p += 6;
    }
    *parts_out = parts;
    free(buf);
    return 1;
}

static int load_root_users(const char *detail_root, char ***users_out, size_t *users_n) {
    *users_out = NULL;
    *users_n = 0;
    char *manifest_path = build_path2(detail_root, "manifest.json");
    if (!manifest_path) return 0;
    char *buf = read_file_all(manifest_path);
    free(manifest_path);
    if (!buf) return 0;

    size_t cap = 8;
    char **users = (char **)calloc(cap, sizeof(char *));
    if (!users) {
        free(buf);
        return 0;
    }
    const char *p = buf;
    while ((p = strstr(p, "\"username\"")) != NULL) {
        char *name = json_get_str(p, "username");
        if (!name) {
            p += 10;
            continue;
        }
        if (*users_n == cap) {
            cap *= 2;
            char **next = (char **)realloc(users, cap * sizeof(char *));
            if (!next) {
                free(name);
                free_str_arr(users, *users_n);
                free(buf);
                return 0;
            }
            users = next;
        }
        users[*users_n] = name;
        (*users_n)++;
        p += 10;
    }
    *users_out = users;
    free(buf);
    return 1;
}

static const char *json_find_key(const char *line, const char *key) {
    char pat[128];
    snprintf(pat, sizeof(pat), "\"%s\"", key);
    size_t pat_len = strlen(pat);
    const char *p = line;
    while ((p = strstr(p, pat)) != NULL) {
        const char *q = p + pat_len;
        while (*q == ' ' || *q == '\t') q++;
        if (*q == ':') {
            q++;
            while (*q == ' ' || *q == '\t') q++;
            return q;
        }
        p = p + 1;
    }
    return NULL;
}

static int json_get_u64(const char *line, const char *key, uint64_t *out) {
    const char *p = json_find_key(line, key);
    if (!p) return 0;
    char *end = NULL;
    unsigned long long v = strtoull(p, &end, 10);
    if (end == p) return 0;
    *out = (uint64_t)v;
    return 1;
}

static int json_get_bool(const char *line, const char *key, int *out) {
    const char *p = json_find_key(line, key);
    if (!p) return 0;
    if (strncmp(p, "true", 4) == 0) {
        *out = 1;
        return 1;
    }
    if (strncmp(p, "false", 5) == 0) {
        *out = 0;
        return 1;
    }
    return 0;
}

static __attribute__((unused)) int json_get_u32(const char *line, const char *key, uint32_t *out) {
    uint64_t v = 0;
    if (!json_get_u64(line, key, &v)) return 0;
    *out = (uint32_t)v;
    return 1;
}

static char *json_get_str(const char *line, const char *key) {
    const char *p = json_find_key(line, key);
    if (!p || *p != '"') return NULL;
    p++;
    size_t cap = 64, len = 0;
    char *out = (char *)malloc(cap);
    if (!out) return NULL;
    while (*p && *p != '"') {
        char ch = *p;
        if (ch == '\\' && p[1]) {
            p++;
            switch (*p) {
                case 'n': ch = '\n'; break;
                case 'r': ch = '\r'; break;
                case 't': ch = '\t'; break;
                case 'b': ch = '\b'; break;
                case 'f': ch = '\f'; break;
                default: ch = *p; break;
            }
        }
        if (len + 2 > cap) {
            cap *= 2;
            char *next = (char *)realloc(out, cap);
            if (!next) {
                free(out);
                return NULL;
            }
            out = next;
        }
        out[len++] = ch;
        p++;
    }
    out[len] = '\0';
    return out;
}

static int find_id_exact(char **dict, size_t count, const char *key, uint32_t *out) {
    for (size_t i = 0; i < count; i++) {
        if (dict[i] && strcmp(dict[i], key) == 0) {
            *out = (uint32_t)i;
            return 1;
        }
    }
    return 0;
}

static int contains_u32(uint32_t *arr, size_t n, uint32_t v) {
    for (size_t i = 0; i < n; i++) if (arr[i] == v) return 1;
    return 0;
}

static char *norm_lower_token(const char *s, int drop_dot) {
    while (*s == ' ') s++;
    size_t n = strlen(s);
    while (n > 0 && s[n - 1] == ' ') n--;
    size_t start = 0;
    if (drop_dot) {
        while (start < n && s[start] == '.') start++;
    }
    char *out = (char *)malloc(n - start + 1);
    if (!out) return NULL;
    size_t w = 0;
    for (size_t i = start; i < n; i++) out[w++] = (char)tolower((unsigned char)s[i]);
    out[w] = '\0';
    return out;
}

static int cmp_doc_size_asc(const void *a, const void *b) {
    const doc_ref *x = (const doc_ref *)a;
    const doc_ref *y = (const doc_ref *)b;
    if (x->size < y->size) return -1;
    if (x->size > y->size) return 1;
    return (x->doc_id < y->doc_id) ? -1 : (x->doc_id > y->doc_id);
}

static int cmp_doc_size_desc(const void *a, const void *b) {
    return cmp_doc_size_asc(b, a);
}

static int cmp_doc_path_asc(const void *a, const void *b) {
    const doc_ref *x = (const doc_ref *)a;
    const doc_ref *y = (const doc_ref *)b;
    const char *px = (x->gid < g_paths_n) ? g_paths[x->gid] : NULL;
    const char *py = (y->gid < g_paths_n) ? g_paths[y->gid] : NULL;
    if (px && py) {
        int c = strcmp(px, py);
        if (c != 0) return c;
    } else if (px && !py) {
        return -1;
    } else if (!px && py) {
        return 1;
    }
    return (x->doc_id < y->doc_id) ? -1 : (x->doc_id > y->doc_id);
}

static enum sort_mode parse_sort_mode(const char *s) {
    if (!s || !*s) return SORT_NONE;
    if (strcmp(s, "size_asc") == 0) return SORT_SIZE_ASC;
    if (strcmp(s, "size_desc") == 0) return SORT_SIZE_DESC;
    if (strcmp(s, "path_asc") == 0) return SORT_PATH_ASC;
    return SORT_NONE;
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
        if (strcmp(tok, "doc_id") == 0) mask |= F_DOC_ID;
        else if (strcmp(tok, "gid") == 0) mask |= F_GID;
        else if (strcmp(tok, "uid") == 0) mask |= F_UID;
        else if (strcmp(tok, "size") == 0) mask |= F_SIZE;
        else if (strcmp(tok, "eid") == 0) mask |= F_EID;
        else if (strcmp(tok, "sid") == 0) mask |= F_SID;
        else if (strcmp(tok, "path") == 0) mask |= F_PATH;
        else if (strcmp(tok, "ext") == 0) mask |= F_EXT;
        else if (strcmp(tok, "user") == 0) mask |= F_USER;
        tok = strtok(NULL, ",");
    }
    free(buf);
    return mask ? mask : F_ALL;
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

static void free_str_arr(char **arr, size_t count) {
    if (!arr) return;
    for (size_t i = 0; i < count; i++) free(arr[i]);
    free(arr);
}

static void clear_global_paths(void) {
    if (g_paths) {
        free_str_arr(g_paths, g_paths_n);
    }
    g_paths = NULL;
    g_paths_n = 0;
    g_treemap_paths_loaded = 0;
}

static void clear_treemap_runtime_state(void) {
    clear_global_paths();
}

static const char *treemap_row_path(const treemap_row *r) {
    if (!r) return NULL;
    if (r->has_gid && g_treemap_paths_loaded && r->gid < g_paths_n && g_paths[r->gid]) return g_paths[r->gid];
    if (r->path && *r->path) return r->path;
    return NULL;
}


static int load_treemap_path_dict_seek(const char *data_dir) {
    char *seek_path = build_path2(data_dir, "api/path_dict.seek");
    if (!seek_path) return 0;
    FILE *sf = fopen(seek_path, "rb");
    free(seek_path);
    if (!sf) return 0;

    char magic[4];
    if (fread(magic, 1, 4, sf) != 4 || memcmp(magic, "PDX1", 4) != 0) {
        fclose(sf);
        return 0;
    }

    uint32_t version = 0;
    uint32_t count = 0;
    if (fread(&version, sizeof(uint32_t), 1, sf) != 1 || fread(&count, sizeof(uint32_t), 1, sf) != 1) {
        fclose(sf);
        return 0;
    }
    if (version != 1) {
        fclose(sf);
        return 0;
    }

    char *dict_path = build_path2(data_dir, "api/path_dict.ndjson");
    if (!dict_path) {
        fclose(sf);
        return 0;
    }
    FILE *df = fopen(dict_path, "rb");
    free(dict_path);
    if (!df) {
        fclose(sf);
        return 0;
    }

    clear_global_paths();

    int loaded = 0;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t gid = 0;
        uint64_t off = 0;
        uint32_t len = 0;
        if (fread(&gid, sizeof(uint32_t), 1, sf) != 1 ||
            fread(&off, sizeof(uint64_t), 1, sf) != 1 ||
            fread(&len, sizeof(uint32_t), 1, sf) != 1) {
            break;
        }
        if (len == 0 || len > (16U * 1024U * 1024U)) continue;
        if (!ensure_str_slot(&g_paths, &g_paths_n, gid)) continue;

        if (fseeko(df, (off_t)off, SEEK_SET) != 0) continue;
        char *line = (char *)malloc((size_t)len + 1);
        if (!line) continue;
        size_t got = fread(line, 1, (size_t)len, df);
        if (got == 0) { free(line); continue; }
        line[got] = '\0';
        while (got > 0 && (line[got - 1] == '\n' || line[got - 1] == '\r')) line[--got] = '\0';

        char *path = json_get_str(line, "p");
        free(line);
        if (!path) continue;

        free(g_paths[gid]);
        g_paths[gid] = path;
        loaded++;
    }

    fclose(df);
    fclose(sf);
    g_treemap_paths_loaded = loaded > 0;
    return loaded > 0;
}

static int load_treemap_path_dict(const char *data_dir) {
    if (load_treemap_path_dict_seek(data_dir)) return 1;

    char *dict_path = build_path2(data_dir, "api/path_dict.ndjson");
    if (!dict_path) return 0;
    FILE *f = fopen(dict_path, "r");
    free(dict_path);
    if (!f) return 0;

    clear_global_paths();

    char *line = NULL;
    size_t line_cap = 0;
    ssize_t nread;
    int loaded = 0;
    while ((nread = getline(&line, &line_cap, f)) > 0) {
        if (nread > 0 && line[nread - 1] == '\n') line[nread - 1] = '\0';
        if (!*line) continue;
        uint32_t gid = 0;
        if (!json_get_u32(line, "gid", &gid)) continue;
        char *path = json_get_str(line, "p");
        if (!path) continue;
        if (!ensure_str_slot(&g_paths, &g_paths_n, gid)) {
            free(path);
            continue;
        }
        free(g_paths[gid]);
        g_paths[gid] = path;
        loaded++;
    }
    free(line);
    fclose(f);
    g_treemap_paths_loaded = loaded > 0;
    return loaded > 0;
}


static int load_detail_path_dict(const char *detail_root) {
    char seek_path[4096], ndjson_path[4096];
    snprintf(seek_path, sizeof(seek_path), "%s/api/path_dict.seek", detail_root);
    snprintf(ndjson_path, sizeof(ndjson_path), "%s/api/path_dict.ndjson", detail_root);

    FILE *sf = fopen(seek_path, "rb");
    FILE *df = fopen(ndjson_path, "rb");
    if (sf && df) {
        char magic[4];
        uint32_t version, count;
        if (fread(magic, 1, 4, sf) == 4 && memcmp(magic, "PDX1", 4) == 0 &&
            fread(&version, 4, 1, sf) == 1 && version == 1 &&
            fread(&count, 4, 1, sf) == 1 && count > 0) {

            clear_global_paths();
            g_paths = (char **)calloc(count, sizeof(char *));
            if (!g_paths) {
                fclose(sf);
                fclose(df);
                return 0;
            }
            g_paths_n = count;

            for (uint32_t i = 0; i < count; i++) {
                uint32_t gid;
                uint64_t offset;
                uint32_t len;
                if (fread(&gid, 4, 1, sf) != 1 ||
                    fread(&offset, 8, 1, sf) != 1 ||
                    fread(&len, 4, 1, sf) != 1) {
                    break;
                }

                if (gid >= count) continue;
                if (fseeko(df, (off_t)offset, SEEK_SET) != 0) continue;

                char *line = (char *)malloc((size_t)len + 1);
                if (!line) continue;
                if (fread(line, 1, (size_t)len, df) != len) {
                    free(line);
                    continue;
                }
                line[len] = '\0';

                char *path = json_get_str(line, "p");
                if (path) {
                    free(g_paths[gid]);
                    g_paths[gid] = path;
                }
                free(line);
            }
            fclose(sf);
            fclose(df);
            return 1;
        }
    }
    if (sf) fclose(sf);
    if (df) fclose(df);

    FILE *f = fopen(ndjson_path, "r");
    if (!f) return 0;

    clear_global_paths();

    char *line = NULL;
    size_t line_cap = 0;
    ssize_t nread;
    int loaded = 0;
    while ((nread = getline(&line, &line_cap, f)) > 0) {
        if (nread > 0 && line[nread - 1] == '\n') line[nread - 1] = '\0';
        if (!*line) continue;
        uint32_t gid = 0;
        if (!json_get_u32(line, "gid", &gid)) continue;
        char *path = json_get_str(line, "p");
        if (!path) continue;
        if (!ensure_str_slot(&g_paths, &g_paths_n, gid)) {
            free(path);
            continue;
        }
        free(g_paths[gid]);
        g_paths[gid] = path;
        loaded++;
    }
    free(line);
    fclose(f);
    return loaded > 0;
}

static char *parent_path_dup(const char *path) {
    if (!path || !*path) return dupstr("");
    const char *end = path + strlen(path) - 1;
    while (end > path && *end == '/') end--;
    if (end <= path) return dupstr("/");
    while (end > path && *end != '/') end--;
    if (end == path) return dupstr("/");
    size_t len = (size_t)(end - path);
    char *out = (char *)malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, path, len);
    out[len] = '\0';
    return out;
}

static int str_contains_ci(const char *haystack, const char *needle) {
    if (!needle || !*needle) return 1;
    if (!haystack) return 0;
    size_t hlen = strlen(haystack);
    size_t nlen = strlen(needle);
    if (nlen > hlen) return 0;
    for (size_t pos = 0; pos <= hlen - nlen; pos++) {
        size_t j = 0;
        while (j < nlen) {
            unsigned char ca = (unsigned char)haystack[pos + j];
            unsigned char cb = (unsigned char)needle[j];
            if (ca >= 'A' && ca <= 'Z') ca = (unsigned char)(ca + 32);
            if (cb >= 'A' && cb <= 'Z') cb = (unsigned char)(cb + 32);
            if (ca != cb) break;
            j++;
        }
        if (j == nlen) return 1;
    }
    return 0;
}

static int kw_match_any(const char *path, const str_list *kw_tokens) {
    if (kw_tokens->count == 0) return 1;
    for (size_t ti = 0; ti < kw_tokens->count; ti++) {
        const char *tok = kw_tokens->items[ti];
        if (!tok || !*tok) continue;
        if (str_contains_ci(path, tok)) return 1;
    }
    return 0;
}

static int doc_list_reserve(doc_list *lst, size_t cap_hint) {
    if (cap_hint <= lst->cap) return 1;
    doc_ref *buf = (doc_ref *)realloc(lst->items, cap_hint * sizeof(doc_ref));
    if (!buf) return 0;
    lst->items = buf;
    lst->cap = cap_hint;
    return 1;
}

static int doc_list_push(doc_list *lst, const doc_ref *d) {
    if (lst->count == lst->cap) {
        size_t next = lst->cap ? lst->cap * 2 : 4096;
        doc_ref *buf = (doc_ref *)realloc(lst->items, next * sizeof(doc_ref));
        if (!buf) return 0;
        lst->items = buf;
        lst->cap = next;
    }
    lst->items[lst->count++] = *d;
    return 1;
}

static void free_doc_list(doc_list *lst) {
    if (!lst) return;
    free(lst->items);
    lst->items = NULL;
    lst->count = 0;
    lst->cap = 0;
}

static int treemap_rows_push(treemap_rows *lst, const treemap_row *r) {
    if (lst->count == lst->cap) {
        size_t next = lst->cap ? lst->cap * 2 : 4096;
        treemap_row *buf = (treemap_row *)realloc(lst->items, next * sizeof(treemap_row));
        if (!buf) return 0;
        lst->items = buf;
        lst->cap = next;
    }
    lst->items[lst->count++] = *r;
    return 1;
}

static void free_treemap_rows(treemap_rows *lst) {
    if (!lst) return;
    for (size_t i = 0; i < lst->count; i++) {
        free(lst->items[i].id);
        free(lst->items[i].name);
        free(lst->items[i].owner);
        free(lst->items[i].pid);
        free(lst->items[i].path);
        free(lst->items[i].type);
    }
    free(lst->items);
    lst->items = NULL;
    lst->count = 0;
    lst->cap = 0;
}

static int load_treemap_shards_dir(const char *data_dir, treemap_rows *rows) {
    char *shards_dir = build_path2(data_dir, "shards");
    if (!shards_dir) return 0;

    DIR *d = opendir(shards_dir);
    if (!d) {
        free(shards_dir);
        return 0;
    }

    struct dirent *entry;
    while ((entry = readdir(d)) != NULL) {
        if (entry->d_name[0] == '.') continue;
        if (strlen(entry->d_name) != 2) continue;

        size_t nd = strlen(shards_dir) + 1 + strlen(entry->d_name) + strlen("/bucket.ndjson") + 1;
        char *bucket_path = (char *)malloc(nd);
        if (!bucket_path) continue;
        snprintf(bucket_path, nd, "%s/%s/bucket.ndjson", shards_dir, entry->d_name);

        FILE *bf = fopen(bucket_path, "r");
        free(bucket_path);
        if (!bf) continue;

        char *line = NULL;
        size_t line_cap = 0;
        ssize_t nread;
        while ((nread = getline(&line, &line_cap, bf)) > 0) {
            if (nread > 0 && line[nread - 1] == '\n') line[nread - 1] = '\0';
            if (!*line) continue;

            treemap_row r = {0};
            r.id = json_get_str(line, "id");
            if (json_get_u32(line, "gid", &r.gid)) r.has_gid = 1;
            else { r.gid = 0; r.has_gid = 0; }
            r.name = json_get_str(line, "n");
            r.owner = json_get_str(line, "o");
            r.pid = json_get_str(line, "pid");
            r.path = json_get_str(line, "p");
            r.type = json_get_str(line, "t");
            if (!json_get_u64(line, "v", &r.value)) r.value = 0;
            if (!json_get_bool(line, "h", &r.has_children)) r.has_children = 0;

            if ((r.path || r.has_gid) && r.name) {
                if (!treemap_rows_push(rows, &r)) {
                    free(r.id); free(r.name); free(r.owner); free(r.pid); free(r.path); free(r.type);
                }
            } else {
                free(r.id); free(r.name); free(r.owner); free(r.pid); free(r.path); free(r.type);
            }
        }
        free(line);
        fclose(bf);
    }
    closedir(d);
    free(shards_dir);
    return 1;
}

static int load_treemap_rows(const char *detail_root, treemap_rows *rows) {
    clear_treemap_runtime_state();
    memset(rows, 0, sizeof(*rows));

    char *tree_map_data = NULL;
    char *manifest = build_path2(detail_root, "manifest.json");
    char *buf = manifest ? read_file_all(manifest) : NULL;
    if (manifest) free(manifest);

    if (buf && strstr(buf, "check-disk-detail-treemap") != NULL) {
        const char *slash = strrchr(detail_root, '/');
        if (!slash || slash == detail_root) {
            free(buf);
            return 0;
        }
        size_t parent_n = (size_t)(slash - detail_root);
        char *parent = (char *)malloc(parent_n + 1);
        if (!parent) {
            free(buf);
            return 0;
        }
        memcpy(parent, detail_root, parent_n);
        parent[parent_n] = '\0';

        size_t tm_n = strlen(parent) + strlen("/tree_map_data") + 1;
        tree_map_data = (char *)malloc(tm_n);
        if (!tree_map_data) {
            free(parent);
            free(buf);
            return 0;
        }
        snprintf(tree_map_data, tm_n, "%s/tree_map_data", parent);
        free(parent);
    }
    free(buf);

    if (!tree_map_data) {
        size_t tm_n = strlen(detail_root) + strlen("/tree_map_data") + 1;
        tree_map_data = (char *)malloc(tm_n);
        if (!tree_map_data) return 0;
        snprintf(tree_map_data, tm_n, "%s/tree_map_data", detail_root);
    }

    int ok = load_treemap_shards_dir(tree_map_data, rows);
    if (!ok) {
        free(tree_map_data);
        return 0;
    }
    (void)load_treemap_path_dict(tree_map_data);
    free(tree_map_data);
    return 1;
}

static __attribute__((unused)) int list_ids_from_filter(const str_list *inputs, char **dict, size_t dict_n,
                                int is_ext, uint32_t **out_ids, size_t *out_n) {
    *out_ids = NULL;
    *out_n = 0;
    if (inputs->count == 0) return 1;
    uint32_t *ids = (uint32_t *)calloc(inputs->count + 1, sizeof(uint32_t));
    if (!ids) return 0;
    size_t n = 0;
    for (size_t i = 0; i < inputs->count; i++) {
        char *norm = norm_lower_token(inputs->items[i], is_ext ? 1 : 0);
        if (!norm) {
            free(ids);
            return 0;
        }
        uint32_t id = 0;
        int ok = 0;
        if (!is_ext) {
            ok = find_id_exact(dict, dict_n, inputs->items[i], &id);
        } else {
            ok = find_id_exact(dict, dict_n, norm, &id);
        }
        if (ok && !contains_u32(ids, n, id)) ids[n++] = id;
        free(norm);
    }
    *out_ids = ids;
    *out_n = n;
    return 1;
}

static int pass_id_filter(uint32_t value, const uint32_t *ids, size_t n) {
    if (n == 0) return 1;
    for (size_t i = 0; i < n; i++) if (ids[i] == value) return 1;
    return 0;
}

static void print_doc_json(const doc_ref *d, unsigned int mask,
                           char **paths, size_t paths_n,
                           char **exts, size_t exts_n,
                           char **users, size_t users_n) {
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
        const char *p = (d->gid < paths_n) ? paths[d->gid] : NULL;
        if (p) print_json_escaped(p);
        else printf("null");
        first = 0;
    }
    if (mask & F_EXT) {
        printf("%s\"ext\":", first ? "" : ",");
        const char *e = (d->eid < exts_n) ? exts[d->eid] : NULL;
        if (e) print_json_escaped(e);
        else printf("null");
        first = 0;
    }
    if (mask & F_USER) {
        printf("%s\"user\":", first ? "" : ",");
        const char *u = (d->uid < users_n) ? users[d->uid] : NULL;
        if (u) print_json_escaped(u);
        else printf("null");
        first = 0;
    }
    putchar('}');
}

int main(int argc, char **argv) {
    if (argc < 2) {
        usage(argv[0]);
        return 2;
    }

    const char *input = argv[1];
    const char *kw_csv = NULL, *ext_csv = NULL, *user_csv = NULL;
    const char *type_filter = NULL;
    uint64_t size_min = 0, size_max = 0;
    int has_min = 0, has_max = 0;
    size_t limit = 0, offset = 0;
    size_t page_num = 0;
    int output_json = 0, output_docs = 0, show_stats = 0;
    unsigned int fields_mask = 0;
    enum sort_mode sort_mode = SORT_NONE;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--kw") == 0 && i + 1 < argc) kw_csv = argv[++i];
        else if (strcmp(argv[i], "--ext") == 0 && i + 1 < argc) ext_csv = argv[++i];
        else if (strcmp(argv[i], "--user") == 0 && i + 1 < argc) user_csv = argv[++i];
        else if (strcmp(argv[i], "--type") == 0 && i + 1 < argc) type_filter = argv[++i];
        else if (strcmp(argv[i], "--min") == 0 && i + 1 < argc) {
            size_min = strtoull(argv[++i], NULL, 10);
            has_min = 1;
        } else if (strcmp(argv[i], "--max") == 0 && i + 1 < argc) {
            size_max = strtoull(argv[++i], NULL, 10);
            has_max = 1;
        } else if (strcmp(argv[i], "--limit") == 0 && i + 1 < argc) {
            limit = (size_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--offset") == 0 && i + 1 < argc) {
            offset = (size_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--page") == 0 && i + 1 < argc) {
            page_num = (size_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--sort") == 0 && i + 1 < argc) {
            sort_mode = parse_sort_mode(argv[++i]);
        } else if (strcmp(argv[i], "--json") == 0) {
            output_json = 1;
        } else if (strcmp(argv[i], "--docs") == 0) {
            output_docs = 1;
        } else if (strcmp(argv[i], "--fields") == 0 && i + 1 < argc) {
            fields_mask = parse_fields_mask(argv[++i]);
        } else if (strcmp(argv[i], "--stats") == 0) {
            show_stats = 1;
        } else {
            usage(argv[0]);
            return 2;
        }
    }

    if (type_filter && strcmp(type_filter, "dir") != 0 && strcmp(type_filter, "file") != 0) {
        usage(argv[0]);
        return 2;
    }

    if (page_num > 0) {
        if (limit == 0) limit = 500;
        if (page_num > 1 && offset == 0) offset = (page_num - 1) * limit;
    }
    if (limit == 0) limit = 500;
    if (page_num == 0) page_num = (offset / limit) + 1;

    char *detail_root = resolve_detail_root(input);
    if (!detail_root) return 1;

    if (is_treemap_layout(detail_root)) {
        treemap_rows rows = {0};
        if (!load_treemap_rows(detail_root, &rows)) {
            fprintf(stderr, "failed to load treemap rows from shards\n");
            free(detail_root);
            return 1;
        }

        str_list kw_raw = {0}, user_raw = {0};
        split_pipe_terms(kw_csv ? kw_csv : "", &kw_raw);
        split_csv(user_csv ? user_csv : "", &user_raw);


        doc_list docs = {0};
        size_t treemap_hint = rows.count < 4096 ? rows.count : 4096;
        if (treemap_hint > 0 && !doc_list_reserve(&docs, treemap_hint)) {
            free_treemap_rows(&rows);
            free_str_list(&kw_raw);
            free_str_list(&user_raw);
            free(detail_root);
            return 1;
        }
        for (size_t i = 0; i < rows.count; i++) {
            treemap_row *r = &rows.items[i];
            const char *resolved_path = treemap_row_path(r);
            if (!resolved_path) continue;

            if ((r->type && strcmp(r->type, "g") == 0) || (r->name && strcmp(r->name, "[files]") == 0) || strstr(resolved_path, "__files__") != NULL) continue;

            if (has_min && r->value < size_min) continue;
            if (has_max && r->value > size_max) continue;
            if (!kw_match_any(resolved_path, &kw_raw) && !kw_match_any(r->name ? r->name : "", &kw_raw)) continue;

            if (user_raw.count > 0) {
                int owner_ok = 0;
                for (size_t u = 0; u < user_raw.count; u++) {
                    if (r->owner && strcmp(r->owner, user_raw.items[u]) == 0) {
                        owner_ok = 1;
                        break;
                    }
                }
                if (!owner_ok) continue;
            }

            uint32_t gid = 0;
            if (r->has_gid && g_treemap_paths_loaded && r->gid < g_paths_n && g_paths[r->gid]) {
                gid = r->gid;
            } else {
                gid = (uint32_t)docs.count;
                if (!ensure_str_slot(&g_paths, &g_paths_n, gid)) continue;
                free(g_paths[gid]);
                g_paths[gid] = dupstr(resolved_path);
                if (!g_paths[gid]) continue;
            }
            append_filter_doc(&docs, (uint32_t)i, gid, 0, 0, r->value);
        }

        if (sort_mode == SORT_SIZE_ASC) qsort(docs.items, docs.count, sizeof(doc_ref), cmp_doc_size_asc);
        else if (sort_mode == SORT_SIZE_DESC) qsort(docs.items, docs.count, sizeof(doc_ref), cmp_doc_size_desc);
        else if (sort_mode == SORT_PATH_ASC) qsort(docs.items, docs.count, sizeof(doc_ref), cmp_doc_path_asc);

        size_t matched = docs.count;
        size_t start = (offset < matched) ? offset : matched;
        size_t emit = 0;
        for (size_t i = start; i < matched && emit < limit; i++) {
            uint32_t did = docs.items[i].doc_id;
            treemap_row *r = (did < rows.count) ? &rows.items[did] : NULL;
            const char *resolved_path = treemap_row_path(r);
            if ((r && r->type && strcmp(r->type, "g") == 0) || (r && r->name && strcmp(r->name, "[files]") == 0) || (resolved_path && strstr(resolved_path, "__files__") != NULL)) continue;
            emit++;
        }

        if (output_json) {
            size_t total_pages = (matched + limit > 0) ? ((matched + limit - 1) / limit) : 1;
            printf("{\"matched\":%zu,\"returned\":%zu,\"page\":%zu,\"page_size\":%zu,\"total_pages\":%zu,\"doc_ids\":[",
                   matched, emit, page_num, limit, total_pages);
            size_t emitted = 0;
            for (size_t i = start; i < matched && emitted < emit; i++) {
                uint32_t did = docs.items[i].doc_id;
                treemap_row *r = (did < rows.count) ? &rows.items[did] : NULL;
                const char *resolved_path = treemap_row_path(r);
                if ((r && r->type && strcmp(r->type, "g") == 0) || (r && r->name && strcmp(r->name, "[files]") == 0) || (resolved_path && strstr(resolved_path, "__files__") != NULL)) continue;
                if (emitted) putchar(',');
                printf("%u", docs.items[i].doc_id);
                emitted++;
            }
            putchar(']');
            if (output_docs && emit > 0) {
                printf(",\"docs\":[");
                emitted = 0;
                for (size_t i = start; i < matched && emitted < emit; i++) {
                    uint32_t did = docs.items[i].doc_id;
                    treemap_row *r = (did < rows.count) ? &rows.items[did] : NULL;
                    const char *resolved_path = treemap_row_path(r);
                    if ((r && r->type && strcmp(r->type, "g") == 0) || (r && r->name && strcmp(r->name, "[files]") == 0) || (resolved_path && strstr(resolved_path, "__files__") != NULL)) continue;
                    if (emitted) putchar(',');
                    printf("{\"doc_id\":%u,\"path\":", docs.items[i].doc_id);
                    if (resolved_path) print_json_escaped(resolved_path); else printf("null");
                    printf(",\"size\":%llu,\"user\":", (unsigned long long)docs.items[i].size);
                    if (r && r->owner) print_json_escaped(r->owner); else printf("null");
                    printf(",\"name\":");
                    if (r && r->name) print_json_escaped(r->name); else printf("null");
                    printf(",\"type\":");
                    if (r && r->type) print_json_escaped(r->type); else printf("null");
                    printf(",\"shard_id\":");
                    if (r && r->id) print_json_escaped(r->id); else printf("null");
                    printf(",\"parent_path\":");
                    if (r && r->pid && *r->pid) {
                        print_json_escaped(r->pid);
                    } else if (resolved_path) {
                        char *pp = parent_path_dup(resolved_path);
                        if (pp) {
                            print_json_escaped(pp);
                            free(pp);
                        } else {
                            printf("null");
                        }
                    } else {
                        printf("null");
                    }
                    printf(",\"has_children\":%s}", (r && r->has_children) ? "true" : "false");
                    emitted++;
                }
                putchar(']');
            }
            puts("}");
        } else {
            printf("matched docs: %zu\n", matched);
            for (size_t i = 0; i < emit; i++) printf("%u\n", docs.items[start + i].doc_id);
        }

        free_doc_list(&docs);
        free_treemap_rows(&rows);
        free_str_list(&kw_raw);
        free_str_list(&user_raw);
                free(detail_root);
        return 0;
    }
    if (!is_text_layout(detail_root)) {
        fprintf(stderr, "detail layout not found at %s (manifest.json schema check failed)\n", detail_root);
        free(detail_root);
        return 1;
    }

    /* ── Load user list from root manifest ── */
    char **g_users = NULL;
    size_t g_users_n = 0;
    if (!load_root_users(detail_root, &g_users, &g_users_n)) {
        fprintf(stderr, "failed to load user list from manifest\n");
        free(detail_root);
        return 1;
    }

    (void)load_detail_path_dict(detail_root);

    /* ── Collect all file rows from per-user chunks ── */
    size_t g_exts_cap = 256, g_exts_n = 0;
    char **g_exts = (char **)calloc(g_exts_cap, sizeof(char *));
    
    str_list kw_raw = {0}, ext_raw = {0}, user_raw = {0};
    split_pipe_terms(kw_csv ? kw_csv : "", &kw_raw);
    split_csv(ext_csv ? ext_csv : "", &ext_raw);
    split_csv(user_csv ? user_csv : "", &user_raw);

    /* kw terms are split only by '|' and matched as whole substrings */

    /* Build user_id filter list */
    uint32_t *user_ids = NULL;
    size_t user_n = 0;
    for (size_t i = 0; i < user_raw.count; i++) {
        uint32_t uid = 0;
        if (find_id_exact(g_users, g_users_n, user_raw.items[i], &uid)) {
            uint32_t *tmp = (uint32_t *)realloc(user_ids, (user_n + 1) * sizeof(uint32_t));
            if (!tmp) { free(user_ids); user_ids = NULL; }
            else { user_ids = tmp; user_ids[user_n++] = uid; }
        }
    }

    unsigned char *user_selected = NULL;
    if (user_n > 0 && g_users_n > 0) {
        user_selected = (unsigned char *)calloc(g_users_n, sizeof(unsigned char));
        if (!user_selected) {
            free(detail_root);
            free_str_arr(g_users, g_users_n);
            free(user_ids);
            free_str_list(&kw_raw);
            free_str_list(&ext_raw);
            free_str_list(&user_raw);
            return 1;
        }
        for (size_t i = 0; i < user_n; i++) {
            if (user_ids[i] < g_users_n) user_selected[user_ids[i]] = 1;
        }
    }

    /* Build ext_id filter list */
    uint32_t *ext_ids = NULL;
    size_t ext_n = 0;
    for (size_t i = 0; i < ext_raw.count; i++) {
        char *norm_ext = norm_lower_token(ext_raw.items[i], 1);
        if (!norm_ext) continue;
        uint32_t eid = 0;
        int found = 0;
        for (size_t j = 0; j < g_exts_n; j++) {
            if (g_exts[j] && strcmp(g_exts[j], norm_ext) == 0) { eid = (uint32_t)j; found = 1; break; }
        }
        if (!found) {
            if (!ensure_str_slot(&g_exts, &g_exts_cap, g_exts_n)) { free(norm_ext); continue; }
            g_exts[g_exts_n] = norm_ext;
            eid = (uint32_t)g_exts_n;
            g_exts_n++;
        } else {
            free(norm_ext);
        }
        if (!contains_u32(ext_ids, ext_n, eid)) {
            uint32_t *tmp = (uint32_t *)realloc(ext_ids, (ext_n + 1) * sizeof(uint32_t));
            if (tmp) { ext_ids = tmp; ext_ids[ext_n++] = eid; }
        }
    }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    doc_list docs = {0};
    size_t docs_hint = user_n > 0 ? (user_n * 4096) : 4096;
    if (!doc_list_reserve(&docs, docs_hint)) {
        free(detail_root);
        free_str_arr(g_users, g_users_n);
        free(user_ids);
        free(user_selected);
        free(ext_ids);
        free_str_list(&kw_raw);
        free_str_list(&ext_raw);
        free_str_list(&user_raw);
                free_str_arr(g_exts, g_exts_cap);
        return 1;
    }
    uint32_t seq_doc_id = 0;
    size_t scanned = 0;

    /* Iterate every user, read their file parts */
    for (size_t ui = 0; ui < g_users_n; ui++) {
        const char *username = g_users[ui];
        uint32_t uid = (uint32_t)ui;

        /* Apply user filter */
        if (user_selected && (uid >= g_users_n || !user_selected[uid])) continue;

        char *user_safedir = safe_user_dir(username);
        if (!user_safedir) continue;

        /* Build path: detail_root/users/<safedir> */
        size_t base_n = strlen(detail_root) + 8;  /* "/users/" */
        size_t user_dir_n = base_n + strlen(user_safedir) + 2;
        char *user_dir = (char *)malloc(user_dir_n);
        if (user_dir) {
            snprintf(user_dir, user_dir_n, "%s/users/%s", detail_root, user_safedir);

            char **parts = NULL;
            size_t parts_n = 0;
            int loaded_parts = 0;
            if (type_filter && strcmp(type_filter, "dir") == 0) {
                char *manifest_path = build_path2(user_dir, "manifest.json");
                char *buf = manifest_path ? read_file_all(manifest_path) : NULL;
                if (manifest_path) free(manifest_path);
                if (buf) {
                    char *dirs_sec = strstr(buf, "\"dirs\"");
                    char *parts_sec = dirs_sec ? strstr(dirs_sec, "\"parts\"") : NULL;
                    char *arr = parts_sec ? strchr(parts_sec, '[') : NULL;
                    char *arr_end = arr ? strchr(arr, ']') : NULL;
                    if (arr && arr_end) {
                        size_t cap = 4;
                        parts = (char **)calloc(cap, sizeof(char *));
                        if (parts) {
                            const char *p = arr;
                            while ((p = strstr(p, "\"path\"")) && p < arr_end) {
                                char *path = json_get_str(p, "path");
                                if (!path) { p += 6; continue; }
                                if (parts_n == cap) {
                                    cap *= 2;
                                    char **next = (char **)realloc(parts, cap * sizeof(char *));
                                    if (!next) {
                                        free(path);
                                        free_str_arr(parts, parts_n);
                                        parts = NULL;
                                        parts_n = 0;
                                        break;
                                    }
                                    parts = next;
                                }
                                parts[parts_n++] = path;
                                p += 6;
                            }
                            loaded_parts = 1;
                        }
                    }
                    free(buf);
                }
            } else {
                loaded_parts = load_user_file_parts(user_dir, &parts, &parts_n);
            }
            if (loaded_parts) {
                for (size_t pi = 0; pi < parts_n; pi++) {
                    char *part_rel = parts[pi];
                    if (!part_rel) continue;

                    size_t part_path_n = strlen(user_dir) + 1 + strlen(part_rel) + 1;
                    char *part_path = (char *)malloc(part_path_n);
                    if (!part_path) continue;
                    snprintf(part_path, part_path_n, "%s/%s", user_dir, part_rel);

                    FILE *pf = fopen(part_path, "r");
                    if (!pf) { free(part_path); continue; }

                    char *line = NULL;
                    size_t line_cap = 0;
                    ssize_t nread;
                    while ((nread = getline(&line, &line_cap, pf)) > 0) {
                        if (nread > 0 && line[nread - 1] == '\n') line[nread - 1] = '\0';
                        if (!*line) continue;

                        uint64_t size = 0;
                        if (!json_get_u64(line, "s", &size)) continue;

                        if (has_min && size < size_min) continue;
                        if (has_max && size > size_max) continue;

                        uint32_t gid = 0;
                        int has_gid = json_get_u32(line, "gid", &gid);
                        if (!has_gid && json_get_u32(line, "d", &gid)) has_gid = 1;

                        char *path_owned = NULL;
                        const char *path_view = NULL;
                        if (has_gid && gid < g_paths_n && g_paths[gid]) {
                            path_view = g_paths[gid];
                        } else {
                            path_owned = json_get_str(line, "p");
                            if (!path_owned) path_owned = json_get_str(line, "n");
                            if (!path_owned) continue;
                            path_view = path_owned;
                            has_gid = 0;
                        }

                        if (!kw_match_any(path_view, &kw_raw)) { free(path_owned); continue; }

                        uint32_t eid = 0;
                        char *ext_str = NULL;
                        char *ext_norm = NULL;
                        if (!(type_filter && strcmp(type_filter, "dir") == 0) && (ext_n > 0 || output_docs)) {
                            ext_str = json_get_str(line, "x");
                            ext_norm = ext_str ? norm_lower_token(ext_str, 1) : NULL;
                            if (ext_norm) {
                                if (!find_id_exact(g_exts, g_exts_n, ext_norm, &eid)) {
                                    if (!ensure_str_slot(&g_exts, &g_exts_cap, g_exts_n)) {
                                        free(ext_norm);
                                        free(ext_str);
                                        free(path_owned);
                                        continue;
                                    }
                                    g_exts[g_exts_n] = dupstr(ext_norm);
                                    eid = (uint32_t)g_exts_n;
                                    g_exts_n++;
                                }
                            }
                        }
                        if (!(type_filter && strcmp(type_filter, "dir") == 0) && !pass_id_filter(eid, ext_ids, ext_n)) {
                            free(ext_norm); free(path_owned); free(ext_str); continue;
                        }

                        if (!has_gid) {
                            gid = seq_doc_id;
                            if (!ensure_str_slot(&g_paths, &g_paths_n, gid)) {
                                free(path_owned); free(ext_norm); free(ext_str);
                                continue;
                            }
                            free(g_paths[gid]);
                            g_paths[gid] = path_owned;
                            if (!g_paths[gid]) {
                                free(path_owned); free(ext_norm); free(ext_str);
                                continue;
                            }
                            path_owned = NULL;
                        }

                        scanned++;
                        append_filter_doc(&docs, seq_doc_id++, gid, uid, eid, size);
                        free(ext_norm); free(ext_str); free(path_owned);
                    }
                    free(line);
                    fclose(pf);
                    free(part_path);
                }
            }
            free_str_arr(parts, parts_n);
            parts = NULL;
            free(user_dir);
        }
        free(user_safedir);
    }

    /* g_paths/g_paths_n now hold inline per-doc paths for output/sorting */

    /* sort */
    if (sort_mode == SORT_SIZE_ASC) qsort(docs.items, docs.count, sizeof(doc_ref), cmp_doc_size_asc);
    else if (sort_mode == SORT_SIZE_DESC) qsort(docs.items, docs.count, sizeof(doc_ref), cmp_doc_size_desc);
    else if (sort_mode == SORT_PATH_ASC) {
        qsort(docs.items, docs.count, sizeof(doc_ref), cmp_doc_path_asc);
    }

    size_t matched = docs.count;
    size_t start = (offset < matched) ? offset : matched;
    size_t remain = (start < matched) ? (matched - start) : 0;
    size_t emit = (remain > limit) ? limit : remain;

    if (output_json) {
        size_t total_pages = (matched + limit > 0) ? ((matched + limit - 1) / limit) : 1;
        printf("{\"matched\":%zu,\"returned\":%zu,\"page\":%zu,\"page_size\":%zu,\"total_pages\":%zu,\"doc_ids\":[",
               matched, emit, page_num, limit, total_pages);
        for (size_t i = 0; i < emit; i++) {
            if (i) putchar(',');
            printf("%u", docs.items[start + i].doc_id);
        }
        putchar(']');

        if (output_docs && emit > 0) {
            unsigned int mask = fields_mask ? fields_mask : F_ALL;
            printf(",\"docs\":[");
            for (size_t i = 0; i < emit; i++) {
                if (i) putchar(',');
                print_doc_json(&docs.items[start + i], mask, g_paths, g_paths_n, g_exts, g_exts_n, g_users, g_users_n);
            }
            putchar(']');
        }
        puts("}");
    } else {
        printf("matched docs: %zu\n", matched);
        for (size_t i = 0; i < emit; i++) printf("%u\n", docs.items[start + i].doc_id);
    }

    if (show_stats) {
        clock_gettime(CLOCK_MONOTONIC, &t1);
        double sec = (double)(t1.tv_sec - t0.tv_sec) + (double)(t1.tv_nsec - t0.tv_nsec) / 1e9;
        fprintf(stderr, "stats: scanned=%zu matched=%zu returned=%zu elapsed=%.6fs\n", scanned, matched, emit, sec);
    }

    free_doc_list(&docs);
    free(ext_ids); free(user_ids); free(user_selected);
    free_str_list(&kw_raw); free_str_list(&ext_raw); free_str_list(&user_raw);     free_str_arr(g_exts, g_exts_n);
    free_str_arr(g_users, g_users_n);
    free(detail_root);
    return 0;
}
