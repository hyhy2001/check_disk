# native_index — C query engine cho detail user

## Binary data layout

```
detail_users/
├── index/
│   ├── index.mmi          # MMI inverted index (CDX1 format)
│   ├── tokens.json        # token dict (for debugging)
│   ├── exts.json         # extension dict
│   └── users.json        # user dict
├── index_seed/
│   ├── docs.bin          # SDX1: uid,gid,size,eid records
│   ├── paths.bin         # PTH1 v1: path string table (direct binary, no gzip)
│   ├── exts.bin          # SDCT: extension strings
│   └── users.bin         # SDCT: user strings
└── users/{alice,bob,...}/
    └── ...               # per-user detail gzip files
```

## Cú pháp CLI

```bash
cdx1_query <index_dir> \
    [--kw kw1,kw2] \
    [--ext .txt,.log] \
    [--user alice,bob] \
    [--min 1024] \
    [--max 1048576] \
    [--limit 100] \
    [--offset 0] \
    [--sort size_asc|size_desc|path_asc] \
    [--fields doc_id,gid,uid,size,eid,sid,path,ext,user] \
    [--json] \
    [--docs]
```

## Ví dụ

```bash
# Tìm file txt chứa "report" của user alice, kích thước 1KB-1MB
./cdx1_query /var/check_disk/detail_users/index \
    --kw report \
    --ext .txt \
    --user alice \
    --min 1024 \
    --max 1048576 \
    --json --docs

# Output JSON
{
  "matched": 42,
  "returned": 42,
  "doc_ids": [3, 7, 12, ...],
  "docs": [
    {"doc_id": 3, "gid": 5, "uid": 0, "size": 4096, "eid": 1, "sid": 0,
     "path": "/home/alice/data/report.txt", "ext": "txt", "user": "alice"},
    ...
  ]
}
```

## PHP 5.4 integration

```php
require_once '/path/to/check_disk/src/native_index/php54_query_helper.php';

$helper = new Cdx1QueryHelper('/path/to/check_disk/src/native_index/cdx1_query');
$data = $helper->query('/var/check_disk/detail_users/index', [
    'keywords'  => ['report', 'log'],
    'extensions'=> ['.txt', '.log'],
    'users'     => ['alice'],
    'size_min'  => 1024,
    'size_max'  => 10485760,
    'limit'     => 100,
    'fields'    => ['doc_id', 'path', 'size', 'ext'],
]);
```

## Build

```bash
cd src/native_index
make clean && make
# output: cdx1_query, libcdx1.so, libcdx1.a
```
