# native_index — native query engine cho detail user

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
query_cli <detail_users_dir> \
    [--kw kw1|kw2] \
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
./query_cli /var/check_disk/detail_users \
    --kw report|error \
    --ext .txt \
    --user alice \
    --min 1024 \
    --max 1048576 \
    --json --docs
```

## PHP 5.4 integration

```php
require_once '/path/to/check_disk/src/native_index/php54_query_helper.php';

$helper = new NativeIndexQueryHelper('/path/to/check_disk/src/native_index/query_cli');
$data = $helper->query('/var/check_disk/detail_users', [
    'keywords'  => ['report', 'error'],
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
# output: query_cli, libnative_index.so, libnative_index.a
```
