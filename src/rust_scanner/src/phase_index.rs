use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufReader, Read, Write};
use std::path::Path;

const MMI_MAGIC: u32 = 0x3149_5843; // "CXI1" little-endian
const SEED_MAGIC: u32 = 0x3158_4453; // "SDX1" little-endian
const SEED_DICT_MAGIC: u32 = 0x5443_4453; // "SDCT" little-endian
const PATHS_BIN_MAGIC: u32 = 0x31485450; // "PTH1" little-endian

#[derive(Clone)]
struct DocRec {
    size: u64,
    gid: u32,
    sid: u32,
    eid: u32,
    uid: u32,
}

fn tokenize_path(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for ch in path.chars() {
        if ch.is_ascii_alphanumeric() {
            cur.push(ch.to_ascii_lowercase());
        } else if !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn write_u32<W: Write>(w: &mut W, v: u32) -> Result<(), String> {
    w.write_all(&v.to_le_bytes()).map_err(|e| e.to_string())
}
fn write_u64<W: Write>(w: &mut W, v: u64) -> Result<(), String> {
    w.write_all(&v.to_le_bytes()).map_err(|e| e.to_string())
}

fn read_dict_binary(path: &Path) -> Result<Vec<String>, String> {
    let mut reader = BufReader::with_capacity(1024 * 1024, File::open(path).map_err(|e| format!("open {}: {}", path.display(), e))?);
    let mut head = [0u8; 12];
    reader.read_exact(&mut head).map_err(|e| format!("read {}: {}", path.display(), e))?;
    let magic = u32::from_le_bytes([head[0], head[1], head[2], head[3]]);

    if magic == 0x4558_5453 || magic == 0x5553_4552 || magic == 0x5041_5448 {
        let count = u32::from_le_bytes([head[8], head[9], head[10], head[11]]) as usize;

        if magic == 0x5041_5448 {
            let mut extra_hdr = [0u8; 12];
            reader
                .read_exact(&mut extra_hdr)
                .map_err(|e| format!("read PATH extra header: {}", e))?;
        } else {
            let mut reserved = [0u8; 4];
            reader
                .read_exact(&mut reserved)
                .map_err(|e| format!("read dict reserved: {}", e))?;
        }

        let mut offsets = Vec::with_capacity(count + 1);
        for _ in 0..=count {
            let mut off_buf = [0u8; 8];
            reader
                .read_exact(&mut off_buf)
                .map_err(|e| format!("read offset: {}", e))?;
            offsets.push(u64::from_le_bytes(off_buf));
        }

        let blob_len = offsets.last().copied().unwrap_or(0) as usize;
        let mut blob = vec![0u8; blob_len];
        reader
            .read_exact(&mut blob)
            .map_err(|e| format!("read dict blob: {}", e))?;

        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let start = offsets[i] as usize;
            let end = offsets[i + 1] as usize;
            if end < start || end > blob.len() {
                return Err(format!("invalid dict offsets at {}: {}..{}", i, start, end));
            }
            let mut bytes = blob[start..end].to_vec();
            if bytes.last().copied() == Some(0) {
                bytes.pop();
            }
            out.push(String::from_utf8_lossy(&bytes).to_string());
        }
        return Ok(out);
    }

    if magic == 0x5052_4152 {
        return Err(format!("unsupported PRNT dict reader for {}", path.display()));
    }

    // Format cũ (SEED_DICT_MAGIC):
    if magic != SEED_DICT_MAGIC {
        return Err(format!("invalid dict magic in {}", path.display()));
    }
    let count = u32::from_le_bytes([head[8], head[9], head[10], head[11]]) as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).map_err(|e| format!("read {}: {}", path.display(), e))?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut bytes = vec![0u8; len];
        reader.read_exact(&mut bytes).map_err(|e| format!("read {}: {}", path.display(), e))?;
        out.push(String::from_utf8_lossy(&bytes).to_string());
    }
    Ok(out)
}

fn build_mmi_index_from_seed(detail_root: &Path) -> Result<bool, String> {
    let seed_dir = detail_root.join("index_seed");
    let docs_path = seed_dir.join("docs.bin");
    let exts_path = seed_dir.join("exts.bin");
    let users_path = seed_dir.join("users.bin");
    let paths_path = seed_dir.join("paths.bin");
    if !docs_path.exists() || !exts_path.exists() || !users_path.exists() || !paths_path.exists() {
        return Ok(false);
    }

    let paths = read_paths_binary(&paths_path)?;
    let exts = read_dict_binary(&exts_path)?;
    let users = read_dict_binary(&users_path)?;

    let mut docs_file = BufReader::with_capacity(8 * 1024 * 1024, File::open(&docs_path).map_err(|e| format!("open {}: {}", docs_path.display(), e))?);
    let mut header = [0u8; 8];
    docs_file.read_exact(&mut header).map_err(|e| format!("read {}: {}", docs_path.display(), e))?;
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    if magic != SEED_MAGIC { return Ok(false); }

    let mut docs: Vec<DocRec> = Vec::new();
    let mut token_to_id: HashMap<String, u32> = HashMap::new();
    let mut tokens: Vec<String> = Vec::new();
    let mut token_posts: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut ext_posts: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut user_posts: HashMap<u32, Vec<u32>> = HashMap::new();

    loop {
        let mut rec = [0u8; 20];
        match docs_file.read_exact(&mut rec) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(format!("read {}: {}", docs_path.display(), e)),
        }
        let uid = u32::from_le_bytes([rec[0], rec[1], rec[2], rec[3]]);
        let gid = u32::from_le_bytes([rec[4], rec[5], rec[6], rec[7]]);
        let size = u64::from_le_bytes([rec[8], rec[9], rec[10], rec[11], rec[12], rec[13], rec[14], rec[15]]);
        let eid = u32::from_le_bytes([rec[16], rec[17], rec[18], rec[19]]);
        let doc_id = docs.len() as u32;
        docs.push(DocRec { size, gid, sid: 0, eid, uid });
        ext_posts.entry(eid).or_default().push(doc_id);
        user_posts.entry(uid).or_default().push(doc_id);
        let path = paths.get(gid as usize).cloned().unwrap_or_default();
        let mut seen: HashSet<u32> = HashSet::new();
        for tok in tokenize_path(&path) {
            let tid = if let Some(id) = token_to_id.get(&tok) { *id } else {
                let id = tokens.len() as u32;
                tokens.push(tok.clone());
                token_to_id.insert(tok, id);
                id
            };
            if seen.insert(tid) { token_posts.entry(tid).or_default().push(doc_id); }
        }
    }

    for list in token_posts.values_mut() { list.sort_unstable(); }
    for list in ext_posts.values_mut() { list.sort_unstable(); }
    for list in user_posts.values_mut() { list.sort_unstable(); }

    let mut token_entries: Vec<(u32, u64, u32)> = Vec::new();
    let mut token_values: Vec<u32> = Vec::new();
    let mut token_keys: Vec<u32> = token_posts.keys().copied().collect(); token_keys.sort_unstable();
    for key in token_keys { let vals = token_posts.get(&key).unwrap(); let off = token_values.len() as u64; token_values.extend_from_slice(vals); token_entries.push((key, off, vals.len() as u32)); }
    let mut ext_entries: Vec<(u32, u64, u32)> = Vec::new();
    let mut ext_values: Vec<u32> = Vec::new();
    let mut ext_keys: Vec<u32> = ext_posts.keys().copied().collect(); ext_keys.sort_unstable();
    for key in ext_keys { let vals = ext_posts.get(&key).unwrap(); let off = ext_values.len() as u64; ext_values.extend_from_slice(vals); ext_entries.push((key, off, vals.len() as u32)); }
    let mut user_entries: Vec<(u32, u64, u32)> = Vec::new();
    let mut user_values: Vec<u32> = Vec::new();
    let mut user_keys: Vec<u32> = user_posts.keys().copied().collect(); user_keys.sort_unstable();
    for key in user_keys { let vals = user_posts.get(&key).unwrap(); let off = user_values.len() as u64; user_values.extend_from_slice(vals); user_entries.push((key, off, vals.len() as u32)); }

    let index_dir = detail_root.join("index");
    fs::create_dir_all(&index_dir).map_err(|e| format!("mkdir {}: {}", index_dir.display(), e))?;
    let mmi_path = index_dir.join("index.mmi");
    let mut out = File::create(&mmi_path).map_err(|e| format!("create {}: {}", mmi_path.display(), e))?;
    let header_size = (6 * 4 + 7 * 8) as u64;
    let docs_offset = header_size;
    let docs_size = (docs.len() as u64) * 32;
    let token_entries_offset = docs_offset + docs_size;
    let token_entries_size = (token_entries.len() as u64) * 24;
    let token_values_offset = token_entries_offset + token_entries_size;
    let token_values_size = (token_values.len() as u64) * 4;
    let ext_entries_offset = token_values_offset + token_values_size;
    let ext_entries_size = (ext_entries.len() as u64) * 24;
    let ext_values_offset = ext_entries_offset + ext_entries_size;
    let ext_values_size = (ext_values.len() as u64) * 4;
    let user_entries_offset = ext_values_offset + ext_values_size;
    let user_entries_size = (user_entries.len() as u64) * 24;
    let user_values_offset = user_entries_offset + user_entries_size;
    write_u32(&mut out, MMI_MAGIC)?; write_u32(&mut out, 1)?; write_u32(&mut out, docs.len() as u32)?; write_u32(&mut out, token_entries.len() as u32)?; write_u32(&mut out, ext_entries.len() as u32)?; write_u32(&mut out, user_entries.len() as u32)?;
    write_u64(&mut out, docs_offset)?; write_u64(&mut out, token_entries_offset)?; write_u64(&mut out, token_values_offset)?; write_u64(&mut out, ext_entries_offset)?; write_u64(&mut out, ext_values_offset)?; write_u64(&mut out, user_entries_offset)?; write_u64(&mut out, user_values_offset)?;
    for (doc_id, d) in docs.iter().enumerate() { write_u32(&mut out, doc_id as u32)?; write_u32(&mut out, 0)?; write_u64(&mut out, d.size)?; write_u32(&mut out, d.gid)?; write_u32(&mut out, d.sid)?; write_u32(&mut out, d.eid)?; write_u32(&mut out, d.uid)?; }
    for (key, off, count) in &token_entries { write_u32(&mut out, *key)?; write_u32(&mut out, 0)?; write_u64(&mut out, *off)?; write_u32(&mut out, *count)?; write_u32(&mut out, 0)?; }
    for v in &token_values { write_u32(&mut out, *v)?; }
    for (key, off, count) in &ext_entries { write_u32(&mut out, *key)?; write_u32(&mut out, 0)?; write_u64(&mut out, *off)?; write_u32(&mut out, *count)?; write_u32(&mut out, 0)?; }
    for v in &ext_values { write_u32(&mut out, *v)?; }
    for (key, off, count) in &user_entries { write_u32(&mut out, *key)?; write_u32(&mut out, 0)?; write_u64(&mut out, *off)?; write_u32(&mut out, *count)?; write_u32(&mut out, 0)?; }
    for v in &user_values { write_u32(&mut out, *v)?; }
    fs::write(index_dir.join("tokens.json"), serde_json::to_vec(&tokens).map_err(|e| e.to_string())?).map_err(|e| format!("write tokens.json: {}", e))?;
    fs::write(index_dir.join("exts.json"), serde_json::to_vec(&exts).map_err(|e| e.to_string())?).map_err(|e| format!("write exts.json: {}", e))?;
    fs::write(index_dir.join("users.json"), serde_json::to_vec(&users).map_err(|e| e.to_string())?).map_err(|e| format!("write users.json: {}", e))?;
    Ok(true)
}

pub fn build_mmi_index(detail_root: &Path) -> Result<(), String> {
    if build_mmi_index_from_seed(detail_root)? {
        return Ok(());
    }
    Err("index_seed missing or invalid; legacy fallback removed".to_string())
}
fn read_paths_binary(path: &Path) -> Result<Vec<String>, String> {
    let mut reader = BufReader::with_capacity(8 * 1024 * 1024, File::open(path).map_err(|e| format!("open {}: {}", path.display(), e))?);
    let mut head = [0u8; 12];
    reader.read_exact(&mut head).map_err(|e| format!("read {}: {}", path.display(), e))?;
    let magic = u32::from_le_bytes([head[0], head[1], head[2], head[3]]);

    // Format offsets+blob (magic PATH):
    if magic == 0x50415448 {
        let count = u32::from_le_bytes([head[8], head[9], head[10], head[11]]) as usize;

        // PATH header is 24 bytes total; we've read 12 already, consume reserved+flags.
        let mut extra_hdr = [0u8; 12];
        reader
            .read_exact(&mut extra_hdr)
            .map_err(|e| format!("read PATH extra header: {}", e))?;

        let mut offsets = Vec::with_capacity(count + 1);
        for _ in 0..=count {
            let mut off_buf = [0u8; 8];
            reader
                .read_exact(&mut off_buf)
                .map_err(|e| format!("read offset: {}", e))?;
            offsets.push(u64::from_le_bytes(off_buf));
        }

        let blob_len = offsets.last().copied().unwrap_or(0) as usize;
        let mut blob = vec![0u8; blob_len];
        reader
            .read_exact(&mut blob)
            .map_err(|e| format!("read path blob: {}", e))?;

        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let start = offsets[i] as usize;
            let end = offsets[i + 1] as usize;
            if end < start || end > blob.len() {
                return Err(format!("invalid PATH offsets at {}: {}..{}", i, start, end));
            }
            let mut bytes = blob[start..end].to_vec();
            if bytes.last().copied() == Some(0) {
                bytes.pop();
            }
            out.push(String::from_utf8_lossy(&bytes).to_string());
        }
        return Ok(out);
    }

    // Format cũ (PATHS_BIN_MAGIC):
    if magic != PATHS_BIN_MAGIC {
        return Err(format!("invalid paths bin magic in {}", path.display()));
    }
    let _version = u32::from_le_bytes([head[4], head[5], head[6], head[7]]);
    let count = u32::from_le_bytes([head[8], head[9], head[10], head[11]]) as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).map_err(|e| format!("read {}: {}", path.display(), e))?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut bytes = vec![0u8; len];
        reader.read_exact(&mut bytes).map_err(|e| format!("read {}: {}", path.display(), e))?;
        out.push(String::from_utf8_lossy(&bytes).to_string());
    }
    Ok(out)
}
