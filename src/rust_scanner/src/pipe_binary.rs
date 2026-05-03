use crc32fast::Hasher;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::io::{BufWriter, Read, Write};
use std::path::Path;

pub const MAGIC_PATH: u32 = 0x5041_5448;
pub const MAGIC_PRNT: u32 = 0x5052_4152;
pub const MAGIC_EXTS: u32 = 0x4558_5453;
pub const MAGIC_USER: u32 = 0x5553_4552;

pub fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<(), String> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| e.to_string())
}

pub fn write_u64<W: Write>(writer: &mut W, value: u64) -> Result<(), String> {
    writer
        .write_all(&value.to_le_bytes())
        .map_err(|e| e.to_string())
}

pub fn write_dict_offsets_blob<W: Write>(writer: &mut W, items: &[String]) -> Result<(), String> {
    let mut offsets: Vec<u64> = Vec::with_capacity(items.len() + 1);
    let mut cursor: u64 = 0;
    offsets.push(cursor);
    for value in items {
        let len = value.as_bytes().len() as u64;
        cursor += len + 1;
        offsets.push(cursor);
    }

    for off in offsets {
        write_u64(writer, off)?;
    }
    for value in items {
        writer
            .write_all(value.as_bytes())
            .map_err(|e| e.to_string())?;
        writer.write_all(&[0u8]).map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub fn write_dict_header_16<W: Write>(
    writer: &mut W,
    magic: u32,
    version: u32,
    count: u32,
) -> Result<(), String> {
    write_u32(writer, magic)?;
    write_u32(writer, version)?;
    write_u32(writer, count)?;
    write_u32(writer, 0)?;
    Ok(())
}

pub fn write_paths_header_24<W: Write>(
    writer: &mut W,
    version: u32,
    count: u32,
) -> Result<(), String> {
    write_u32(writer, MAGIC_PATH)?;
    write_u32(writer, version)?;
    write_u32(writer, count)?;
    write_u32(writer, 0)?;
    write_u64(writer, 0)?;
    Ok(())
}

// ── detail_users/dict/* ──────────────────────────────────────────────────────

fn write_detail_dict_headers<W: std::io::Write>(
    w: &mut W,
    magic: u32,
    count: u32,
    offsets: &[u64],
    _blob_size: usize,
) -> Result<(), String> {
    write_u32(w, magic)?;
    write_u32(w, 1)?;
    write_u32(w, count)?;
    write_u32(w, 0)?;
    for off in offsets {
        write_u64(w, *off)?;
    }
    Ok(())
}

pub fn write_detail_users_dict_all(
    dict_dir: &Path,
    paths: &[String],
    parents: &[u32],
    exts: &[String],
    users: &[String],
) -> Result<(), String> {
    fs::create_dir_all(dict_dir).map_err(|e| format!("mkdir {}: {}", dict_dir.display(), e))?;

    // paths.dict.bin (offsets+blob, null-term)
    let mut offsets: Vec<u64> = Vec::with_capacity(paths.len() + 1);
    let mut cur: u64 = 0;
    offsets.push(cur);
    for p in paths {
        cur += p.as_bytes().len() as u64 + 1;
        offsets.push(cur);
    }
    let paths_file = fs::File::create(dict_dir.join("paths.dict.bin"))
        .map_err(|e| format!("create paths.dict.bin: {}", e))?;
    let mut w = BufWriter::with_capacity(8 * 1024 * 1024, paths_file);
    write_detail_dict_headers(
        &mut w,
        MAGIC_PATH,
        paths.len() as u32,
        &offsets,
        cur as usize,
    )?;
    for p in paths {
        w.write_all(p.as_bytes()).map_err(|e| e.to_string())?;
        w.write_all(&[0u8]).map_err(|e| e.to_string())?;
    }
    w.flush()
        .map_err(|e| format!("flush paths.dict.bin: {}", e))?;

    // paths.parent.bin (PRNT magic, 16-byte header)
    let parent_file = fs::File::create(dict_dir.join("paths.parent.bin"))
        .map_err(|e| format!("create paths.parent.bin: {}", e))?;
    let mut pw = BufWriter::new(parent_file);
    write_u32(&mut pw, MAGIC_PRNT).map_err(|e| e.to_string())?;
    write_u32(&mut pw, 1).map_err(|e| e.to_string())?;
    write_u32(&mut pw, parents.len() as u32).map_err(|e| e.to_string())?;
    write_u32(&mut pw, 0).map_err(|e| e.to_string())?;
    for p in parents {
        write_u32(&mut pw, *p).map_err(|e| e.to_string())?;
    }
    pw.flush()
        .map_err(|e| format!("flush paths.parent.bin: {}", e))?;

    // exts.dict.bin
    let mut e_offsets: Vec<u64> = Vec::with_capacity(exts.len() + 1);
    cur = 0;
    e_offsets.push(cur);
    for e in exts {
        cur += e.as_bytes().len() as u64 + 1;
        e_offsets.push(cur);
    }
    let exts_file = fs::File::create(dict_dir.join("exts.dict.bin"))
        .map_err(|e| format!("create exts.dict.bin: {}", e))?;
    let mut ew = BufWriter::with_capacity(1024 * 1024, exts_file);
    write_detail_dict_headers(
        &mut ew,
        MAGIC_EXTS,
        exts.len() as u32,
        &e_offsets,
        cur as usize,
    )?;
    for e in exts {
        ew.write_all(e.as_bytes()).map_err(|e| e.to_string())?;
        ew.write_all(&[0u8]).map_err(|e| e.to_string())?;
    }
    ew.flush()
        .map_err(|e| format!("flush exts.dict.bin: {}", e))?;

    // users.dict.bin
    let mut u_offsets: Vec<u64> = Vec::with_capacity(users.len() + 1);
    cur = 0;
    u_offsets.push(cur);
    for u in users {
        cur += u.as_bytes().len() as u64 + 1;
        u_offsets.push(cur);
    }
    let users_file = fs::File::create(dict_dir.join("users.dict.bin"))
        .map_err(|e| format!("create users.dict.bin: {}", e))?;
    let mut uw = BufWriter::with_capacity(1024 * 1024, users_file);
    write_detail_dict_headers(
        &mut uw,
        MAGIC_USER,
        users.len() as u32,
        &u_offsets,
        cur as usize,
    )?;
    for u in users {
        uw.write_all(u.as_bytes()).map_err(|e| e.to_string())?;
        uw.write_all(&[0u8]).map_err(|e| e.to_string())?;
    }
    uw.flush()
        .map_err(|e| format!("flush users.dict.bin: {}", e))?;

    Ok(())
}

// ── detail_users/cols/* ─────────────────────────────────────────────────────

const MAGIC_DOCU: u32 = 0x444F4355; // "DOCU"
const MAGIC_DOCP: u32 = 0x444F4350; // "DOCP"
const MAGIC_DOCS: u32 = 0x444F4353; // "DOCS"
const MAGIC_DOCE: u32 = 0x444F4345; // "DOCE"

fn write_col_header<W: std::io::Write>(w: &mut W, magic: u32, count: u64) -> Result<(), String> {
    write_u32(w, magic)?;
    write_u32(w, 1)?;
    write_u64(w, count)
}

pub fn write_detail_users_cols(
    cols_dir: &Path,
    docs: &[(u32, u32, u64, u32)],
) -> Result<(), String> {
    fs::create_dir_all(cols_dir).map_err(|e| format!("mkdir {}: {}", cols_dir.display(), e))?;

    let n = docs.len() as u64;

    // doc.uid.bin
    let uid_file = fs::File::create(cols_dir.join("doc.uid.bin"))
        .map_err(|e| format!("create doc.uid.bin: {}", e))?;
    let mut uw = BufWriter::with_capacity(8 * 1024 * 1024, uid_file);
    write_col_header(&mut uw, MAGIC_DOCU, n)?;
    for &(uid, _, _, _) in docs {
        write_u32(&mut uw, uid)?;
    }
    uw.flush()
        .map_err(|e| format!("flush doc.uid.bin: {}", e))?;

    // doc.path_id.bin
    let pid_file = fs::File::create(cols_dir.join("doc.path_id.bin"))
        .map_err(|e| format!("create doc.path_id.bin: {}", e))?;
    let mut pw = BufWriter::with_capacity(8 * 1024 * 1024, pid_file);
    write_col_header(&mut pw, MAGIC_DOCP, n)?;
    for &(_, path_id, _, _) in docs {
        write_u32(&mut pw, path_id)?;
    }
    pw.flush()
        .map_err(|e| format!("flush doc.path_id.bin: {}", e))?;

    // doc.size.bin
    let size_file = fs::File::create(cols_dir.join("doc.size.bin"))
        .map_err(|e| format!("create doc.size.bin: {}", e))?;
    let mut sw = BufWriter::with_capacity(8 * 1024 * 1024, size_file);
    write_col_header(&mut sw, MAGIC_DOCS, n)?;
    for &(_, _, size, _) in docs {
        write_u64(&mut sw, size)?;
    }
    sw.flush()
        .map_err(|e| format!("flush doc.size.bin: {}", e))?;

    // doc.ext_id.bin
    let eid_file = fs::File::create(cols_dir.join("doc.ext_id.bin"))
        .map_err(|e| format!("create doc.ext_id.bin: {}", e))?;
    let mut ew = BufWriter::with_capacity(8 * 1024 * 1024, eid_file);
    write_col_header(&mut ew, MAGIC_DOCE, n)?;
    for &(_, _, _, eid) in docs {
        write_u32(&mut ew, eid)?;
    }
    ew.flush()
        .map_err(|e| format!("flush doc.ext_id.bin: {}", e))?;

    Ok(())
}

// ── detail_users/agg/* ──────────────────────────────────────────────────────

const MAGIC_UIDS: u32 = 0x55494453; // UIDS
const MAGIC_DUSZ: u32 = 0x4455535A; // DUSZ
const MAGIC_PERM: u32 = 0x5045524D; // PERM

pub fn write_detail_users_agg(
    agg_dir: &Path,
    uid_totals: &[(u32, u64, u64, u64)], // uid_id, file_count, total_bytes, total_dirs
    dir_user_sizes: &[(u32, u32, u64)],  // dir_path_id, uid_id, bytes
    perm_events: &[(u32, u32, u16, u8)], // uid_id, path_id, errcode_id, kind
) -> Result<(), String> {
    fs::create_dir_all(agg_dir).map_err(|e| format!("mkdir {}: {}", agg_dir.display(), e))?;

    // uid_totals.bin
    let uid_path = agg_dir.join("uid_totals.bin");
    let uid_file =
        fs::File::create(&uid_path).map_err(|e| format!("create {}: {}", uid_path.display(), e))?;
    let mut uw = BufWriter::with_capacity(1024 * 1024, uid_file);
    write_u32(&mut uw, MAGIC_UIDS)?;
    write_u32(&mut uw, 1)?;
    write_u32(&mut uw, uid_totals.len() as u32)?;
    write_u32(&mut uw, 0)?;
    for &(uid_id, file_count, total_bytes, total_dirs) in uid_totals {
        write_u32(&mut uw, uid_id)?;
        write_u32(&mut uw, 0)?;
        write_u64(&mut uw, file_count)?;
        write_u64(&mut uw, total_bytes)?;
        write_u64(&mut uw, total_dirs)?;
    }
    uw.flush()
        .map_err(|e| format!("flush uid_totals.bin: {}", e))?;

    // dir_user_sizes.bin
    let dusz_path = agg_dir.join("dir_user_sizes.bin");
    let dusz_file = fs::File::create(&dusz_path)
        .map_err(|e| format!("create {}: {}", dusz_path.display(), e))?;
    let mut dw = BufWriter::with_capacity(8 * 1024 * 1024, dusz_file);
    write_u32(&mut dw, MAGIC_DUSZ)?;
    write_u32(&mut dw, 1)?;
    write_u32(&mut dw, dir_user_sizes.len() as u32)?;
    write_u32(&mut dw, 0)?;
    for &(dir_path_id, uid_id, bytes) in dir_user_sizes {
        write_u32(&mut dw, dir_path_id)?;
        write_u32(&mut dw, uid_id)?;
        write_u64(&mut dw, bytes)?;
    }
    dw.flush()
        .map_err(|e| format!("flush dir_user_sizes.bin: {}", e))?;

    // perm_events.bin
    let perm_path = agg_dir.join("perm_events.bin");
    let perm_file = fs::File::create(&perm_path)
        .map_err(|e| format!("create {}: {}", perm_path.display(), e))?;
    let mut pw = BufWriter::with_capacity(1024 * 1024, perm_file);
    write_u32(&mut pw, MAGIC_PERM)?;
    write_u32(&mut pw, 1)?;
    write_u32(&mut pw, perm_events.len() as u32)?;
    write_u32(&mut pw, 0)?;
    for &(uid_id, path_id, errcode_id, kind) in perm_events {
        write_u32(&mut pw, uid_id)?;
        write_u32(&mut pw, path_id)?;
        pw.write_all(&errcode_id.to_le_bytes())
            .map_err(|e| e.to_string())?;
        pw.write_all(&[kind]).map_err(|e| e.to_string())?;
        pw.write_all(&[0u8]).map_err(|e| e.to_string())?;
    }
    pw.flush()
        .map_err(|e| format!("flush perm_events.bin: {}", e))?;

    Ok(())
}

// ── detail_users/users/u_XXXX.bin ───────────────────────────────────────────

const MAGIC_USRF: u32 = 0x55535246; // USRF

pub fn write_detail_user_bins(
    users_dir: &Path,
    uid_order: &[u32],
    team_ids: &HashMap<u32, u32>,
    uid_totals: &HashMap<u32, (u64, u64, u64)>, // files, bytes, dirs
    top_dirs: &HashMap<u32, Vec<(u32, i64)>>,
    user_files: &HashMap<u32, Vec<(u32, u64)>>,
    chunk_target: usize,
) -> Result<(), String> {
    fs::create_dir_all(users_dir).map_err(|e| format!("mkdir {}: {}", users_dir.display(), e))?;

    let chunk_target = chunk_target.max(1);

    for uid_id in uid_order {
        let (total_files, total_bytes, total_dirs) =
            uid_totals.get(uid_id).copied().unwrap_or((0, 0, 0));
        let team_id = team_ids.get(uid_id).copied().unwrap_or(0);
        let mut tdirs = top_dirs.get(uid_id).cloned().unwrap_or_default();
        tdirs.sort_by(|a, b| b.1.cmp(&a.1));

        let mut files = user_files.get(uid_id).cloned().unwrap_or_default();
        files.sort_by(|a, b| b.1.cmp(&a.1));

        let mut chunks: Vec<Vec<(u32, u64)>> = Vec::new();
        if !files.is_empty() {
            for part in files.chunks(chunk_target) {
                chunks.push(part.to_vec());
            }
        }

        let path = users_dir.join(format!("u_{:04}.bin", uid_id + 1));
        let file =
            fs::File::create(&path).map_err(|e| format!("create {}: {}", path.display(), e))?;
        let mut w = BufWriter::with_capacity(8 * 1024 * 1024, file);

        // Header 48 bytes
        write_u32(&mut w, MAGIC_USRF)?;
        write_u32(&mut w, 1)?;
        write_u32(&mut w, *uid_id)?;
        write_u32(&mut w, team_id)?;
        write_u64(&mut w, total_files)?;
        write_u64(&mut w, total_bytes)?;
        write_u64(&mut w, total_dirs)?;
        write_u32(&mut w, tdirs.len() as u32)?;
        write_u32(&mut w, chunks.len() as u32)?;

        // Section A
        for (path_id, bytes) in &tdirs {
            write_u32(&mut w, *path_id)?;
            w.write_all(&bytes.to_le_bytes())
                .map_err(|e| e.to_string())?;
        }

        // Prepare section C payload + chunk table
        let mut chunk_counts: Vec<u32> = Vec::new();
        let mut chunk_bytes: Vec<u64> = Vec::new();
        let mut payload: Vec<u8> = Vec::new();
        let mut offsets: Vec<u64> = Vec::new();
        let mut cur_offset: u64 = 0;
        for chunk in &chunks {
            offsets.push(cur_offset);
            chunk_counts.push(chunk.len() as u32);
            let mut sum = 0u64;
            for (path_id, size) in chunk {
                payload.extend_from_slice(&path_id.to_le_bytes());
                payload.extend_from_slice(&size.to_le_bytes());
                sum = sum.saturating_add(*size);
                cur_offset += 12;
            }
            chunk_bytes.push(sum);
        }

        // Section B
        for i in 0..chunks.len() {
            write_u32(&mut w, i as u32)?;
            write_u32(&mut w, chunk_counts[i])?;
            write_u64(&mut w, chunk_bytes[i])?;
            write_u64(&mut w, offsets[i])?;
        }

        // Section C
        w.write_all(&payload).map_err(|e| e.to_string())?;
        w.flush()
            .map_err(|e| format!("flush {}: {}", path.display(), e))?;
    }

    Ok(())
}

// ─── TreeMap Binary Output ─────────────────────────────────────────────────────

const MAGIC_NODE: u32 = 0x4E4F4445; // "NODE"
const MAGIC_CHLD: u32 = 0x43484C44; // "CHLD"
const MAGIC_ROOT: u32 = 0x52504F54; // "RPRT"
const MAGIC_NAMD: u32 = 0x4E414D44; // "NAMD"

#[derive(Clone)]
pub struct TreeNode {
    pub path_id: u32,
    pub size: u64,
    pub node_type: u8, // 0=file, 1=dir, 2=symlink
    pub name: String,
}

/// Write tree binary outputs to target_dir.
/// Returns (node_count, child_count, top_root_count, name_count).
pub fn write_treemap_bins(
    target_dir: &Path,
    tree_root: &str,
    dir_sizes: &HashMap<String, Vec<(String, i64)>>,
    _dir_owner_map: &HashMap<String, String>,
    all_paths: &[(String, u32)], // (path_string, path_id)
    path_to_id: &HashMap<String, u32>,
) -> Result<(u32, u32, u32, u32), String> {
    let out = target_dir;
    fs::create_dir_all(out).map_err(|e| format!("mkdir {}: {}", out.display(), e))?;

    // Build path_id -> name map
    let mut path_id_to_name: HashMap<u32, String> = HashMap::new();
    for (path_str, pid) in all_paths {
        let name = std::path::Path::new(path_str)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        path_id_to_name.insert(*pid, name);
    }

    // Compute dir recursive sizes from dir_sizes
    // dir_sizes: path -> [(uid, size)]
    let mut dir_total: HashMap<String, i64> = HashMap::new();
    for (dpath, uid_sizes) in dir_sizes {
        let total: i64 = uid_sizes.iter().map(|(_, sz)| *sz).sum();
        if total > 0 {
            *dir_total.entry(dpath.clone()).or_insert(0) += total;
        }
    }

    // Determine node types
    let is_dir: HashSet<String> = dir_total.keys().cloned().collect();
    let is_file: HashSet<String> = {
        let mut s = HashSet::new();
        for (p, _) in all_paths {
            if !is_dir.contains(p) {
                s.insert(p.clone());
            }
        }
        s
    };

    // Build parent-child relationships
    let root = normalize_path(tree_root);
    let mut parent_map: HashMap<u32, u32> = HashMap::new();
    for (path_str, _) in all_paths {
        if path_str == &root {
            continue;
        }
        let parent_path = parent_path_str(path_str);
        if let Some(&pid) = path_to_id.get(path_str) {
            let ppid = path_to_id.get(&parent_path).copied().unwrap_or(0);
            parent_map.insert(pid, ppid);
        }
    }

    // Collect nodes (path_id, size, type, name)
    let mut nodes: Vec<TreeNode> = Vec::new();
    for (path_str, pid) in all_paths {
        let total_sz: i64 = dir_total.get(path_str).copied().unwrap_or(0);
        let node_type = if is_dir.contains(path_str) {
            1u8
        } else if is_file.contains(path_str) {
            0u8
        } else {
            0u8
        };
        let name = path_id_to_name.get(pid).cloned().unwrap_or_default();
        nodes.push(TreeNode {
            path_id: *pid,
            size: total_sz.max(0) as u64,
            node_type,
            name,
        });
    }

    // Sort by path_id
    nodes.sort_by_key(|n| n.path_id);
    let node_count = nodes.len() as u32;

    // Write nodes.bin
    let nodes_path = out.join("nodes.bin");
    {
        let file = fs::File::create(&nodes_path)
            .map_err(|e| format!("create {}: {}", nodes_path.display(), e))?;
        let mut w = BufWriter::with_capacity(8 * 1024 * 1024, file);
        write_u32(&mut w, MAGIC_NODE)?;
        write_u32(&mut w, 1)?;
        write_u32(&mut w, node_count)?;
        write_u32(&mut w, 0)?; // reserved
        for node in &nodes {
            write_u32(&mut w, node.path_id)?;
            write_u64(&mut w, node.size)?;
            w.write_all(&[node.node_type, 0, 0, 0])
                .map_err(|e| e.to_string())?;
        }
        w.flush()
            .map_err(|e| format!("flush {}: {}", nodes_path.display(), e))?;
    }

    // Build children list
    let mut children: Vec<(u32, u32)> = parent_map
        .iter()
        .map(|(&child, &parent)| (parent, child))
        .collect();
    children.sort_by(|a, b| match a.0.cmp(&b.0) {
        std::cmp::Ordering::Equal => a.1.cmp(&b.1),
        o => o,
    });
    let child_count = children.len() as u32;

    // Write children.bin
    let children_path = out.join("children.bin");
    {
        let file = fs::File::create(&children_path)
            .map_err(|e| format!("create {}: {}", children_path.display(), e))?;
        let mut w = BufWriter::with_capacity(8 * 1024 * 1024, file);
        write_u32(&mut w, MAGIC_CHLD)?;
        write_u32(&mut w, 1)?;
        write_u32(&mut w, child_count)?;
        write_u32(&mut w, 0)?;
        for (parent, child) in &children {
            write_u32(&mut w, *parent)?;
            write_u32(&mut w, *child)?;
            write_u32(&mut w, 0)?;
        }
        w.flush()
            .map_err(|e| format!("flush {}: {}", children_path.display(), e))?;
    }

    // Top roots: direct children of root path
    let root_id = path_to_id.get(&root).copied().unwrap_or(0);
    let top_roots: Vec<u32> = children
        .iter()
        .filter(|(p, _)| *p == root_id)
        .map(|(_, c)| *c)
        .collect();
    let top_root_count = top_roots.len() as u32;

    // Write top_roots.bin
    let roots_path = out.join("top_roots.bin");
    {
        let file = fs::File::create(&roots_path)
            .map_err(|e| format!("create {}: {}", roots_path.display(), e))?;
        let mut w = BufWriter::with_capacity(256 * 1024, file);
        write_u32(&mut w, MAGIC_ROOT)?;
        write_u32(&mut w, 1)?;
        write_u32(&mut w, top_root_count)?;
        write_u32(&mut w, 0)?;
        for &root_child in &top_roots {
            write_u32(&mut w, root_child)?;
            write_u64(&mut w, 0)?; // reserved
        }
        w.flush()
            .map_err(|e| format!("flush {}: {}", roots_path.display(), e))?;
    }

    // Name dict: all unique basenames
    let mut names: Vec<String> = nodes.iter().map(|n| n.name.clone()).collect();
    names.sort();
    names.dedup();
    let name_count = names.len() as u32;

    // Build name_dict.bin (same offsets+blob pattern)
    let names_path = out.join("name_dict.bin");
    {
        let file = fs::File::create(&names_path)
            .map_err(|e| format!("create {}: {}", names_path.display(), e))?;
        let mut w = BufWriter::with_capacity(256 * 1024, file);
        write_u32(&mut w, MAGIC_NAMD)?;
        write_u32(&mut w, 1)?;
        write_u32(&mut w, name_count)?;
        write_u32(&mut w, 0)?;

        let mut offsets: Vec<u64> = Vec::with_capacity(name_count as usize + 1);
        let mut blob: Vec<u8> = Vec::new();
        offsets.push(0);
        for name in &names {
            blob.extend_from_slice(name.as_bytes());
            blob.push(0);
            offsets.push(blob.len() as u64);
        }

        // Write offsets
        for off in &offsets {
            write_u64(&mut w, *off)?;
        }
        // Write blob
        w.write_all(&blob).map_err(|e| e.to_string())?;
        w.flush()
            .map_err(|e| format!("flush {}: {}", names_path.display(), e))?;
    }

    Ok((node_count, child_count, top_root_count, name_count))
}

/// Normalize a path string (strip trailing slash, handle empty → "/").
fn normalize_path(p: &str) -> String {
    let p = p.trim_end_matches('/');
    if p.is_empty() { "/" } else { p }.to_string()
}

/// Return the parent path of `path_str`.
fn parent_path_str(path: &str) -> String {
    let p = normalize_path(path);
    if p == "/" {
        return p;
    }
    let mut parts: Vec<&str> = p.split('/').collect();
    if parts.len() <= 1 {
        return "/".to_string();
    }
    parts.pop();
    if parts.len() == 1 && parts[0].is_empty() {
        return "/".to_string();
    }
    parts.join("/")
}

// ─── Checksum Utilities (Epic B3) ─────────────────────────────────────────────

pub fn crc32c_of_files(base_dir: &Path, rel_paths: &[&str]) -> Result<String, String> {
    let mut sorted_paths: Vec<&str> = rel_paths.to_vec();
    sorted_paths.sort_unstable();

    let mut hasher = Hasher::new();
    hasher.update(b"CHECK_DISK_CRC32C_V1");

    for rel in sorted_paths {
        let full = base_dir.join(rel);
        let mut f = fs::File::open(&full)
            .map_err(|e| format!("open {} for checksum: {}", full.display(), e))?;
        let mut buf = [0u8; 65536];
        loop {
            let n = f
                .read(&mut buf)
                .map_err(|e| format!("read {} for checksum: {}", full.display(), e))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
    }

    Ok(format!("{:08x}", hasher.finalize()))
}

pub fn file_size(path: &Path) -> u64 {
    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

// ─── TreeMap Manifest + API JSON ───────────────────────────────────────────────

pub fn write_treemap_manifest(
    target_dir: &Path,
    node_count: u32,
    child_count: u32,
    top_root_count: u32,
    name_count: u32,
    created_at: i64,
) -> Result<(), String> {
    let files_for_checksum = ["nodes.bin", "children.bin", "top_roots.bin", "name_dict.bin"];
    let checksum = crc32c_of_files(target_dir, &files_for_checksum)?;

    let manifest = serde_json::json!({
        "schema": "check-disk-detail-treemap",
        "version": 1,
        "created_at": created_at,
        "total_nodes": node_count,
        "total_children": child_count,
        "total_top_roots": top_root_count,
        "files": {
            "nodes.bin": {
                "bytes": file_size(&target_dir.join("nodes.bin")),
                "records": node_count,
                "description": "node_id, size, type, reserved"
            },
            "children.bin": {
                "bytes": file_size(&target_dir.join("children.bin")),
                "records": child_count,
                "description": "parent_id, child_id, reserved"
            },
            "top_roots.bin": {
                "bytes": file_size(&target_dir.join("top_roots.bin")),
                "records": top_root_count,
                "description": "root child path_id"
            },
            "name_dict.bin": {
                "bytes": file_size(&target_dir.join("name_dict.bin")),
                "records": name_count,
                "description": "offsets+blob dict of basenames"
            }
        },
        "checksum": checksum
    });

    let path = target_dir.join("manifest.json");
    let json_str = serde_json::to_string_pretty(&manifest)
        .map_err(|e| format!("serialize manifest: {}", e))?;
    fs::write(&path, json_str).map_err(|e| format!("write {}: {}", path.display(), e))?;

    let api_dir = target_dir.join("api");
    fs::create_dir_all(&api_dir).map_err(|e| format!("mkdir api: {}", e))?;
    let root_min = serde_json::json!({
        "schema": "check-disk-treemap-api-v1",
        "total_nodes": node_count,
        "total_children": child_count,
        "top_roots": top_root_count,
        "files": {
            "nodes": "nodes.bin",
            "children": "children.bin",
            "top_roots": "top_roots.bin",
            "name_dict": "name_dict.bin"
        }
    });
    let root_min_path = api_dir.join("root.min.json");
    let root_json_str = serde_json::to_string(&root_min)
        .map_err(|e| format!("serialize root.min: {}", e))?;
    fs::write(&root_min_path, root_json_str)
        .map_err(|e| format!("write {}: {}", root_min_path.display(), e))?;

    Ok(())
}
