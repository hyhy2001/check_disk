use std::fs;

pub(crate) fn format_num(mut n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let mut s = String::new();
    let mut count = 0;
    while n > 0 {
        if count != 0 && count % 3 == 0 {
            s.insert(0, ',');
        }
        s.insert(0, (b'0' + (n % 10) as u8) as char);
        n /= 10;
        count += 1;
    }
    s
}

pub(crate) fn format_size(bytes: u64) -> String {
    let kb = 1024_f64;
    let mb = kb * 1024_f64;
    let gb = mb * 1024_f64;
    let tb = gb * 1024_f64;
    let bytes_f = bytes as f64;
    if bytes_f >= tb {
        format!("{:.2} TB", bytes_f / tb)
    } else if bytes_f >= gb {
        format!("{:.2} GB", bytes_f / gb)
    } else if bytes_f >= mb {
        format!("{:.2} MB", bytes_f / mb)
    } else if bytes_f >= kb {
        format!("{:.2} KB", bytes_f / kb)
    } else {
        format!("{} B", bytes)
    }
}

pub(crate) fn format_rate(rate: f64) -> String {
    let int_part = rate as u64;
    let frac = ((rate - int_part as f64).abs() * 10.0).round() as u8;
    format!("{}.{}", format_num(int_part), frac)
}

pub(crate) fn get_rss_mb() -> f64 {
    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                let kb: u64 = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                return kb as f64 / 1024.0;
            }
        }
    }
    0.0
}

pub(crate) fn error_code_from_message(msg: &str) -> &'static str {
    if msg.contains("os error 13")
        || msg.contains("Permission denied")
        || msg.contains("permission denied")
    {
        "EACCES"
    } else if msg.contains("os error 2")
        || msg.contains("No such file")
        || msg.contains("no such file")
    {
        "ENOENT"
    } else if msg.contains("os error 5") {
        "EIO"
    } else {
        "EOTHER"
    }
}
