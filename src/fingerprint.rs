//! 设备指纹：形态与哈希逐字对齐官方 CLI 1.53.1。
//!
//! 与 CLI 的唯一区别是「信号值」：CLI 读真实机器（注册表 / ioreg / machine-id、网卡 MAC、
//! os.userInfo、git config），这里按 apiKey 确定性地伪造一组逼真值。
//!
//! 为什么必须由 apiKey 派生而不是随机：指纹代表「这个账号对应的那台设备」，重启、内存回收、
//! 多实例、月额度用尽停用数周后恢复，上游都应看到同一台设备；换指纹本身就是可疑信号。

use crate::util::{sha256_bytes, sha256_hex};
use serde_json::{json, Value};

/// CLI 的根盐（buildMachineFingerprint 常量 sb）
const FP_SALT: &str = "command-code:device-fingerprint:v1";

/// CPU 型号与核心数对应表（仅 Windows x64）
const FINGERPRINT_CPUS: &[(&str, u32)] = &[
    ("12th Gen Intel(R) Core(TM) i7-12650H", 10),
    ("12th Gen Intel(R) Core(TM) i5-12400F", 6),
    ("12th Gen Intel(R) Core(TM) i9-12900K", 16),
    ("13th Gen Intel(R) Core(TM) i7-13700K", 16),
    ("13th Gen Intel(R) Core(TM) i5-13600K", 14),
    ("13th Gen Intel(R) Core(TM) i9-13900K", 24),
    ("Intel(R) Core(TM) Ultra 7 155H", 16),
    ("Intel(R) Core(TM) Ultra 9 285H", 16),
    ("Intel(R) Core(TM) i9-14900K", 24),
    ("Intel(R) Core(TM) i7-14700K", 20),
    ("AMD Ryzen 7 7800X3D", 8),
    ("AMD Ryzen 9 7950X", 16),
    ("AMD Ryzen 5 7600", 6),
    ("AMD Ryzen 9 7900X", 12),
    ("AMD Ryzen 7 5800X3D", 8),
];

const FINGERPRINT_MEMS: &[u32] = &[8, 16, 24, 32, 48, 64];

const FINGERPRINT_TZS: &[&str] = &[
    "America/New_York",
    "America/Chicago",
    "America/Los_Angeles",
    "America/Toronto",
    "Europe/London",
    "Europe/Berlin",
    "Europe/Paris",
    "Europe/Moscow",
    "Asia/Shanghai",
    "Asia/Tokyo",
    "Asia/Singapore",
    "Asia/Seoul",
    "Asia/Hong_Kong",
    "Australia/Sydney",
    "Pacific/Auckland",
];

/// 随机 2~5 个 MAC
const FINGERPRINT_MAC_COUNT_RANGE: &[u32] = &[2, 3, 4, 5];

const FP_OS_USERS: &[&str] = &["dev", "user", "admin", "coder", "engineer", "work"];
const FP_MAIL_DOMAINS: &[&str] = &["gmail.com", "outlook.com", "qq.com", "163.com"];

/// 设备档案：指纹 / config.environment / config.workingDir / x-project-slug / lifecycle.os 共用同一份，
/// 避免出现「指纹说 win32、环境说 linux」这类自相矛盾，也避免把宿主机真实信息交给上游。
#[derive(Debug, Clone)]
pub struct DeviceProfile {
    pub platform: &'static str,
    pub arch: &'static str,
    pub os_release: &'static str,
    pub is_container: bool,
    pub project_dir: String,
}

impl DeviceProfile {
    pub fn new(device_project_dir: &str) -> Self {
        Self {
            platform: "win32",
            arch: "x64",
            os_release: "10.0.22631",
            is_container: false,
            project_dir: if device_project_dir.is_empty() {
                "C:\\Users\\dev\\projects\\app".to_string()
            } else {
                device_project_dir.to_string()
            },
        }
    }
}

/// 伪造信号的派生源。加 salt 可成批换身份 —— 真实账号的 key 动不了，这是逃生口。
/// 注意：哈希阶段用的是 CLI 的固定盐（FP_SALT），salt 只影响「伪造出哪台机器」。
fn fp_digest(salt: &str, api_key: &str, field: &str) -> [u8; 32] {
    sha256_bytes(format!("{salt}\0{api_key}\0{field}").as_bytes())
}

/// 从候选池确定性地挑一项：打分取最大（严格大于才替换 → 取第一个最大值，与 JS 的
/// `Buffer.compare(score, bestScore) > 0` 完全一致）。
/// 以后往池里加候选只影响「新候选恰好胜出」的那部分 key，不会像取模那样因为池长度变化让所有 key 一起换设备。
fn fp_pick_index(salt: &str, api_key: &str, field: &str, len: usize, label_of: impl Fn(usize) -> String) -> usize {
    let mut best_idx = 0usize;
    let mut best: Option<[u8; 32]> = None;
    for i in 0..len {
        let score = fp_digest(salt, api_key, &format!("{field}\0{}", label_of(i)));
        if best.map(|b| score > b).unwrap_or(true) {
            best = Some(score);
            best_idx = i;
        }
    }
    best_idx
}

/// CLI 的 hashSignal：sha256(FP_SALT + "\0" + value.toLowerCase())，空值返回 None（JSON 里被丢掉）。
fn fingerprint_hash(value: &str) -> Option<String> {
    let v = value.trim();
    if v.is_empty() {
        return None;
    }
    Some(sha256_hex(format!("{FP_SALT}\0{}", v.to_lowercase()).as_bytes()))
}

fn hex_of(salt: &str, api_key: &str, field: &str, bytes: usize) -> String {
    hex::encode(&fp_digest(salt, api_key, field)[..bytes])
}

pub fn generate_fingerprint(salt: &str, api_key: &str, profile: &DeviceProfile) -> Value {
    let cpu_idx = fp_pick_index(salt, api_key, "cpu", FINGERPRINT_CPUS.len(), |i| {
        format!("{}|{}", FINGERPRINT_CPUS[i].0, FINGERPRINT_CPUS[i].1)
    });
    let (cpu_model, cpu_count) = FINGERPRINT_CPUS[cpu_idx];

    let mem_idx = fp_pick_index(salt, api_key, "mem", FINGERPRINT_MEMS.len(), |i| {
        FINGERPRINT_MEMS[i].to_string()
    });
    let mem_gib = FINGERPRINT_MEMS[mem_idx];

    let tz_idx = fp_pick_index(salt, api_key, "timezone", FINGERPRINT_TZS.len(), |i| {
        FINGERPRINT_TZS[i].to_string()
    });
    let timezone = FINGERPRINT_TZS[tz_idx];

    let mac_count_idx = fp_pick_index(
        salt,
        api_key,
        "macCount",
        FINGERPRINT_MAC_COUNT_RANGE.len(),
        |i| FINGERPRINT_MAC_COUNT_RANGE[i].to_string(),
    );
    let mac_count = FINGERPRINT_MAC_COUNT_RANGE[mac_count_idx] as usize;

    let user_idx = fp_pick_index(salt, api_key, "osUser", FP_OS_USERS.len(), |i| {
        FP_OS_USERS[i].to_string()
    });
    let os_user = FP_OS_USERS[user_idx];

    let mail_idx = fp_pick_index(salt, api_key, "mailDomain", FP_MAIL_DOMAINS.len(), |i| {
        FP_MAIL_DOMAINS[i].to_string()
    });
    let mail_domain = FP_MAIL_DOMAINS[mail_idx];

    // Windows MachineGuid 形状：8-4-4-4-12
    let mid = hex_of(salt, api_key, "machineId", 16);
    let machine_id = format!(
        "{}-{}-{}-{}-{}",
        &mid[0..8],
        &mid[8..12],
        &mid[12..16],
        &mid[16..20],
        &mid[20..32]
    );

    let mut macs: Vec<String> = (0..mac_count)
        .map(|i| {
            let b = fp_digest(salt, api_key, &format!("mac{i}"));
            b[..6]
                .iter()
                .map(|x| format!("{x:02x}"))
                .collect::<Vec<_>>()
                .join(":")
        })
        .collect();
    macs.sort(); // CLI 对 MAC 去重后排序

    let hostname = format!(
        "DESKTOP-{}",
        hex_of(salt, api_key, "hostname", 4).to_uppercase()
    );
    let git_email = format!(
        "{}.{}@{}",
        os_user,
        hex_of(salt, api_key, "gitEmail", 3),
        mail_domain
    );

    let machine_id_hash = fingerprint_hash(&machine_id);
    let mac_hashes: Vec<String> = macs.iter().filter_map(|m| fingerprint_hash(m)).collect();
    let os_user_hash = fingerprint_hash(os_user);
    let hostname_hash = fingerprint_hash(&hostname);
    let git_email_hash = fingerprint_hash(&git_email);

    // CLI 的 thumbmark：主盐 + "\0machine\0" + join([machineId, macs.join(",")])
    // （machineId 非空时不再拼 hostname / cpuModel）
    let machine_id_trimmed = machine_id.trim();
    let thumb_seed: Vec<String> = [
        machine_id_trimmed.to_string(),
        macs.join(","),
        if machine_id_trimmed.is_empty() {
            hostname.clone()
        } else {
            String::new()
        },
        if machine_id_trimmed.is_empty() {
            cpu_model.to_string()
        } else {
            String::new()
        },
    ]
    .into_iter()
    .filter(|s| !s.is_empty())
    .collect();

    let thumbmark = sha256_hex(
        format!(
            "{FP_SALT}\0machine\0{}",
            if thumb_seed.is_empty() {
                "unknown".to_string()
            } else {
                thumb_seed.join("|")
            }
        )
        .as_bytes(),
    );

    // 注意键序：与 CLI 的 components 对象字面量顺序一致（undefined 的哈希会被 JSON.stringify 丢掉，
    // 这里的信号值保证非空，因此哈希必然存在）
    let mut components = serde_json::Map::new();
    if let Some(v) = machine_id_hash {
        components.insert("machineIdHash".into(), Value::String(v));
    }
    components.insert(
        "macHashes".to_string(),
        Value::Array(mac_hashes.into_iter().map(Value::String).collect()),
    );
    if let Some(v) = os_user_hash {
        components.insert("osUserHash".into(), Value::String(v));
    }
    if let Some(v) = hostname_hash {
        components.insert("hostnameHash".into(), Value::String(v));
    }
    if let Some(v) = git_email_hash {
        components.insert("gitEmailHash".into(), Value::String(v));
    }
    components.insert("platform".into(), json!(profile.platform));
    components.insert("arch".into(), json!(profile.arch));
    components.insert("osRelease".into(), json!(profile.os_release));
    components.insert("cpuModel".into(), json!(cpu_model));
    components.insert("cpuCount".into(), json!(cpu_count));
    components.insert("memGiB".into(), json!(mem_gib));
    components.insert("isContainer".into(), json!(profile.is_container));
    components.insert("timezone".into(), json!(timezone));
    components.insert("runtime".into(), json!("cli"));
    components.insert("collectorVersion".into(), json!(1));

    json!({
        "thumbmark": thumbmark,
        "components": Value::Object(components),
    })
}
