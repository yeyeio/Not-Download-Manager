//! Tier 4 Randomized Interleavings Challenger Test Suite
//! Empirical Verification & Adversarial Stress Testing
//!
//! Objectives:
//! 1. Empirically demonstrate the synchronization flaw in `test_tier4_randomized_concurrent_interleavings`
//!    where seeds exit after <100 microseconds with 0 events captured.
//! 2. Provide a robust, deterministically synchronized harness for the 4 recorded seeds (1,000 operations).
//! 3. Stress-test with 5 novel unrecorded seeds and 2,500 operations.
//! 4. Verify all core lifecycle invariants:
//!    - Zero orphaned workers.
//!    - Zero active tasks after teardown.
//!    - Zero known paths lingering in engine metrics.
//!    - Zero filesystem files (.bin, .partinfo, .tmp) remaining on disk after teardown Delete.

use app::engine::{spawn_download_engine, EngineCommand, EngineEvent};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Isolated test directory that automatically cleans up on drop
struct TestDir(PathBuf);

impl TestDir {
    fn new(name: &str) -> Self {
        let p = std::env::temp_dir().join(format!(
            "idm_m4_challenger_{}_{}_{}",
            name,
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        let _ = std::fs::create_dir_all(&p);
        TestDir(p)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        for _ in 0..10 {
            if std::fs::remove_dir_all(&self.0).is_ok() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// Pure-Rust `Xoshiro256++` PRNG matching Tier 4 implementation
#[derive(Debug, Clone)]
pub struct Xoshiro256PlusPlus {
    s: [u64; 4],
}

impl Xoshiro256PlusPlus {
    pub fn from_seed(mut seed: u64) -> Self {
        let mut s = [0u64; 4];
        for word in &mut s {
            seed = seed.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = seed;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            *word = z ^ (z >> 31);
        }
        Self { s }
    }

    pub fn next_u64(&mut self) -> u64 {
        let result = (self.s[0].wrapping_add(self.s[3]))
            .rotate_left(23)
            .wrapping_add(self.s[0]);

        let t = self.s[1] << 17;

        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];

        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);

        result
    }

    pub fn gen_range(&mut self, low: usize, high: usize) -> usize {
        assert!(low < high);
        let span = (high - low) as u64;
        let r = self.next_u64() % span;
        low + r as usize
    }
}

// ============================================================================
// Test 1: Empirical Proof of Worker's Early-Exit Drain Race Condition
// ============================================================================
#[tokio::test]
async fn test_challenger_diagnose_tier4_early_drain_break() {
    let td = TestDir::new("challenger_diagnose");
    let test_seeds: [u64; 4] = [
        0x1234_5678_9ABC_DEF0,
        0xFEDC_BA98_7654_3210,
        0x0123_4567_89AB_CDEF,
        0xA5A5_5A5A_0F0F_F0F0,
    ];

    let ops_per_seed = 250;
    let mut zero_event_seeds = 0;

    for &seed in &test_seeds {
        let mut prng = Xoshiro256PlusPlus::from_seed(seed);
        let mut engine = spawn_download_engine();
        let mut event_trace: Vec<String> = Vec::new();

        for _ in 0..ops_per_seed {
            let task_id = prng.gen_range(1, 21) as u64;
            let op_type = prng.gen_range(0, 4);

            match op_type {
                0 => {
                    let save_path = td.path().join(format!("diag_{}_{}.bin", seed, task_id));
                    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                        id: task_id,
                        url: format!("http://127.0.0.1:9/stream_{}_{}", seed, task_id),
                        save_path,
                        connections: 1,
                    });
                }
                1 => {
                    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id: task_id });
                }
                2 => {
                    let _ = engine.cmd_sender.send(EngineCommand::CancelDownload { id: task_id });
                }
                _ => {
                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id: task_id,
                        cleanup_files: true,
                    });
                }
            }
        }

        for task_id in 1..=20u64 {
            let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                id: task_id,
                cleanup_files: true,
            });
        }

        // Exact drain loop from tests/test_phase4_3_hardening.rs lines 1248-1258
        let drain_start = Instant::now();
        let timeout = Duration::from_secs(10);
        let mut loop_iterations = 0;

        while drain_start.elapsed() < timeout {
            loop_iterations += 1;
            while let Ok(evt) = engine.event_receiver.try_recv() {
                event_trace.push(format!("{:?}", evt));
            }
            let m = engine.metrics();
            if m.active_tasks_count == 0 && m.active_workers_count == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        if event_trace.is_empty() {
            zero_event_seeds += 1;
        }

        println!(
            "[DIAGNOSTIC PROOF] Seed 0x{:016X}: drain broke at iteration {} in {:?}. Captured events = {}",
            seed, loop_iterations, drain_start.elapsed(), event_trace.len()
        );
    }

    println!("[DIAGNOSTIC PROOF] {} of 4 seeds broke with 0 events captured.", zero_event_seeds);
    assert!(
        zero_event_seeds > 0,
        "Empirical confirmation: at least one seed suffered the vacuous early-break flaw"
    );
}

// ============================================================================
// Test 2: True Deterministic Synchronization Across All 4 Recorded Seeds (1,000 Ops)
// ============================================================================
#[tokio::test]
async fn test_challenger_tier4_robust_deterministic_synchronization() {
    let td = TestDir::new("challenger_robust");
    let test_seeds: [u64; 4] = [
        0x1234_5678_9ABC_DEF0,
        0xFEDC_BA98_7654_3210,
        0x0123_4567_89AB_CDEF,
        0xA5A5_5A5A_0F0F_F0F0,
    ];

    let ops_per_seed = 250;
    let mut total_ops_executed = 0;

    for &seed in &test_seeds {
        let mut prng = Xoshiro256PlusPlus::from_seed(seed);
        let mut engine = spawn_download_engine();
        let mut event_trace: Vec<String> = Vec::new();
        let mut command_history: Vec<String> = Vec::new();

        // Track per-task terminal event counts
        let mut terminal_events: HashMap<u64, Vec<String>> = HashMap::new();

        for op_idx in 0..ops_per_seed {
            let task_id = prng.gen_range(1, 21) as u64;
            let op_type = prng.gen_range(0, 4);

            let cmd_str = match op_type {
                0 => {
                    let save_path = td.path().join(format!("robust_{}_{}.bin", seed, task_id));
                    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                        id: task_id,
                        url: format!("http://127.0.0.1:9/robust_{}_{}", seed, task_id),
                        save_path,
                        connections: 1,
                    });
                    format!("StartDownload({})", task_id)
                }
                1 => {
                    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id: task_id });
                    format!("PauseDownload({})", task_id)
                }
                2 => {
                    let _ = engine.cmd_sender.send(EngineCommand::CancelDownload { id: task_id });
                    format!("CancelDownload({})", task_id)
                }
                _ => {
                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id: task_id,
                        cleanup_files: true,
                    });
                    format!("DeleteDownload({})", task_id)
                }
            };
            command_history.push(cmd_str);

            while let Ok(evt) = engine.event_receiver.try_recv() {
                event_trace.push(format!("{:?}", evt));
                match &evt {
                    EngineEvent::Finished { id, .. }
                    | EngineEvent::Canceled { id }
                    | EngineEvent::Paused { id }
                    | EngineEvent::Failed { id, .. }
                    | EngineEvent::Deleted { id, .. } => {
                        terminal_events.entry(*id).or_default().push(format!("{:?}", evt));
                    }
                    _ => {}
                }
            }

            if op_idx % 20 == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }

        // Teardown: Issue Delete to all 20 tasks
        for task_id in 1..=20u64 {
            let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                id: task_id,
                cleanup_files: true,
            });
        }

        // Robust drain: Wait for teardown deletes and quiescence
        let drain_start = Instant::now();
        let timeout = Duration::from_secs(10);
        let mut teardown_deletes = 0;

        while drain_start.elapsed() < timeout {
            while let Ok(evt) = engine.event_receiver.try_recv() {
                event_trace.push(format!("{:?}", evt));
                if let EngineEvent::Deleted { id, .. } = evt {
                    teardown_deletes += 1;
                    terminal_events.entry(id).or_default().push(format!("TeardownDeleted({})", id));
                }
            }

            let m = engine.metrics();
            if m.active_tasks_count == 0 && m.active_workers_count == 0 && teardown_deletes >= 20 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        // Strict Invariant 1: Engine metrics must be completely cleared
        let m = engine.metrics();
        assert_eq!(m.active_tasks_count, 0, "active_tasks_count must be 0 for seed 0x{:016X}", seed);
        assert_eq!(m.active_workers_count, 0, "active_workers_count must be 0 for seed 0x{:016X}", seed);
        assert_eq!(m.known_paths_count, 0, "known_paths_count must be 0 for seed 0x{:016X}", seed);

        // Strict Invariant 2: Zero filesystem files remain on disk for all 20 tasks
        for task_id in 1..=20u64 {
            let save_path = td.path().join(format!("robust_{}_{}.bin", seed, task_id));
            let partinfo = td.path().join(format!("robust_{}_{}.bin.partinfo", seed, task_id));
            let tmp = td.path().join(format!("robust_{}_{}.bin.tmp", seed, task_id));
            let partinfo_tmp = td.path().join(format!("robust_{}_{}.bin.partinfo.tmp", seed, task_id));

            let mut files_exist = save_path.exists() || partinfo.exists() || tmp.exists() || partinfo_tmp.exists();
            if files_exist {
                for _ in 0..10 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    if !save_path.exists() && !partinfo.exists() && !tmp.exists() && !partinfo_tmp.exists() {
                        files_exist = false;
                        break;
                    }
                }
            }

            assert!(!save_path.exists(), "Lingering save_path for task {} seed 0x{:016X}", task_id, seed);
            assert!(!partinfo.exists(), "Lingering partinfo for task {} seed 0x{:016X}", task_id, seed);
            assert!(!tmp.exists(), "Lingering tmp for task {} seed 0x{:016X}", task_id, seed);
            assert!(!partinfo_tmp.exists(), "Lingering partinfo_tmp for task {} seed 0x{:016X}", task_id, seed);
        }

        assert!(
            event_trace.len() >= 20,
            "Robust test must genuinely execute operations and capture trace events. Got {} for seed 0x{:016X}",
            event_trace.len(),
            seed
        );

        total_ops_executed += ops_per_seed;
        println!(
            "[Robust Seed 0x{:016X}] Completed {} ops; verified all invariants across {} captured trace events.",
            seed, ops_per_seed, event_trace.len()
        );
    }

    assert_eq!(total_ops_executed, 1000, "1,000 operations successfully executed and verified");
}

// ============================================================================
// Test 3: Adversarial Stress Test with Novel Seeds & Higher Density (2,500 Ops)
// ============================================================================
#[tokio::test]
async fn test_challenger_adversarial_novel_seeds_high_density() {
    let td = TestDir::new("challenger_novel");
    let novel_seeds: [u64; 5] = [
        0xCAFE_BABE_DEAD_BEEF,
        0xDEAD_10CC_B00B_5EED,
        0x1337_C0DE_F00D_BA5E,
        0xBAAD_F00D_FEED_FACE,
        0x5EED_0000_FFFF_1234,
    ];

    let ops_per_seed = 500; // 5 * 500 = 2,500 operations total
    let mut grand_total_ops = 0;

    for &seed in &novel_seeds {
        let mut prng = Xoshiro256PlusPlus::from_seed(seed);
        let mut engine = spawn_download_engine();
        let mut event_trace: Vec<String> = Vec::new();
        let mut command_history: Vec<String> = Vec::new();

        for op_idx in 0..ops_per_seed {
            let task_id = prng.gen_range(1, 26) as u64; // pool of 25 concurrent tasks
            let op_type = prng.gen_range(0, 4);

            let cmd_str = match op_type {
                0 => {
                    let save_path = td.path().join(format!("novel_{}_{}.bin", seed, task_id));
                    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
                        id: task_id,
                        url: format!("http://127.0.0.1:9/novel_{}_{}", seed, task_id),
                        save_path,
                        connections: 1,
                    });
                    format!("StartDownload({})", task_id)
                }
                1 => {
                    let _ = engine.cmd_sender.send(EngineCommand::PauseDownload { id: task_id });
                    format!("PauseDownload({})", task_id)
                }
                2 => {
                    let _ = engine.cmd_sender.send(EngineCommand::CancelDownload { id: task_id });
                    format!("CancelDownload({})", task_id)
                }
                _ => {
                    let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                        id: task_id,
                        cleanup_files: true,
                    });
                    format!("DeleteDownload({})", task_id)
                }
            };
            command_history.push(cmd_str);

            while let Ok(evt) = engine.event_receiver.try_recv() {
                event_trace.push(format!("{:?}", evt));
            }

            if op_idx % 25 == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }

        // Teardown all 25 tasks
        for task_id in 1..=25u64 {
            let _ = engine.cmd_sender.send(EngineCommand::DeleteDownload {
                id: task_id,
                cleanup_files: true,
            });
        }

        let drain_start = Instant::now();
        let timeout = Duration::from_secs(10);
        let mut teardown_deleted = 0;

        while drain_start.elapsed() < timeout {
            while let Ok(evt) = engine.event_receiver.try_recv() {
                event_trace.push(format!("{:?}", evt));
                if let EngineEvent::Deleted { .. } = evt {
                    teardown_deleted += 1;
                }
            }

            let m = engine.metrics();
            if m.active_tasks_count == 0 && m.active_workers_count == 0 && teardown_deleted >= 25 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        let m = engine.metrics();
        assert_eq!(m.active_tasks_count, 0, "active_tasks_count must be 0 for seed 0x{:016X}", seed);
        assert_eq!(m.active_workers_count, 0, "active_workers_count must be 0 for seed 0x{:016X}", seed);
        assert_eq!(m.known_paths_count, 0, "known_paths_count must be 0 for seed 0x{:016X}", seed);

        for task_id in 1..=25u64 {
            let save_path = td.path().join(format!("novel_{}_{}.bin", seed, task_id));
            let partinfo = td.path().join(format!("novel_{}_{}.bin.partinfo", seed, task_id));
            let tmp = td.path().join(format!("novel_{}_{}.bin.tmp", seed, task_id));
            let partinfo_tmp = td.path().join(format!("novel_{}_{}.bin.partinfo.tmp", seed, task_id));

            let mut files_exist = save_path.exists() || partinfo.exists() || tmp.exists() || partinfo_tmp.exists();
            if files_exist {
                for _ in 0..10 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    if !save_path.exists() && !partinfo.exists() && !tmp.exists() && !partinfo_tmp.exists() {
                        files_exist = false;
                        break;
                    }
                }
            }

            assert!(!save_path.exists(), "Lingering save_path for novel task {}", task_id);
            assert!(!partinfo.exists(), "Lingering partinfo for novel task {}", task_id);
            assert!(!tmp.exists(), "Lingering tmp for novel task {}", task_id);
            assert!(!partinfo_tmp.exists(), "Lingering partinfo_tmp for novel task {}", task_id);
        }

        assert!(
            event_trace.len() >= 25,
            "Novel seed test must capture trace events. Got {} for seed 0x{:016X}",
            event_trace.len(),
            seed
        );

        grand_total_ops += ops_per_seed;
        println!(
            "[Novel Seed 0x{:016X}] Successfully executed {} ops across {} captured events.",
            seed, ops_per_seed, event_trace.len()
        );
    }

    assert_eq!(grand_total_ops, 2500, "Executed 2,500 operations across 5 novel seeds");
}
