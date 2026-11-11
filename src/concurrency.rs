use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

/// Explicit lifecycle states for active download workers
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum WorkerState {
    Idle = 0,
    Requesting = 1,
    Downloading = 2,
    Writing = 3,
    Backoff = 4,
    Retiring = 5,
    Terminated = 6,
}

/// Structured stages of adaptive recovery after server rate limiting
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RecoveryState {
    Normal,
    Throttled,
    Backoff,
    RecoveryProbe,
    GradualRecovery,
    Stable,
}

/// Telemetry captured per worker for rolling window analysis
#[derive(Debug, Clone, Default)]
pub struct WorkerTelemetry {
    pub worker_id: usize,
    pub bytes_downloaded: u64,
    pub idle_time: Duration,
    pub request_setup_time: Duration,
    pub network_wait_time: Duration,
    pub writer_backpressure_time: Duration,
    pub rtt_samples: Vec<Duration>,
    pub request_count: usize,
    pub error_count: usize,
    pub http_429_count: usize,
    pub http_503_count: usize,
}

/// Rolling metrics snapshot evaluated by the controller
#[derive(Debug, Clone)]
pub struct TelemetrySnapshot {
    pub timestamp: Instant,
    pub aggregate_throughput_bps: f64,
    pub smoothed_throughput_bps: f64,
    pub active_concurrency: usize,
    pub target_concurrency: usize,
    pub avg_rtt: Duration,
    pub total_idle_time: Duration,
    pub total_request_setup_time: Duration,
    pub total_network_wait_time: Duration,
    pub total_writer_backpressure_time: Duration,
    pub retry_count: usize,
    pub error_rate: f64, // retries / (requests + retries)
    pub http_429_count: usize,
    pub http_503_count: usize,
    pub delta_http_429: usize,
    pub delta_http_503: usize,
    pub delta_retries: usize,
    pub delta_bytes: u64,
    pub server_throttle_until: Option<Instant>,
    pub sleeping_workers: usize,
    pub total_backoff_duration: Duration,
    pub writer_queue_depth: usize,
    pub in_flight_writes: usize,
    pub progress_ratio: f64,
    pub remaining_chunks: usize,
}

/// Structured explanation of why the controller adjusted or maintained concurrency
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DecisionReason {
    Initial,
    InitialBdpSeed { rtt_ms: u64, seeded_concurrency: usize },
    SlowStartScaleUp { gain_pct: i32, new_concurrency: usize },
    SlowStartTransitionToAimd { reason: String },
    CooldownActive,
    ThrottledHttp429,
    ThrottledHttp503,
    ThrottledCoalesced,
    ServerThrottleRecovery { restored_concurrency: usize, target_concurrency: usize },
    RecoveryProbe { probed_concurrency: usize, pre_throttle_target: usize },
    GradualRecovery { restored_concurrency: usize, target: usize },
    RecoveryStable { settled_concurrency: usize },
    HighErrorRate { error_rate_pct: u32 },
    WriterBackpressure { queue_depth: usize },
    WorkerStarvation { remaining_chunks: usize },
    ScaleUpThroughputGain { gain_pct: i32 },
    ScaleDownPlateau { drop_or_stagnation_pct: i32 },
    SteadyOptimal,
    MinConcurrencyBound,
    MaxConcurrencyBound,
    FixedStrategy,
}

/// Structured timeline point recording concurrency changes during a download
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ConcurrencyTimelineEntry {
    pub elapsed_ms: u64,
    pub concurrency: usize,
    pub throughput_mb_s: f64,
    pub reason: DecisionReason,
}

/// Structured diagnostic event emitted on every controller evaluation
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConcurrencyDecisionEvent {
    pub task_id: u64,
    pub elapsed_ms: u64,
    pub previous_concurrency: usize,
    pub target_concurrency: usize,
    pub reason: DecisionReason,
    pub aggregate_throughput_bps: f64,
    pub smoothed_throughput_bps: f64,
    pub writer_queue_depth: usize,
    pub in_flight_writes: usize,
    pub error_count: usize,
    pub avg_rtt_ms: f64,
    pub request_setup_ms: u64,
    pub network_wait_ms: u64,
    pub writer_backpressure_ms: u64,
}

/// Configuration parameters for the adaptive concurrency controller
#[derive(Debug, Clone)]
pub struct AdaptiveConfig {
    pub min_concurrency: usize,
    pub max_concurrency: usize,
    pub initial_concurrency: usize,
    pub evaluation_interval: Duration,
    pub cooldown_duration: Duration,
    pub scale_up_step: usize,
    pub scale_down_step: usize,
    pub throughput_gain_threshold: f64, // e.g. 0.05 (+5%)
    pub writer_queue_high_watermark: usize, // e.g. 200 of 256
    pub writer_bp_time_threshold: Duration, // e.g. 25ms
    pub error_rate_threshold: f64, // e.g. 0.10 (10%)
    pub enable_bdp_probe: bool,
    pub slow_start_eval_interval: Duration,
    pub slow_start_scale_up_step: usize,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            min_concurrency: 1,
            max_concurrency: 16,
            initial_concurrency: 4,
            evaluation_interval: Duration::from_millis(500),
            cooldown_duration: Duration::from_millis(1500),
            scale_up_step: 1,
            scale_down_step: 1,
            throughput_gain_threshold: 0.05,
            writer_queue_high_watermark: 200,
            writer_bp_time_threshold: Duration::from_millis(25),
            error_rate_threshold: 0.10,
            enable_bdp_probe: true,
            slow_start_eval_interval: Duration::from_millis(35),
            slow_start_scale_up_step: 2,
        }
    }
}

/// Concurrency strategy selection
#[derive(Debug, Clone)]
pub enum ConcurrencyStrategy {
    Fixed(usize),
    Adaptive(AdaptiveConfig),
}

/// Internal action state tracking for AIMD / hill-climbing decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LastAction {
    None,
    ScaledUp,
    ScaledDown,
    Maintained,
}

/// Controller phase distinguishing rapid slow-start probing from steady AIMD
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ControllerPhase {
    SlowStart,
    CongestionAvoidance,
}

/// Computes an initial concurrency recommendation based on measured network RTT
pub fn seed_initial_concurrency_from_rtt(rtt: Duration, min_c: usize, max_c: usize) -> usize {
    let rtt_ms = rtt.as_secs_f64() * 1000.0;
    let target = if rtt_ms < 15.0 {
        2 // LAN / ultra-low latency: start small, RAM-efficient
    } else if rtt_ms < 35.0 {
        4 // Moderate latency (~25ms)
    } else if rtt_ms < 75.0 {
        8 // Higher latency (~50ms): avoid connection starvation
    } else {
        12 // Long-haul / high latency (>=75ms)
    };
    target.clamp(min_c, max_c)
}

/// Adaptive Concurrency Controller maintaining state machine & EWMA smoothing
#[derive(Debug)]
pub struct AdaptiveConcurrencyController {
    pub task_id: u64,
    pub strategy: ConcurrencyStrategy,
    pub current_concurrency: usize,
    pub phase: ControllerPhase,
    pub last_decision_time: Instant,
    pub last_action: LastAction,
    pub last_action_throughput: f64,
    pub smoothed_throughput: f64,
    pub alpha: f64, // EWMA alpha (e.g. 0.3)
    pub start_time: Instant,
    pub timeline: Vec<ConcurrencyTimelineEntry>,
    pub evaluation_count: usize,
    // Phase 5.2.4 & 5.2.7: Throttling recovery state machine
    pub pre_throttle_concurrency: usize,
    pub throttle_cooldown_until: Instant,
    pub consecutive_clean_evals: usize,
    pub recovery_state: RecoveryState,
    pub consecutive_probe_successes: usize,
}

impl AdaptiveConcurrencyController {
    pub fn new(task_id: u64, strategy: ConcurrencyStrategy) -> Self {
        let initial_c = match &strategy {
            ConcurrencyStrategy::Fixed(c) => *c,
            ConcurrencyStrategy::Adaptive(cfg) => cfg.initial_concurrency.clamp(cfg.min_concurrency, cfg.max_concurrency),
        };
        let now = Instant::now();
        let initial_entry = ConcurrencyTimelineEntry {
            elapsed_ms: 0,
            concurrency: initial_c,
            throughput_mb_s: 0.0,
            reason: DecisionReason::Initial,
        };
        record_task_concurrency_timeline(task_id, initial_entry.clone());
        Self {
            task_id,
            strategy,
            current_concurrency: initial_c,
            phase: ControllerPhase::SlowStart,
            last_decision_time: now,
            last_action: LastAction::None,
            last_action_throughput: 0.0,
            smoothed_throughput: 0.0,
            alpha: 0.3,
            start_time: now,
            timeline: vec![initial_entry],
            evaluation_count: 0,
            pre_throttle_concurrency: 0,
            throttle_cooldown_until: now,
            consecutive_clean_evals: 0,
            recovery_state: RecoveryState::Normal,
            consecutive_probe_successes: 0,
        }
    }

    /// Dynamically returns the next evaluation interval based on controller phase
    pub fn current_eval_interval(&self) -> Duration {
        match &self.strategy {
            ConcurrencyStrategy::Fixed(_) => Duration::from_millis(150),
            ConcurrencyStrategy::Adaptive(cfg) => match self.phase {
                ControllerPhase::SlowStart => cfg.slow_start_eval_interval,
                ControllerPhase::CongestionAvoidance => cfg.evaluation_interval,
            },
        }
    }

    /// Seeds initial concurrency from measured initial probe RTT before workers are spawned
    pub fn init_with_rtt(&mut self, rtt: Duration) {
        if let ConcurrencyStrategy::Adaptive(cfg) = &self.strategy {
            if cfg.enable_bdp_probe {
                let seeded = seed_initial_concurrency_from_rtt(rtt, cfg.min_concurrency, cfg.max_concurrency)
                    .max(cfg.initial_concurrency);
                if seeded != self.current_concurrency {
                    let rtt_ms = (rtt.as_secs_f64() * 1000.0).round() as u64;
                    self.current_concurrency = seeded;
                    let entry = ConcurrencyTimelineEntry {
                        elapsed_ms: self.start_time.elapsed().as_millis() as u64,
                        concurrency: seeded,
                        throughput_mb_s: 0.0,
                        reason: DecisionReason::InitialBdpSeed { rtt_ms, seeded_concurrency: seeded },
                    };
                    self.timeline.push(entry.clone());
                    record_task_concurrency_timeline(self.task_id, entry);
                }
            }
        }
    }
    /// Evaluates telemetry and outputs the new target concurrency and structured decision event
    pub fn evaluate(&mut self, snapshot: &TelemetrySnapshot) -> ConcurrencyDecisionEvent {
        self.evaluation_count += 1;
        let elapsed_ms = self.start_time.elapsed().as_millis() as u64;
        let prev_c = self.current_concurrency;

        // Update EWMA smoothed throughput
        if self.smoothed_throughput <= 0.0 {
            self.smoothed_throughput = snapshot.aggregate_throughput_bps;
        } else {
            self.smoothed_throughput = self.alpha * snapshot.aggregate_throughput_bps
                + (1.0 - self.alpha) * self.smoothed_throughput;
        }

        let strategy = self.strategy.clone();
        let (target_c, reason) = match &strategy {
            ConcurrencyStrategy::Fixed(c) => (*c, DecisionReason::FixedStrategy),
            ConcurrencyStrategy::Adaptive(cfg) => {
                self.decide_adaptive(cfg, snapshot)
            }
        };

        self.current_concurrency = target_c;

        let timeline_entry = ConcurrencyTimelineEntry {
            elapsed_ms,
            concurrency: target_c,
            throughput_mb_s: snapshot.aggregate_throughput_bps / (1024.0 * 1024.0),
            reason: reason.clone(),
        };
        self.timeline.push(timeline_entry.clone());
        record_task_concurrency_timeline(self.task_id, timeline_entry);

        ConcurrencyDecisionEvent {
            task_id: self.task_id,
            elapsed_ms,
            previous_concurrency: prev_c,
            target_concurrency: target_c,
            reason,
            aggregate_throughput_bps: snapshot.aggregate_throughput_bps,
            smoothed_throughput_bps: self.smoothed_throughput,
            writer_queue_depth: snapshot.writer_queue_depth,
            in_flight_writes: snapshot.in_flight_writes,
            error_count: snapshot.retry_count,
            avg_rtt_ms: snapshot.avg_rtt.as_secs_f64() * 1000.0,
            request_setup_ms: snapshot.total_request_setup_time.as_millis() as u64,
            network_wait_ms: snapshot.total_network_wait_time.as_millis() as u64,
            writer_backpressure_ms: snapshot.total_writer_backpressure_time.as_millis() as u64,
        }
    }

    fn decide_adaptive(&mut self, cfg: &AdaptiveConfig, snapshot: &TelemetrySnapshot) -> (usize, DecisionReason) {
        let now = Instant::now();

        // 1. Emergency Server Throttling (HTTP 429 or HTTP 503)
        let has_429 = snapshot.delta_http_429 > 0;
        let has_503 = snapshot.delta_http_503 > 0;

        if has_429 || has_503 {
            self.consecutive_clean_evals = 0;
            self.consecutive_probe_successes = 0;
            self.phase = ControllerPhase::CongestionAvoidance;

            // Deduplication (Objective 6): if multiple workers received 429/503 from the same burst
            // and we already backed off recently, coalesce and avoid repeatedly halving
            if now < self.throttle_cooldown_until && self.recovery_state == RecoveryState::Throttled {
                self.last_action = LastAction::Maintained;
                return (self.current_concurrency, DecisionReason::ThrottledCoalesced);
            }

            self.pre_throttle_concurrency = self.pre_throttle_concurrency
                .max(self.current_concurrency)
                .max(cfg.initial_concurrency);

            let new_c = (self.current_concurrency / 2).max(cfg.min_concurrency);
            self.last_decision_time = now;
            let cooldown = if let Some(deadline) = snapshot.server_throttle_until {
                deadline.max(now + cfg.cooldown_duration)
            } else {
                now + cfg.cooldown_duration
            };
            self.throttle_cooldown_until = cooldown;
            self.recovery_state = RecoveryState::Throttled;
            self.last_action = LastAction::ScaledDown;
            self.last_action_throughput = self.smoothed_throughput;

            let reason = if has_429 {
                DecisionReason::ThrottledHttp429
            } else {
                DecisionReason::ThrottledHttp503
            };
            return (new_c, reason);
        }

        // 2. Recovery State Machine (Phase 5.2.7)
        match self.recovery_state {
            RecoveryState::Throttled | RecoveryState::Backoff => {
                if snapshot.sleeping_workers > 0 && now < self.throttle_cooldown_until {
                    // Check if healthy workers are actively completing chunks cleanly
                    if snapshot.delta_bytes > 0 && snapshot.delta_retries == 0 && self.evaluation_count > 1 {
                        self.consecutive_clean_evals += 1;
                        if self.consecutive_clean_evals >= 2 && self.pre_throttle_concurrency > self.current_concurrency {
                            // Healthy workers confirmed server recovery! Launch bounded probe
                            self.recovery_state = RecoveryState::RecoveryProbe;
                            let probed = (self.current_concurrency + 1)
                                .min(self.pre_throttle_concurrency)
                                .min(cfg.max_concurrency);
                            self.last_decision_time = now;
                            self.last_action = LastAction::ScaledUp;
                            self.consecutive_clean_evals = 0;
                            return (
                                probed,
                                DecisionReason::RecoveryProbe {
                                    probed_concurrency: probed,
                                    pre_throttle_target: self.pre_throttle_concurrency,
                                },
                            );
                        }
                    }
                    self.recovery_state = RecoveryState::Backoff;
                    self.last_action = LastAction::Maintained;
                    return (self.current_concurrency, DecisionReason::CooldownActive);
                } else if now >= self.throttle_cooldown_until && self.pre_throttle_concurrency > self.current_concurrency {
                    // Backoff timer expired: launch recovery probe!
                    self.recovery_state = RecoveryState::RecoveryProbe;
                    let probed = (self.current_concurrency + 1)
                        .min(self.pre_throttle_concurrency)
                        .min(cfg.max_concurrency);
                    self.last_decision_time = now;
                    self.last_action = LastAction::ScaledUp;
                    self.consecutive_clean_evals = 0;
                    return (
                        probed,
                        DecisionReason::RecoveryProbe {
                            probed_concurrency: probed,
                            pre_throttle_target: self.pre_throttle_concurrency,
                        },
                    );
                } else if now < self.throttle_cooldown_until {
                    self.last_action = LastAction::Maintained;
                    return (self.current_concurrency, DecisionReason::CooldownActive);
                } else {
                    self.recovery_state = RecoveryState::Normal;
                }
            }
            RecoveryState::RecoveryProbe => {
                if snapshot.delta_retries == 0 && snapshot.delta_bytes > 0 {
                    // Probe succeeded! Move to GradualRecovery
                    self.recovery_state = RecoveryState::GradualRecovery;
                    self.consecutive_probe_successes += 1;
                    let step = cfg.slow_start_scale_up_step.min(2);
                    let next_c = (self.current_concurrency + step)
                        .min(self.pre_throttle_concurrency)
                        .min(cfg.max_concurrency);
                    self.last_decision_time = now;
                    self.last_action = LastAction::ScaledUp;

                    if next_c >= self.pre_throttle_concurrency {
                        self.recovery_state = RecoveryState::Stable;
                        self.pre_throttle_concurrency = 0;
                        return (
                            next_c,
                            DecisionReason::RecoveryStable {
                                settled_concurrency: next_c,
                            },
                        );
                    } else {
                        return (
                            next_c,
                            DecisionReason::GradualRecovery {
                                restored_concurrency: next_c,
                                target: self.pre_throttle_concurrency,
                            },
                        );
                    }
                } else {
                    self.last_action = LastAction::Maintained;
                    return (self.current_concurrency, DecisionReason::CooldownActive);
                }
            }
            RecoveryState::GradualRecovery => {
                if snapshot.delta_retries == 0 && snapshot.delta_bytes > 0 {
                    self.consecutive_probe_successes += 1;
                    let step = cfg.slow_start_scale_up_step.min(2);
                    let next_c = (self.current_concurrency + step)
                        .min(self.pre_throttle_concurrency)
                        .min(cfg.max_concurrency);
                    self.last_decision_time = now;
                    self.last_action = LastAction::ScaledUp;

                    if next_c >= self.pre_throttle_concurrency {
                        self.recovery_state = RecoveryState::Stable;
                        self.pre_throttle_concurrency = 0;
                        return (
                            next_c,
                            DecisionReason::RecoveryStable {
                                settled_concurrency: next_c,
                            },
                        );
                    } else {
                        return (
                            next_c,
                            DecisionReason::GradualRecovery {
                                restored_concurrency: next_c,
                                target: self.pre_throttle_concurrency,
                            },
                        );
                    }
                } else {
                    self.last_action = LastAction::Maintained;
                    return (self.current_concurrency, DecisionReason::CooldownActive);
                }
            }
            RecoveryState::Stable => {
                self.consecutive_clean_evals += 1;
                if self.consecutive_clean_evals >= 2 {
                    self.recovery_state = RecoveryState::Normal;
                    self.pre_throttle_concurrency = 0;
                }
                self.last_action = LastAction::Maintained;
                return (self.current_concurrency, DecisionReason::SteadyOptimal);
            }
            RecoveryState::Normal => {}
        }

        // 3. Writer Backpressure Check: High queue depth or high backpressure stall
        if snapshot.writer_queue_depth >= cfg.writer_queue_high_watermark
            || snapshot.total_writer_backpressure_time >= cfg.writer_bp_time_threshold
        {
            self.last_decision_time = now;
            self.phase = ControllerPhase::CongestionAvoidance;
            // If writer is severely backed up (> 230 items), scale down by 1
            if snapshot.writer_queue_depth > 230 && self.current_concurrency > cfg.min_concurrency {
                let new_c = self.current_concurrency.saturating_sub(1).max(cfg.min_concurrency);
                self.last_action = LastAction::ScaledDown;
                self.last_action_throughput = self.smoothed_throughput;
                return (new_c, DecisionReason::WriterBackpressure { queue_depth: snapshot.writer_queue_depth });
            } else {
                self.last_action = LastAction::Maintained;
                return (self.current_concurrency, DecisionReason::WriterBackpressure { queue_depth: snapshot.writer_queue_depth });
            }
        }

        // 4. High Error Rate / Network Dropouts
        if snapshot.error_rate >= cfg.error_rate_threshold && snapshot.retry_count > 0 {
            self.last_decision_time = now;
            self.phase = ControllerPhase::CongestionAvoidance;
            let new_c = self.current_concurrency.saturating_sub(cfg.scale_down_step).max(cfg.min_concurrency);
            self.last_action = LastAction::ScaledDown;
            self.last_action_throughput = self.smoothed_throughput;
            return (new_c, DecisionReason::HighErrorRate { error_rate_pct: (snapshot.error_rate * 100.0) as u32 });
        }

        // 5. Worker Starvation Check: remaining chunks < current concurrency
        if snapshot.remaining_chunks > 0 && snapshot.remaining_chunks < self.current_concurrency {
            self.last_decision_time = now;
            self.last_action = LastAction::Maintained;
            return (self.current_concurrency, DecisionReason::WorkerStarvation { remaining_chunks: snapshot.remaining_chunks });
        }

        // 6. Controller Phase Handling: SlowStart vs CongestionAvoidance
        match self.phase {
            ControllerPhase::SlowStart => {
                if self.last_action_throughput <= 0.0 {
                    // First slow-start sample: record baseline throughput and ramp up
                    self.last_decision_time = now;
                    self.last_action = LastAction::ScaledUp;
                    self.last_action_throughput = snapshot.aggregate_throughput_bps;

                    if self.current_concurrency < cfg.max_concurrency {
                        let new_c = (self.current_concurrency + cfg.slow_start_scale_up_step).min(cfg.max_concurrency);
                        (new_c, DecisionReason::SlowStartScaleUp { gain_pct: 100, new_concurrency: new_c })
                    } else {
                        self.phase = ControllerPhase::CongestionAvoidance;
                        (self.current_concurrency, DecisionReason::MaxConcurrencyBound)
                    }
                } else {
                    let baseline = self.last_action_throughput;
                    let current = snapshot.aggregate_throughput_bps;
                    let gain = if baseline > 0.0 { (current - baseline) / baseline } else { 0.0 };

                    if gain >= 0.10 {
                        // Healthy throughput gain during slow-start (>=10%): continue aggressive probe!
                        self.last_decision_time = now;
                        self.last_action = LastAction::ScaledUp;
                        self.last_action_throughput = current;

                        if self.current_concurrency < cfg.max_concurrency {
                            let new_c = (self.current_concurrency + cfg.slow_start_scale_up_step).min(cfg.max_concurrency);
                            (new_c, DecisionReason::SlowStartScaleUp { gain_pct: (gain * 100.0) as i32, new_concurrency: new_c })
                        } else {
                            self.phase = ControllerPhase::CongestionAvoidance;
                            (self.current_concurrency, DecisionReason::MaxConcurrencyBound)
                        }
                    } else {
                        // Plateau or drop detected in slow-start (<10% gain): transition to AIMD CongestionAvoidance!
                        self.phase = ControllerPhase::CongestionAvoidance;
                        self.last_decision_time = now;
                        self.last_action_throughput = self.smoothed_throughput;

                        if gain < -0.05 && self.current_concurrency > cfg.min_concurrency {
                            let new_c = (self.current_concurrency - 1).max(cfg.min_concurrency);
                            self.last_action = LastAction::ScaledDown;
                            (new_c, DecisionReason::SlowStartTransitionToAimd { reason: format!("Plateau/Drop ({:.1}%), backing off 1 worker", gain * 100.0) })
                        } else {
                            self.last_action = LastAction::Maintained;
                            (self.current_concurrency, DecisionReason::SlowStartTransitionToAimd { reason: format!("Plateau reached ({:.1}% gain), locking steady", gain * 100.0) })
                        }
                    }
                }
            }
            ControllerPhase::CongestionAvoidance => {
                // Cooldown check in CongestionAvoidance between regular adjustments
                if self.last_decision_time.elapsed() < cfg.cooldown_duration {
                    return (self.current_concurrency, DecisionReason::CooldownActive);
                }

                // 7. Throughput Gradient / Hill-Climbing Assessment in AIMD
                match self.last_action {
                    LastAction::ScaledUp => {
                        let baseline = self.last_action_throughput;
                        let current = self.smoothed_throughput;
                        let gain = if baseline > 0.0 { (current - baseline) / baseline } else { 0.0 };

                        if gain >= cfg.throughput_gain_threshold {
                            // Successful scale up: throughput improved significantly!
                            if self.current_concurrency < cfg.max_concurrency {
                                let new_c = (self.current_concurrency + cfg.scale_up_step).min(cfg.max_concurrency);
                                self.last_decision_time = now;
                                self.last_action = LastAction::ScaledUp;
                                self.last_action_throughput = current;
                                (new_c, DecisionReason::ScaleUpThroughputGain { gain_pct: (gain * 100.0) as i32 })
                            } else {
                                self.last_decision_time = now;
                                self.last_action = LastAction::Maintained;
                                (self.current_concurrency, DecisionReason::MaxConcurrencyBound)
                            }
                        } else if gain < -0.05 {
                            // True throughput regression after scaling up: revert step!
                            let drop_pct = (gain * 100.0) as i32;
                            let new_c = self.current_concurrency.saturating_sub(cfg.scale_down_step).max(cfg.min_concurrency);
                            self.last_decision_time = now;
                            self.last_action = LastAction::ScaledDown;
                            self.last_action_throughput = current;
                            (new_c, DecisionReason::ScaleDownPlateau { drop_or_stagnation_pct: drop_pct })
                        } else {
                            // Throughput plateaued without regression: maintain current optimal concurrency stably!
                            self.last_decision_time = now;
                            self.last_action = LastAction::Maintained;
                            self.last_action_throughput = current;
                            (self.current_concurrency, DecisionReason::SteadyOptimal)
                        }
                    }
                    LastAction::ScaledDown => {
                        // After scaling down, if throughput stayed essentially identical or improved, stay steady
                        self.last_decision_time = now;
                        self.last_action = LastAction::Maintained;
                        self.last_action_throughput = self.smoothed_throughput;
                        (self.current_concurrency, DecisionReason::SteadyOptimal)
                    }
                    LastAction::None | LastAction::Maintained => {
                        // If steady and healthy with room to grow, probe scale-up
                        if self.current_concurrency < cfg.max_concurrency {
                            let new_c = (self.current_concurrency + cfg.scale_up_step).min(cfg.max_concurrency);
                            self.last_decision_time = now;
                            self.last_action = LastAction::ScaledUp;
                            self.last_action_throughput = self.smoothed_throughput;
                            (new_c, DecisionReason::ScaleUpThroughputGain { gain_pct: 0 })
                        } else {
                            self.last_decision_time = now;
                            (self.current_concurrency, DecisionReason::MaxConcurrencyBound)
                        }
                    }
                }
            }
        }
    }
}

/// Per-task telemetry accumulator thread-safe aggregator
#[derive(Debug)]
pub struct TaskTelemetryCollector {
    pub task_id: u64,
    workers: Mutex<HashMap<usize, WorkerTelemetry>>,
    writer_queue_depth: AtomicUsize,
    in_flight_writes: AtomicUsize,
    last_sample_time: Mutex<Instant>,
    last_committed_bytes: AtomicU64,
    last_snapshot_reqs: AtomicUsize,
    last_snapshot_429: AtomicUsize,
    last_snapshot_503: AtomicUsize,
    last_snapshot_retries: AtomicUsize,
    server_throttle_until: Mutex<Option<Instant>>,
    sleeping_workers: AtomicUsize,
    total_backoff_ms: AtomicU64,
    worker_states: [AtomicU8; 32],
}

impl TaskTelemetryCollector {
    pub fn new(task_id: u64) -> Self {
        const INIT_ATOMIC_U8: AtomicU8 = AtomicU8::new(0);
        Self {
            task_id,
            workers: Mutex::new(HashMap::new()),
            writer_queue_depth: AtomicUsize::new(0),
            in_flight_writes: AtomicUsize::new(0),
            last_sample_time: Mutex::new(Instant::now()),
            last_committed_bytes: AtomicU64::new(0),
            last_snapshot_reqs: AtomicUsize::new(0),
            last_snapshot_429: AtomicUsize::new(0),
            last_snapshot_503: AtomicUsize::new(0),
            last_snapshot_retries: AtomicUsize::new(0),
            server_throttle_until: Mutex::new(None),
            sleeping_workers: AtomicUsize::new(0),
            total_backoff_ms: AtomicU64::new(0),
            worker_states: [INIT_ATOMIC_U8; 32],
        }
    }

    pub fn set_worker_state(&self, worker_id: usize, state: WorkerState) {
        if worker_id < 32 {
            self.worker_states[worker_id].store(state as u8, Ordering::SeqCst);
        }
    }

    pub fn get_worker_state(&self, worker_id: usize) -> WorkerState {
        if worker_id < 32 {
            match self.worker_states[worker_id].load(Ordering::SeqCst) {
                0 => WorkerState::Idle,
                1 => WorkerState::Requesting,
                2 => WorkerState::Downloading,
                3 => WorkerState::Writing,
                4 => WorkerState::Backoff,
                5 => WorkerState::Retiring,
                _ => WorkerState::Terminated,
            }
        } else {
            WorkerState::Terminated
        }
    }

    pub fn count_worker_states(&self, active_worker_count: usize) -> (usize, usize, usize, usize) {
        let mut network_active = 0;
        let mut writing = 0;
        let mut backoff = 0;
        let mut idle_or_retiring = 0;
        let limit = if active_worker_count > 0 {
            active_worker_count.min(32)
        } else {
            32
        };
        for i in 0..limit {
            match self.worker_states[i].load(Ordering::SeqCst) {
                1 | 2 => network_active += 1,  // Requesting, Downloading
                3 => writing += 1,              // Writing
                4 => backoff += 1,              // Backoff
                0 | 5 => idle_or_retiring += 1, // Idle, Retiring
                _ => {}                         // Terminated (6) or unallocated (>6) do not count
            }
        }
        (network_active, writing, backoff, idle_or_retiring)
    }

    pub fn active_network_transfers(&self, active_worker_count: usize) -> usize {
        let (network_active, _, _, _) = self.count_worker_states(active_worker_count);
        network_active
    }

    pub fn record_bytes(&self, worker_id: usize, bytes: u64) {
        let mut map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        let w = map.entry(worker_id).or_default();
        w.worker_id = worker_id;
        w.bytes_downloaded += bytes;
    }

    pub fn record_idle(&self, worker_id: usize, dur: Duration) {
        let mut map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        let w = map.entry(worker_id).or_default();
        w.worker_id = worker_id;
        w.idle_time += dur;
    }

    pub fn record_request_setup(&self, worker_id: usize, dur: Duration) {
        let mut map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        let w = map.entry(worker_id).or_default();
        w.worker_id = worker_id;
        w.request_setup_time += dur;
    }

    pub fn record_network_wait(&self, worker_id: usize, dur: Duration) {
        let mut map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        let w = map.entry(worker_id).or_default();
        w.worker_id = worker_id;
        w.network_wait_time += dur;
    }

    pub fn record_writer_backpressure(&self, worker_id: usize, dur: Duration) {
        let mut map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        let w = map.entry(worker_id).or_default();
        w.worker_id = worker_id;
        w.writer_backpressure_time += dur;
    }

    pub fn record_rtt(&self, worker_id: usize, rtt: Duration) {
        let mut map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        let w = map.entry(worker_id).or_default();
        w.worker_id = worker_id;
        w.request_count += 1;
        w.rtt_samples.push(rtt);
        if w.rtt_samples.len() > 32 {
            w.rtt_samples.remove(0);
        }
    }

    pub fn record_retry_or_error(&self, worker_id: usize) {
        let mut map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        let w = map.entry(worker_id).or_default();
        w.worker_id = worker_id;
        w.error_count += 1;
    }

    pub fn record_http_429(&self, worker_id: usize) {
        let mut map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        let w = map.entry(worker_id).or_default();
        w.worker_id = worker_id;
        w.http_429_count += 1;
        w.request_count += 1;
    }

    pub fn record_http_503(&self, worker_id: usize) {
        let mut map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        let w = map.entry(worker_id).or_default();
        w.worker_id = worker_id;
        w.http_503_count += 1;
        w.request_count += 1;
    }

    pub fn record_throttle_backoff(&self, backoff: Duration) {
        let deadline = Instant::now() + backoff;
        let mut lock = self.server_throttle_until.lock().unwrap_or_else(|p| p.into_inner());
        match *lock {
            Some(cur) => {
                if deadline > cur {
                    *lock = Some(deadline);
                }
            }
            None => {
                *lock = Some(deadline);
            }
        }
    }

    pub fn record_backoff_start(&self, backoff: Duration) {
        self.sleeping_workers.fetch_add(1, Ordering::SeqCst);
        self.total_backoff_ms.fetch_add(backoff.as_millis() as u64, Ordering::Relaxed);
        self.record_throttle_backoff(backoff);
    }

    pub fn record_backoff_end(&self) {
        loop {
            let cur = self.sleeping_workers.load(Ordering::Relaxed);
            if cur == 0 {
                break;
            }
            if self
                .sleeping_workers
                .compare_exchange_weak(cur, cur - 1, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    pub fn record_backoff_start_worker(&self, worker_id: usize, backoff: Duration) {
        self.record_backoff_start(backoff);
        self.set_worker_state(worker_id, WorkerState::Backoff);
    }

    pub fn record_backoff_end_worker(&self, worker_id: usize) {
        self.record_backoff_end();
        self.set_worker_state(worker_id, WorkerState::Idle);
    }

    pub fn sleeping_workers(&self) -> usize {
        self.sleeping_workers.load(Ordering::Relaxed)
    }

    pub fn total_backoff_duration(&self) -> Duration {
        Duration::from_millis(self.total_backoff_ms.load(Ordering::Relaxed))
    }

    pub fn update_writer_metrics(&self, queue_depth: usize, in_flight: usize) {
        self.writer_queue_depth.store(queue_depth, Ordering::Relaxed);
        self.in_flight_writes.store(in_flight, Ordering::Relaxed);
    }

    /// Takes a rolling snapshot of metrics for controller evaluation
    pub fn take_snapshot(
        &self,
        current_committed: u64,
        total_size: Option<u64>,
        remaining_chunks: usize,
        active_concurrency: usize,
        target_concurrency: usize,
    ) -> TelemetrySnapshot {
        let now = Instant::now();
        let mut last_time = self.last_sample_time.lock().unwrap_or_else(|p| p.into_inner());
        let dt = now.duration_since(*last_time).as_secs_f64().max(0.001);
        *last_time = now;

        let prev_committed = self.last_committed_bytes.swap(current_committed, Ordering::SeqCst);
        let delta_bytes = current_committed.saturating_sub(prev_committed);
        let aggregate_throughput_bps = delta_bytes as f64 / dt;

        let map = self.workers.lock().unwrap_or_else(|p| p.into_inner());
        let mut total_idle = Duration::ZERO;
        let mut total_req_setup = Duration::ZERO;
        let mut total_net_wait = Duration::ZERO;
        let mut total_bp = Duration::ZERO;
        let mut rtt_sum = Duration::ZERO;
        let mut rtt_count = 0usize;
        let mut req_count = 0usize;
        let mut err_count = 0usize;
        let mut c_429 = 0usize;
        let mut c_503 = 0usize;

        for w in map.values() {
            total_idle += w.idle_time;
            total_req_setup += w.request_setup_time;
            total_net_wait += w.network_wait_time;
            total_bp += w.writer_backpressure_time;
            req_count += w.request_count;
            err_count += w.error_count;
            c_429 += w.http_429_count;
            c_503 += w.http_503_count;
            for r in &w.rtt_samples {
                rtt_sum += *r;
                rtt_count += 1;
            }
        }

        let avg_rtt = if rtt_count > 0 {
            rtt_sum / rtt_count as u32
        } else {
            Duration::ZERO
        };

        let delta_http_429 = c_429.saturating_sub(self.last_snapshot_429.swap(c_429, Ordering::SeqCst));
        let delta_http_503 = c_503.saturating_sub(self.last_snapshot_503.swap(c_503, Ordering::SeqCst));
        let delta_retries = err_count.saturating_sub(self.last_snapshot_retries.swap(err_count, Ordering::SeqCst));
        let delta_req = req_count.saturating_sub(self.last_snapshot_reqs.swap(req_count, Ordering::SeqCst));

        let delta_ops = delta_req + delta_retries;
        let interval_error_rate = if delta_ops > 0 {
            delta_retries as f64 / delta_ops as f64
        } else {
            0.0
        };

        let server_throttle_until = self.server_throttle_until.lock().unwrap_or_else(|p| p.into_inner()).clone();

        let progress_ratio = match total_size {
            Some(tot) if tot > 0 => current_committed as f64 / tot as f64,
            _ => 0.0,
        };

        TelemetrySnapshot {
            timestamp: now,
            aggregate_throughput_bps,
            smoothed_throughput_bps: aggregate_throughput_bps, // initialized by controller
            active_concurrency,
            target_concurrency,
            avg_rtt,
            total_idle_time: total_idle,
            total_request_setup_time: total_req_setup,
            total_network_wait_time: total_net_wait,
            total_writer_backpressure_time: total_bp,
            retry_count: err_count,
            error_rate: interval_error_rate,
            http_429_count: c_429,
            http_503_count: c_503,
            delta_http_429,
            delta_http_503,
            delta_retries,
            delta_bytes,
            server_throttle_until,
            sleeping_workers: self.sleeping_workers.load(Ordering::Relaxed),
            total_backoff_duration: Duration::from_millis(self.total_backoff_ms.load(Ordering::Relaxed)),
            writer_queue_depth: self.writer_queue_depth.load(Ordering::Relaxed),
            in_flight_writes: self.in_flight_writes.load(Ordering::Relaxed),
            progress_ratio,
            remaining_chunks,
        }
    }
}

/// RAII guard ensuring every backoff increment has exactly one matching decrement,
/// even when a worker task is cancelled, dropped, or unwound during sleep.
pub struct BackoffGuard {
    telemetry: std::sync::Arc<TaskTelemetryCollector>,
    worker_id: usize,
    active: bool,
}

impl BackoffGuard {
    pub fn new(telemetry: std::sync::Arc<TaskTelemetryCollector>, worker_id: usize, backoff: Duration) -> Self {
        telemetry.record_backoff_start_worker(worker_id, backoff);
        Self {
            telemetry,
            worker_id,
            active: true,
        }
    }
}

impl Drop for BackoffGuard {
    fn drop(&mut self) {
        if self.active {
            self.telemetry.record_backoff_end_worker(self.worker_id);
            self.active = false;
        }
    }
}

/// Coherent, indivisible snapshot of WorkerSlotPool state taken in a single atomic operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotPoolSnapshot {
    pub raw_state: u64,
    pub allocated_mask: u32,
    pub retiring_mask: u32,
    pub allocated_count: usize,
    pub retiring_count: usize,
    pub active_count: usize,
}

/// Thread-safe reusable slot pool for bounded worker slots (0..MAX_WORKERS, where MAX_WORKERS <= 32).
/// Uses a single packed AtomicU64 (lower 32 bits = allocated_mask, upper 32 bits = retiring_mask).
/// This guarantees that every query and mutation across allocated and retiring masks is an
/// indivisible, 100% coherent atomic transaction with zero window for intermediate inconsistent states.
#[derive(Debug)]
pub struct WorkerSlotPool {
    state: AtomicU64,
    max_slots: usize,
}

impl WorkerSlotPool {
    pub fn new(max_slots: usize) -> Self {
        assert!(max_slots > 0 && max_slots <= 32, "max_slots must be between 1 and 32");
        Self {
            state: AtomicU64::new(0),
            max_slots,
        }
    }

    /// Atomically acquire an available slot ID.
    /// Sets bit `i` in the allocated mask (lower 32) and clears bit `i` in the retiring mask (upper 32)
    /// in a single atomic compare-exchange transaction.
    pub fn acquire_slot(&self) -> Option<usize> {
        let mut current = self.state.load(Ordering::SeqCst);
        loop {
            let alloc_mask = (current & 0xFFFF_FFFF) as u32;
            let mut found = None;
            for i in 0..self.max_slots {
                if (alloc_mask & (1 << i)) == 0 {
                    found = Some(i);
                    break;
                }
            }
            let slot = match found {
                Some(s) => s,
                None => return None, // All slots currently in use
            };

            let next_alloc = (alloc_mask | (1 << slot)) as u64;
            let next_retire = ((current >> 32) as u32 & !(1 << slot)) as u64;
            let next = (next_retire << 32) | next_alloc;

            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return Some(slot),
                Err(actual) => current = actual,
            }
        }
    }

    /// Atomically mark a slot as retiring (still allocated, but exiting via scale-down).
    /// Sets bit `slot` in the retiring mask (upper 32 bits) via compare-exchange.
    /// Returns true if the slot was active and successfully transitioned to retiring;
    /// returns false if the slot was not allocated or was already retiring.
    pub fn mark_retiring(&self, slot: usize) -> bool {
        if slot >= self.max_slots {
            return false;
        }
        let slot_bit = 1u64 << slot;
        let retiring_bit = slot_bit << 32;

        let mut current = self.state.load(Ordering::SeqCst);
        loop {
            let alloc_mask = (current & 0xFFFF_FFFF) as u32;
            let ret_mask = (current >> 32) as u32;

            if (alloc_mask & (1 << slot)) == 0 || (ret_mask & (1 << slot)) != 0 {
                return false; // Not allocated or already retiring
            }

            let next = current | retiring_bit;
            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Atomically retire a slot only if active workers strictly exceed `target_active` and `active > 1`.
    /// Performs an indivisible CAS on the single packed AtomicU64 state.
    /// Returns true if the slot was successfully committed to retirement; returns false otherwise.
    pub fn try_retire_slot(&self, slot: usize, target_active: usize) -> bool {
        if slot >= self.max_slots {
            return false;
        }
        let slot_bit = 1u64 << slot;
        let retiring_bit = slot_bit << 32;

        let mut current = self.state.load(Ordering::SeqCst);
        loop {
            let alloc_mask = (current & 0xFFFF_FFFF) as u32;
            let ret_mask = (current >> 32) as u32;

            // Slot must be allocated and not already retiring
            if (alloc_mask & (1 << slot)) == 0 || (ret_mask & (1 << slot)) != 0 {
                return false;
            }

            // Derive active worker count directly from this atomic snapshot
            let active_count = (alloc_mask & !ret_mask).count_ones() as usize;
            if active_count <= target_active || active_count <= 1 {
                return false;
            }

            let next = current | retiring_bit;
            match self.state.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }

    /// Atomically release a slot ID back to the pool.
    /// Clears both allocated bit (lower 32) and retiring bit (upper 32) in a single atomic transaction.
    pub fn release_slot(&self, slot: usize) {
        if slot < 32 {
            let clear_mask = !((1u64 << slot) | ((1u64 << slot) << 32));
            self.state.fetch_and(clear_mask, Ordering::SeqCst);
        }
    }

    /// Takes a 100% indivisible, atomic snapshot of all slot pool state.
    /// The equation `allocated_count == active_count + retiring_count` is guaranteed to hold unconditionally.
    pub fn snapshot(&self) -> SlotPoolSnapshot {
        let raw = self.state.load(Ordering::SeqCst);
        let allocated_mask = (raw & 0xFFFF_FFFF) as u32;
        let retiring_mask = (raw >> 32) as u32;
        let allocated_count = allocated_mask.count_ones() as usize;
        let retiring_count = retiring_mask.count_ones() as usize;
        let active_count = (allocated_mask & !retiring_mask).count_ones() as usize;
        SlotPoolSnapshot {
            raw_state: raw,
            allocated_mask,
            retiring_mask,
            allocated_count,
            retiring_count,
            active_count,
        }
    }

    /// Check if a slot is currently allocated (active or retiring).
    pub fn is_allocated(&self, slot: usize) -> bool {
        if slot < 32 {
            (self.state.load(Ordering::SeqCst) & (1u64 << slot)) != 0
        } else {
            false
        }
    }

    /// Check if a slot is currently marked as retiring.
    pub fn is_retiring(&self, slot: usize) -> bool {
        if slot < 32 {
            (self.state.load(Ordering::SeqCst) & ((1u64 << slot) << 32)) != 0
        } else {
            false
        }
    }

    /// Total number of currently allocated slots (active + retiring).
    pub fn allocated_count(&self) -> usize {
        self.snapshot().allocated_count
    }

    /// Number of slots currently marked as retiring.
    pub fn retiring_count(&self) -> usize {
        self.snapshot().retiring_count
    }

    /// Number of active workers eligible to lease chunks (allocated and non-retiring).
    pub fn active_worker_count(&self) -> usize {
        self.snapshot().active_count
    }

    /// Returns a list of currently allocated slot indices.
    pub fn active_slots(&self) -> Vec<usize> {
        let alloc_mask = self.snapshot().allocated_mask;
        let mut slots = Vec::new();
        for i in 0..self.max_slots {
            if (alloc_mask & (1 << i)) != 0 {
                slots.push(i);
            }
        }
        slots
    }
}

/// Internal RAII defuser ensuring a newly acquired slot ID and active worker counter are released
/// if an error, panic, or cancellation unwinds the stack in the factory function
/// before the full `WorkerLifecycleGuard` is constructed. Constructed immediately upon slot acquisition.
struct EarlyAllocDefuser {
    slot_id: usize,
    slot_pool: std::sync::Arc<WorkerSlotPool>,
    task_active_workers: std::sync::Arc<AtomicUsize>,
    counter_incremented: bool,
    defused: bool,
}

#[doc(hidden)]
pub mod test_support {
    use std::sync::atomic::AtomicU64;
    use std::sync::Mutex;
    pub static ACQUIRE_FAILPOINT_MUTEX: Mutex<()> = Mutex::new(());
    pub static SIMULATE_ACQUIRE_PANIC_TASK_ID: AtomicU64 = AtomicU64::new(0);
}

impl Drop for EarlyAllocDefuser {
    fn drop(&mut self) {
        if !self.defused {
            if self.counter_incremented {
                self.task_active_workers.fetch_sub(1, Ordering::SeqCst);
            }
            self.slot_pool.release_slot(self.slot_id);
        }
    }
}

pub const GUARD_STATE_ACTIVE: u8 = 0;
pub const GUARD_STATE_RETIRING: u8 = 1;
pub const GUARD_STATE_TERMINATED: u8 = 2;

/// RAII guard ensuring a worker's slot allocation, state reset, and active worker count
/// are strictly maintained without leaks across cancellations, errors, pauses, and exits.
pub struct WorkerLifecycleGuard {
    pub slot_id: usize,
    task_active_workers: std::sync::Arc<AtomicUsize>,
    telemetry: std::sync::Arc<TaskTelemetryCollector>,
    slot_pool: std::sync::Arc<WorkerSlotPool>,
    tracker: std::sync::Arc<AtomicU64>,
    state: AtomicU8,
}

impl WorkerLifecycleGuard {
    /// Safe factory for acquiring a slot and creating the guard in the scheduler context
    /// BEFORE spawning the worker task. Guarantees zero-leak ownership contract and panic safety.
    pub fn acquire(
        slot_pool: std::sync::Arc<WorkerSlotPool>,
        task_active_workers: std::sync::Arc<AtomicUsize>,
        telemetry: std::sync::Arc<TaskTelemetryCollector>,
        trackers: &[std::sync::Arc<AtomicU64>],
    ) -> Option<Self> {
        if trackers.is_empty() {
            return None;
        }

        let slot_id = slot_pool.acquire_slot()?;
        // Defuser is constructed IMMEDIATELY after slot acquisition:
        let mut defuser = EarlyAllocDefuser {
            slot_id,
            slot_pool: slot_pool.clone(),
            task_active_workers: task_active_workers.clone(),
            counter_incremented: false,
            defused: false,
        };

        let target_panic_id = test_support::SIMULATE_ACQUIRE_PANIC_TASK_ID.load(Ordering::SeqCst);
        if target_panic_id != 0 && target_panic_id == telemetry.task_id {
            panic!("Simulated panic after slot acquisition before guard completion");
        }

        task_active_workers.fetch_add(1, Ordering::SeqCst);
        defuser.counter_incremented = true;

        let tracker = trackers[slot_id % trackers.len()].clone();
        let guard = Self::new(slot_id, task_active_workers, telemetry, slot_pool, tracker);

        defuser.defused = true;
        Some(guard)
    }

    /// Construct a guard for an already allocated slot (used by tests or direct ownership transfer).
    pub fn new(
        slot_id: usize,
        task_active_workers: std::sync::Arc<AtomicUsize>,
        telemetry: std::sync::Arc<TaskTelemetryCollector>,
        slot_pool: std::sync::Arc<WorkerSlotPool>,
        tracker: std::sync::Arc<AtomicU64>,
    ) -> Self {
        telemetry.set_worker_state(slot_id, WorkerState::Idle);
        tracker.store(0, Ordering::Relaxed);
        Self {
            slot_id,
            task_active_workers,
            telemetry,
            slot_pool,
            tracker,
            state: AtomicU8::new(GUARD_STATE_ACTIVE),
        }
    }

    /// Atomic claim operation: transitions guard state from ACTIVE to RETIRING.
    /// Exactly one caller per guard can ever win this claim.
    fn claim_retirement(&self) -> bool {
        self.state.compare_exchange(
            GUARD_STATE_ACTIVE,
            GUARD_STATE_RETIRING,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ).is_ok()
    }

    /// Mark that this worker has been explicitly retired during scale-down.
    /// Atomically claims retirement on the guard, transitions slot in pool,
    /// decrements `task_active_workers`, and sets telemetry to `Retiring`.
    /// Returns true if this caller won the retirement claim.
    /// If the pool rejects the transition, rolls back guard state to ACTIVE via CAS and returns false.
    pub fn mark_retired(&self) -> bool {
        if !self.claim_retirement() {
            return false;
        }
        if self.slot_pool.mark_retiring(self.slot_id) {
            self.task_active_workers.fetch_sub(1, Ordering::SeqCst);
            self.telemetry.set_worker_state(self.slot_id, WorkerState::Retiring);
            true
        } else {
            // Pool transition rejected! Transactionally roll back guard state:
            // RETIRING -> ACTIVE via CAS. If the guard was concurrently terminated, leave TERMINATED.
            let _ = self.state.compare_exchange(
                GUARD_STATE_RETIRING,
                GUARD_STATE_ACTIVE,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            false
        }
    }

    /// Try to retire this worker if live active workers exceed target concurrency.
    /// Uses a single atomic claim on guard state coordinated with atomic CAS on WorkerSlotPool.
    /// Returns true if this worker successfully committed to retirement; returns false otherwise.
    /// If the pool rejects the transition, rolls back guard state to ACTIVE via CAS and returns false.
    pub fn try_retire(&self, target_concurrency: usize) -> bool {
        if self.state.load(Ordering::SeqCst) != GUARD_STATE_ACTIVE {
            return false;
        }

        let snap = self.slot_pool.snapshot();
        if snap.active_count <= target_concurrency || snap.active_count <= 1 {
            return false;
        }

        if !self.claim_retirement() {
            return false;
        }

        if self.slot_pool.try_retire_slot(self.slot_id, target_concurrency) {
            self.task_active_workers.fetch_sub(1, Ordering::SeqCst);
            self.telemetry.set_worker_state(self.slot_id, WorkerState::Retiring);
            true
        } else {
            // Target concurrency was satisfied by another concurrent worker under pool CAS,
            // or active count was <= 1. Transactionally roll back guard state: RETIRING -> ACTIVE via CAS.
            let _ = self.state.compare_exchange(
                GUARD_STATE_RETIRING,
                GUARD_STATE_ACTIVE,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
            false
        }
    }

    /// Query whether this worker has committed to retirement.
    pub fn is_retired(&self) -> bool {
        self.state.load(Ordering::SeqCst) == GUARD_STATE_RETIRING
    }

    /// Returns the raw guard lifecycle state (ACTIVE, RETIRING, or TERMINATED).
    pub fn lifecycle_state(&self) -> u8 {
        self.state.load(Ordering::SeqCst)
    }
}

impl Drop for WorkerLifecycleGuard {
    fn drop(&mut self) {
        let prev = self.state.swap(GUARD_STATE_TERMINATED, Ordering::SeqCst);
        if prev == GUARD_STATE_ACTIVE {
            // Unretired guard: dropped due to error, chunk exhaustion, panic, or abort.
            // Decrement task_active_workers exactly once.
            self.task_active_workers.fetch_sub(1, Ordering::SeqCst);
        }
        // If prev was GUARD_STATE_RETIRING, task_active_workers was already decremented upon retirement.
        // It is NOT decremented again!
        self.telemetry.set_worker_state(self.slot_id, WorkerState::Terminated);
        self.tracker.store(0, Ordering::Relaxed);
        self.slot_pool.release_slot(self.slot_id);
    }
}

// Global registry for per-task strategies and diagnostic events
static GLOBAL_STRATEGY: RwLock<Option<ConcurrencyStrategy>> = RwLock::new(None);
static TASK_STRATEGIES: RwLock<Option<HashMap<u64, ConcurrencyStrategy>>> = RwLock::new(None);
static TASK_DECISION_HISTORY: RwLock<Option<HashMap<u64, Vec<ConcurrencyDecisionEvent>>>> = RwLock::new(None);

pub fn set_global_concurrency_strategy(strategy: Option<ConcurrencyStrategy>) {
    let mut lock = GLOBAL_STRATEGY.write().unwrap_or_else(|p| p.into_inner());
    *lock = strategy;
}

pub fn get_global_concurrency_strategy() -> Option<ConcurrencyStrategy> {
    let lock = GLOBAL_STRATEGY.read().unwrap_or_else(|p| p.into_inner());
    lock.clone()
}

pub fn set_task_concurrency_strategy(task_id: u64, strategy: ConcurrencyStrategy) {
    let mut lock = TASK_STRATEGIES.write().unwrap_or_else(|p| p.into_inner());
    let map = lock.get_or_insert_with(HashMap::new);
    map.insert(task_id, strategy);
}

pub fn get_task_concurrency_strategy(task_id: u64) -> Option<ConcurrencyStrategy> {
    let lock = TASK_STRATEGIES.read().unwrap_or_else(|p| p.into_inner());
    if let Some(ref map) = *lock {
        if let Some(strat) = map.get(&task_id) {
            return Some(strat.clone());
        }
    }
    get_global_concurrency_strategy()
}

pub fn record_task_concurrency_decision(event: ConcurrencyDecisionEvent) {
    let mut lock = TASK_DECISION_HISTORY.write().unwrap_or_else(|p| p.into_inner());
    let map = lock.get_or_insert_with(HashMap::new);
    let list = map.entry(event.task_id).or_default();
    list.push(event);
    if list.len() > 1000 {
        list.remove(0);
    }
}

pub fn get_task_concurrency_decisions(task_id: u64) -> Vec<ConcurrencyDecisionEvent> {
    let lock = TASK_DECISION_HISTORY.read().unwrap_or_else(|p| p.into_inner());
    if let Some(ref map) = *lock {
        if let Some(list) = map.get(&task_id) {
            return list.clone();
        }
    }
    Vec::new()
}

pub fn clear_task_concurrency_decisions(task_id: u64) {
    let mut lock = TASK_DECISION_HISTORY.write().unwrap_or_else(|p| p.into_inner());
    if let Some(ref mut map) = *lock {
        map.remove(&task_id);
    }
}

static TASK_TIMELINES: RwLock<Option<HashMap<u64, Vec<ConcurrencyTimelineEntry>>>> = RwLock::new(None);

pub fn record_task_concurrency_timeline(task_id: u64, entry: ConcurrencyTimelineEntry) {
    let mut lock = TASK_TIMELINES.write().unwrap_or_else(|p| p.into_inner());
    let map = lock.get_or_insert_with(HashMap::new);
    let list = map.entry(task_id).or_default();
    list.push(entry);
    if list.len() > 1000 {
        list.remove(0);
    }
}

pub fn get_task_concurrency_timeline(task_id: u64) -> Vec<ConcurrencyTimelineEntry> {
    let lock = TASK_TIMELINES.read().unwrap_or_else(|p| p.into_inner());
    if let Some(ref map) = *lock {
        if let Some(list) = map.get(&task_id) {
            return list.clone();
        }
    }
    Vec::new()
}

pub fn clear_task_concurrency_timeline(task_id: u64) {
    let mut lock = TASK_TIMELINES.write().unwrap_or_else(|p| p.into_inner());
    if let Some(ref mut map) = *lock {
        map.remove(&task_id);
    }
}

/// Comprehensive telemetry summary recorded for a completed or ongoing task
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TaskTelemetrySummary {
    pub task_id: u64,
    pub total_backoff_duration_ms: u64,
    pub sleeping_workers: usize,
    pub active_workers: usize,
    pub configured_concurrency: usize,
    pub available_chunks: usize,
    pub retries: usize,
    pub server_throttle_duration_ms: u64,
    pub recovery_duration_ms: u64,
}

static TASK_TELEMETRY_SUMMARIES: RwLock<Option<HashMap<u64, TaskTelemetrySummary>>> = RwLock::new(None);

pub fn record_task_telemetry_summary(task_id: u64, summary: TaskTelemetrySummary) {
    let mut lock = TASK_TELEMETRY_SUMMARIES.write().unwrap_or_else(|p| p.into_inner());
    let map = lock.get_or_insert_with(HashMap::new);
    map.insert(task_id, summary);
}

pub fn get_task_telemetry_summary(task_id: u64) -> Option<TaskTelemetrySummary> {
    let lock = TASK_TELEMETRY_SUMMARIES.read().unwrap_or_else(|p| p.into_inner());
    if let Some(ref map) = *lock {
        return map.get(&task_id).cloned();
    }
    None
}

pub fn clear_task_telemetry_summary(task_id: u64) {
    let mut lock = TASK_TELEMETRY_SUMMARIES.write().unwrap_or_else(|p| p.into_inner());
    if let Some(ref mut map) = *lock {
        map.remove(&task_id);
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_snapshot() -> TelemetrySnapshot {
        TelemetrySnapshot {
            timestamp: Instant::now(),
            aggregate_throughput_bps: 10_000_000.0,
            smoothed_throughput_bps: 10_000_000.0,
            active_concurrency: 4,
            target_concurrency: 4,
            avg_rtt: Duration::from_millis(20),
            total_idle_time: Duration::ZERO,
            total_request_setup_time: Duration::from_millis(10),
            total_network_wait_time: Duration::from_millis(50),
            total_writer_backpressure_time: Duration::ZERO,
            retry_count: 0,
            error_rate: 0.0,
            http_429_count: 0,
            http_503_count: 0,
            delta_http_429: 0,
            delta_http_503: 0,
            delta_retries: 0,
            delta_bytes: 1_000_000,
            server_throttle_until: None,
            sleeping_workers: 0,
            total_backoff_duration: Duration::ZERO,
            writer_queue_depth: 10,
            in_flight_writes: 15,
            progress_ratio: 0.25,
            remaining_chunks: 100,
        }
    }

    #[test]
    fn test_fixed_concurrency_strategy_remains_constant() {
        let mut ctrl = AdaptiveConcurrencyController::new(1, ConcurrencyStrategy::Fixed(8));
        let snap = dummy_snapshot();
        let evt = ctrl.evaluate(&snap);
        assert_eq!(evt.target_concurrency, 8);
        assert_eq!(evt.reason, DecisionReason::FixedStrategy);
    }

    #[test]
    fn test_bdp_seeding_from_rtt() {
        // LAN (<15ms)
        assert_eq!(seed_initial_concurrency_from_rtt(Duration::from_millis(1), 1, 16), 2);
        // Moderate (~25ms)
        assert_eq!(seed_initial_concurrency_from_rtt(Duration::from_millis(25), 1, 16), 4);
        // High (~50ms)
        assert_eq!(seed_initial_concurrency_from_rtt(Duration::from_millis(50), 1, 16), 8);
        // Very high (>=75ms)
        assert_eq!(seed_initial_concurrency_from_rtt(Duration::from_millis(100), 1, 16), 12);

        let mut config = AdaptiveConfig::default();
        config.initial_concurrency = 2;
        let mut ctrl = AdaptiveConcurrencyController::new(100, ConcurrencyStrategy::Adaptive(config));
        assert_eq!(ctrl.current_concurrency, 2);

        // Seed with 50ms RTT
        ctrl.init_with_rtt(Duration::from_millis(50));
        assert_eq!(ctrl.current_concurrency, 8, "50ms RTT should seed concurrency to 8");
        assert_eq!(ctrl.timeline.len(), 2);
        assert!(matches!(ctrl.timeline[1].reason, DecisionReason::InitialBdpSeed { rtt_ms: 50, seeded_concurrency: 8 }));
    }

    #[test]
    fn test_http_429_immediate_scale_down() {
        let mut config = AdaptiveConfig::default();
        config.initial_concurrency = 8;
        let mut ctrl = AdaptiveConcurrencyController::new(2, ConcurrencyStrategy::Adaptive(config));
        
        let mut snap = dummy_snapshot();
        snap.delta_http_429 = 1;
        let evt = ctrl.evaluate(&snap);
        assert_eq!(evt.target_concurrency, 4, "HTTP 429 should halve concurrency from 8 to 4");
        assert_eq!(evt.reason, DecisionReason::ThrottledHttp429);
        assert_eq!(ctrl.phase, ControllerPhase::CongestionAvoidance);
    }

    #[test]
    fn test_http_503_immediate_scale_down() {
        let mut config = AdaptiveConfig::default();
        config.initial_concurrency = 8;
        let mut ctrl = AdaptiveConcurrencyController::new(3, ConcurrencyStrategy::Adaptive(config));
        
        let mut snap = dummy_snapshot();
        snap.delta_http_503 = 1;
        let evt = ctrl.evaluate(&snap);
        assert_eq!(evt.target_concurrency, 4, "HTTP 503 should halve concurrency from 8 to 4");
        assert_eq!(evt.reason, DecisionReason::ThrottledHttp503);
        assert_eq!(ctrl.phase, ControllerPhase::CongestionAvoidance);
    }

    #[test]
    fn test_throttling_deduplication_coalescing() {
        let mut config = AdaptiveConfig::default();
        config.initial_concurrency = 8;
        let mut ctrl = AdaptiveConcurrencyController::new(4, ConcurrencyStrategy::Adaptive(config));
        
        // First 429 halves to 4
        let mut snap1 = dummy_snapshot();
        snap1.delta_http_429 = 1;
        let evt1 = ctrl.evaluate(&snap1);
        assert_eq!(evt1.target_concurrency, 4);
        assert_eq!(evt1.reason, DecisionReason::ThrottledHttp429);

        // Immediate second 429 from another worker within 400ms cooldown: must coalesce and NOT halve again to 2
        let mut snap2 = dummy_snapshot();
        snap2.delta_http_429 = 1;
        let evt2 = ctrl.evaluate(&snap2);
        assert_eq!(evt2.target_concurrency, 4, "Simultaneous throttle must coalesce");
        assert_eq!(evt2.reason, DecisionReason::ThrottledCoalesced);
    }

    #[test]
    fn test_server_throttle_gradual_recovery() {
        let mut config = AdaptiveConfig::default();
        config.initial_concurrency = 8;
        let mut ctrl = AdaptiveConcurrencyController::new(5, ConcurrencyStrategy::Adaptive(config));
        
        // Initial 429 throttles 8 -> 4
        let mut snap1 = dummy_snapshot();
        snap1.delta_http_429 = 1;
        let evt1 = ctrl.evaluate(&snap1);
        assert_eq!(evt1.target_concurrency, 4);

        // Advance past cooldown
        ctrl.throttle_cooldown_until = Instant::now() - Duration::from_millis(10);

        // Subsequent clean evaluations gradually restore concurrency +1 per cycle back to 8
        let clean_snap = dummy_snapshot();
        let evt2 = ctrl.evaluate(&clean_snap);
        assert_eq!(evt2.target_concurrency, 5, "First recovery step should be 5");
        assert!(matches!(evt2.reason, DecisionReason::ServerThrottleRecovery { restored_concurrency: 5, target_concurrency: 8 } | DecisionReason::RecoveryProbe { probed_concurrency: 5, pre_throttle_target: 8 }));

        let evt3 = ctrl.evaluate(&clean_snap);
        assert_eq!(evt3.target_concurrency, 6, "Second recovery step should be 6");

        let evt4 = ctrl.evaluate(&clean_snap);
        assert_eq!(evt4.target_concurrency, 7);

        let evt5 = ctrl.evaluate(&clean_snap);
        assert_eq!(evt5.target_concurrency, 8, "Target concurrency restored");
        assert_eq!(ctrl.pre_throttle_concurrency, 0, "Recovery state reset after reaching target");
    }

    #[test]
    fn test_writer_backpressure_prevents_scale_up() {
        let mut config = AdaptiveConfig::default();
        config.initial_concurrency = 4;
        config.cooldown_duration = Duration::ZERO; // bypass cooldown for testing
        let mut ctrl = AdaptiveConcurrencyController::new(4, ConcurrencyStrategy::Adaptive(config));
        
        let mut snap = dummy_snapshot();
        snap.writer_queue_depth = 210; // above 200 threshold
        let evt = ctrl.evaluate(&snap);
        assert_eq!(evt.target_concurrency, 4, "Writer backpressure must hold concurrency");
        assert!(matches!(evt.reason, DecisionReason::WriterBackpressure { queue_depth: 210 }));
        assert_eq!(ctrl.phase, ControllerPhase::CongestionAvoidance);
    }

    #[test]
    fn test_writer_severe_backpressure_scales_down() {
        let mut config = AdaptiveConfig::default();
        config.initial_concurrency = 4;
        config.cooldown_duration = Duration::ZERO;
        let mut ctrl = AdaptiveConcurrencyController::new(5, ConcurrencyStrategy::Adaptive(config));
        
        let mut snap = dummy_snapshot();
        snap.writer_queue_depth = 240; // severe > 230
        let evt = ctrl.evaluate(&snap);
        assert_eq!(evt.target_concurrency, 3, "Severe backpressure must decrement concurrency");
        assert!(matches!(evt.reason, DecisionReason::WriterBackpressure { queue_depth: 240 }));
    }

    #[test]
    fn test_high_error_rate_scales_down() {
        let mut config = AdaptiveConfig::default();
        config.initial_concurrency = 4;
        config.cooldown_duration = Duration::ZERO;
        let mut ctrl = AdaptiveConcurrencyController::new(6, ConcurrencyStrategy::Adaptive(config));
        
        let mut snap = dummy_snapshot();
        snap.retry_count = 5;
        snap.error_rate = 0.20; // 20% > 10%
        let evt = ctrl.evaluate(&snap);
        assert_eq!(evt.target_concurrency, 3, "High error rate must decrement concurrency");
        assert!(matches!(evt.reason, DecisionReason::HighErrorRate { error_rate_pct: 20 }));
    }

    #[test]
    fn test_worker_starvation_holds_concurrency() {
        let mut config = AdaptiveConfig::default();
        config.initial_concurrency = 4;
        config.cooldown_duration = Duration::ZERO;
        let mut ctrl = AdaptiveConcurrencyController::new(7, ConcurrencyStrategy::Adaptive(config));
        
        let mut snap = dummy_snapshot();
        snap.remaining_chunks = 2; // remaining 2 < concurrency 4
        let evt = ctrl.evaluate(&snap);
        assert_eq!(evt.target_concurrency, 4, "Starvation must hold concurrency");
        assert!(matches!(evt.reason, DecisionReason::WorkerStarvation { remaining_chunks: 2 }));
    }

    #[test]
    fn test_slow_start_probe_and_transition_to_aimd() {
        let mut config = AdaptiveConfig::default();
        config.initial_concurrency = 2;
        config.slow_start_scale_up_step = 2;
        config.cooldown_duration = Duration::ZERO;
        let mut ctrl = AdaptiveConcurrencyController::new(8, ConcurrencyStrategy::Adaptive(config));
        assert_eq!(ctrl.phase, ControllerPhase::SlowStart);
        assert_eq!(ctrl.current_eval_interval(), Duration::from_millis(35));

        // Tick 1 in SlowStart: first sample records baseline and ramps up by +2
        let mut snap = dummy_snapshot();
        snap.aggregate_throughput_bps = 10_000_000.0;
        let evt1 = ctrl.evaluate(&snap);
        assert_eq!(evt1.target_concurrency, 4);
        assert!(matches!(evt1.reason, DecisionReason::SlowStartScaleUp { new_concurrency: 4, .. }));
        assert_eq!(ctrl.phase, ControllerPhase::SlowStart);

        // Tick 2: throughput grew to 15 MB/s (+50% gain) -> continue slow-start!
        snap.aggregate_throughput_bps = 15_000_000.0;
        let evt2 = ctrl.evaluate(&snap);
        assert_eq!(evt2.target_concurrency, 6);
        assert!(matches!(evt2.reason, DecisionReason::SlowStartScaleUp { new_concurrency: 6, .. }));
        assert_eq!(ctrl.phase, ControllerPhase::SlowStart);

        // Tick 3: throughput plateaus at 15.2 MB/s (<5% gain) -> transition to CongestionAvoidance!
        snap.aggregate_throughput_bps = 15_200_000.0;
        let evt3 = ctrl.evaluate(&snap);
        assert_eq!(evt3.target_concurrency, 6);
        assert!(matches!(evt3.reason, DecisionReason::SlowStartTransitionToAimd { .. }));
        assert_eq!(ctrl.phase, ControllerPhase::CongestionAvoidance);
        assert_eq!(ctrl.current_eval_interval(), Duration::from_millis(500));
    }

    #[test]
    fn test_concurrency_timeline_recording() {
        clear_task_concurrency_timeline(99);
        let mut config = AdaptiveConfig::default();
        config.initial_concurrency = 4;
        let mut ctrl = AdaptiveConcurrencyController::new(99, ConcurrencyStrategy::Adaptive(config));

        let mut snap = dummy_snapshot();
        snap.aggregate_throughput_bps = 10_000_000.0;
        ctrl.evaluate(&snap);

        let timeline = get_task_concurrency_timeline(99);
        assert_eq!(timeline.len(), 2); // Initial + First evaluation
        assert_eq!(timeline[0].concurrency, 4);
        assert_eq!(timeline[1].concurrency, 6);
    }

    #[test]
    fn test_throttling_coalescing_and_gradual_recovery() {
        let mut config = AdaptiveConfig::default();
        config.initial_concurrency = 8;
        config.min_concurrency = 1;
        config.max_concurrency = 16;
        let mut ctrl = AdaptiveConcurrencyController::new(101, ConcurrencyStrategy::Adaptive(config));

        // Step 1: Initial 429 encountered -> halve concurrency from 8 to 4
        let mut snap = dummy_snapshot();
        snap.delta_http_429 = 1;
        let evt1 = ctrl.evaluate(&snap);
        assert_eq!(evt1.target_concurrency, 4);
        assert!(matches!(evt1.reason, DecisionReason::ThrottledHttp429));

        // Step 2: Immediate follow-up 429 within 400ms cooldown window -> coalesce, do NOT halve to 2!
        let evt2 = ctrl.evaluate(&snap);
        assert_eq!(evt2.target_concurrency, 4);
        assert!(matches!(evt2.reason, DecisionReason::ThrottledCoalesced));

        // Step 3: Server recovers (clean evaluation, no 429/503), but cooldown still active
        snap.delta_http_429 = 0;
        let evt3 = ctrl.evaluate(&snap);
        // While cooldown active, consecutive_clean_evals increments but concurrency held
        assert_eq!(evt3.target_concurrency, 4);

        // Advance simulated time past 400ms cooldown
        ctrl.throttle_cooldown_until = std::time::Instant::now() - Duration::from_millis(10);

        // Step 4: First clean evaluation past cooldown -> gradual restoration (+1 to 5)
        let evt4 = ctrl.evaluate(&snap);
        assert_eq!(evt4.target_concurrency, 5);
        assert!(matches!(evt4.reason, DecisionReason::ServerThrottleRecovery { restored_concurrency: 5, target_concurrency: 8 } | DecisionReason::RecoveryProbe { probed_concurrency: 5, pre_throttle_target: 8 }));

        // Step 5: Next clean evaluations -> gradual restoration +1 each time until pre-throttle level (8)
        let evt5 = ctrl.evaluate(&snap);
        assert_eq!(evt5.target_concurrency, 6);
        let evt6 = ctrl.evaluate(&snap);
        assert_eq!(evt6.target_concurrency, 7);
        let evt7 = ctrl.evaluate(&snap);
        assert_eq!(evt7.target_concurrency, 8);
        assert!(matches!(evt7.reason, DecisionReason::ServerThrottleRecovery { restored_concurrency: 8, target_concurrency: 8 } | DecisionReason::RecoveryStable { settled_concurrency: 8 }));

        // Step 6: Full recovery finished, subsequent clean evals transition back to normal AIMD
        let evt8 = ctrl.evaluate(&snap);
        assert!(!matches!(evt8.reason, DecisionReason::ServerThrottleRecovery { .. }));
    }
}
