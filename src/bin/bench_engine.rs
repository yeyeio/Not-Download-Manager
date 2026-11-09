use app::engine::*;
use std::path::PathBuf;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: bench_engine <url> <save_path> <connections>");
        std::process::exit(1);
    }
    let url = args[1].clone();
    let save_path = PathBuf::from(&args[2]);
    let connections: usize = args[3].parse().unwrap_or(8);

    let mut engine = spawn_download_engine();
    let start = Instant::now();
    let _ = engine.cmd_sender.send(EngineCommand::StartDownload {
        id: 1,
        url,
        save_path: save_path.clone(),
        connections,
    });

    let mut last_bytes = 0;
    while let Some(evt) = engine.event_receiver.blocking_recv() {
        match evt {
            EngineEvent::ProgressUpdate { downloaded, .. } => {
                last_bytes = downloaded;
            }
            EngineEvent::Finished { .. } => {
                let duration = start.elapsed().as_secs_f64();
                let speed_mb = (last_bytes as f64 / (1024.0 * 1024.0)) / duration;
                println!(
                    "BENCH_RESULT: bytes={} duration={:.4} speed_mb_s={:.2}",
                    last_bytes, duration, speed_mb
                );
                std::process::exit(0);
            }
            EngineEvent::Failed { error, .. } => {
                eprintln!("BENCH_FAILED: error={}", error);
                std::process::exit(1);
            }
            _ => {}
        }
    }
}
