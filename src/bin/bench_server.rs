use not_download_manager::bench_server::{compute_deterministic_sha256, start_benchmark_server, ServerConfig};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut config = ServerConfig::default();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" => {
                if let Some(val) = args.get(i + 1) {
                    config.port = val.parse().unwrap_or(8080);
                    i += 1;
                }
            }
            "--size" => {
                if let Some(val) = args.get(i + 1) {
                    config.total_file_size = val.parse().unwrap_or(100 * 1024 * 1024);
                    i += 1;
                }
            }
            "--throttle" => {
                if let Some(val) = args.get(i + 1) {
                    config.throttle_mbps = Some(val.parse().unwrap_or(100.0));
                    i += 1;
                }
            }
            "--latency" => {
                if let Some(val) = args.get(i + 1) {
                    config.latency_ms = val.parse().unwrap_or(50);
                    i += 1;
                }
            }
            "--max-conn" => {
                if let Some(val) = args.get(i + 1) {
                    config.max_connections = Some(val.parse().unwrap_or(16));
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }

    println!("===============================================================");
    println!(" HYPERDOWNLOAD CONTROLLED HTTP BENCHMARK SERVER ");
    println!("===============================================================");
    println!("Port:             {}", config.port);
    println!("Total Size:       {} bytes ({:.2} MB)", config.total_file_size, config.total_file_size as f64 / 1_048_576.0);
    println!("Throttle:         {:?} Mbps", config.throttle_mbps);
    println!("Latency:          {} ms", config.latency_ms);
    println!("Max Connections:  {:?}", config.max_connections);

    println!("Calculating deterministic SHA-256 for verification...");
    let t0 = std::time::Instant::now();
    let expected_hash = compute_deterministic_sha256(config.total_file_size);
    println!("Expected SHA-256: {} (computed in {:.2?})", expected_hash, t0.elapsed());

    let handle = start_benchmark_server(config).await;
    println!("Server running at: {}", handle.url);
    println!("Press Ctrl+C to stop...");

    tokio::signal::ctrl_c().await.unwrap();
    println!("\nShutting down server...");
    handle.shutdown().await;
    println!("Server stopped cleanly.");
}
