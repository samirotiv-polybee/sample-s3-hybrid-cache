//! Cache-hit throughput at two `cache.disk_streaming_chunk_size` / `disk_streaming_queue_depth`
//! settings, through the proxy over loopback TCP.
//!
//! `CLIENTS` keep-alive clients each read `REQUESTS` whole-extent range GETs of a
//! FruitFox-sized object (2.4 MB, the mean detection read) from the cache as fast as they can.
//! The two settings run alternately, `ROUNDS` times each, on two proxies of their own, and the
//! medians are compared, so a slower or busier host moves both alike.
//!
//! Ignored by default because it measures time; run it alone with
//! `cargo test --release --test hit_stream_throughput_test -- --ignored --nocapture`.
//! Harness copied from `tests/hit_stream_memory_test.rs`.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::S3ClientApi;

use common::{StubResponse, StubS3Client};

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
const OBJECT_SIZE: usize = 2_400_000;
const CLIENTS: usize = 32;
const REQUESTS: usize = 40;
const ROUNDS: usize = 7;
const PATH: &str = "/mybucket/hit-stream-throughput.bin";

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn test_config(cache_dir: std::path::PathBuf, chunk_size: usize, queue_depth: usize) -> Config {
    let mut config = Config::default();
    config.cache.cache_dir = cache_dir;
    config.cache.download_coordination.enabled = false;
    config.cache.ram_cache_enabled = false;
    config.cache.bucket_settings_staleness_threshold = Duration::ZERO;
    config.cache.max_cache_size = 1024 * 1024 * 1024;
    config.cache.read_cache_enabled = true;
    config.cache.disk_streaming_chunk_size = chunk_size;
    config.cache.disk_streaming_queue_depth = queue_depth;
    config.server.max_concurrent_requests = 100;
    config
}

async fn make_cache_infra(config: &Arc<Config>) -> (Arc<CacheManager>, Arc<RangeHandler>) {
    let cache_manager = Arc::new(CacheManager::new_with_shared_storage(
        config.cache.cache_dir.clone(),
        config.cache.ram_cache_enabled,
        config.cache.max_ram_cache_size,
        config.cache.max_cache_size,
        CacheEvictionAlgorithm::TinyLFU,
        1024,
        false,
        config.cache.get_ttl,
        config.cache.head_ttl,
        config.cache.put_ttl,
        config.cache.actively_remove_cached_data,
        config.cache.shared_storage.clone(),
        config.cache.write_cache_percent,
        config.cache.write_cache_enabled,
        config.cache.incomplete_upload_ttl,
        config.cache.metadata_cache.clone(),
        config.cache.eviction_trigger_percent,
        config.cache.eviction_target_percent,
        config.cache.read_cache_enabled,
        config.cache.bucket_settings_staleness_threshold,
        config.cache.compression_batch_size,
        config.cache.evaluate_conditions_from_cache,
        Duration::from_secs(10),
        64,
        Duration::from_secs(5),
    ));

    let disk_cache_manager = Arc::new(tokio::sync::RwLock::new(
        cache_manager.create_configured_disk_cache_manager(),
    ));
    cache_manager.initialize().await.expect("cache init");
    disk_cache_manager
        .write()
        .await
        .initialize()
        .await
        .expect("disk cache init");

    let range_handler = Arc::new(RangeHandler::new(
        Arc::clone(&cache_manager),
        Arc::clone(&disk_cache_manager),
    ));

    (cache_manager, range_handler)
}

struct ProxyServer {
    addr: SocketAddr,
    _shutdown_tx: oneshot::Sender<()>,
}

async fn spawn_proxy_server(
    config: Arc<Config>,
    cache_manager: Arc<CacheManager>,
    range_handler: Arc<RangeHandler>,
    s3_client: Arc<dyn S3ClientApi + Send + Sync>,
) -> ProxyServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.server.max_concurrent_requests,
    ));
    let inflight_tracker = Arc::new(InFlightTracker::new());
    let inflight_ledger = s3_client.get_inflight_ledger();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                accept = listener.accept() => {
                    let (stream, peer) = match accept {
                        Ok(v) => v,
                        Err(_) => break,
                    };
                    let io = TokioIo::new(stream);

                    let config = Arc::clone(&config);
                    let cache_manager = Arc::clone(&cache_manager);
                    let range_handler = Arc::clone(&range_handler);
                    let s3_client = Arc::clone(&s3_client);
                    let inflight_tracker = Arc::clone(&inflight_tracker);
                    let request_semaphore = Arc::clone(&semaphore);
                    let inflight_ledger = Arc::clone(&inflight_ledger);

                    tokio::spawn(async move {
                        let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                            let config = Arc::clone(&config);
                            let cache_manager = Arc::clone(&cache_manager);
                            let range_handler = Arc::clone(&range_handler);
                            let s3_client = Arc::clone(&s3_client);
                            let inflight_tracker = Arc::clone(&inflight_tracker);
                            let request_semaphore = Arc::clone(&request_semaphore);
                            let inflight_ledger = Arc::clone(&inflight_ledger);
                            async move {
                                HttpProxy::handle_request(
                                    req,
                                    peer,
                                    config,
                                    cache_manager,
                                    s3_client,
                                    range_handler,
                                    request_semaphore,
                                    None,
                                    None,
                                    inflight_tracker,
                                    None,
                                    None,
                                    None,
                                    inflight_ledger,
                                )
                                .await
                            }
                        });

                        // A stalled client is dropped mid-body; that error is expected.
                        let _ = http1::Builder::new().serve_connection(io, service).await;
                    });
                }
                _ = &mut shutdown_rx => break,
            }
        }
    });

    ProxyServer {
        addr,
        _shutdown_tx: shutdown_tx,
    }
}

const AUTHORIZATION: &str =
    "AWS4-HMAC-SHA256 Credential=AKIA-TEST/20250101/us-east-1/s3/aws4_request, \
     SignedHeaders=host;x-amz-date, Signature=sig";

type HttpClient = Client<hyper_util::client::legacy::connect::HttpConnector, Full<Bytes>>;

/// One GET read to the end without collecting the body: status, `x-cache`, body length.
async fn get(
    client: &HttpClient,
    addr: SocketAddr,
    range: Option<&str>,
) -> (StatusCode, Option<String>, usize) {
    let mut builder = Request::builder()
        .method("GET")
        .uri(format!("http://{}{}", addr, PATH))
        .header("authorization", AUTHORIZATION);
    if let Some(range) = range {
        builder = builder.header("range", range);
    }
    let response = tokio::time::timeout(
        Duration::from_secs(60),
        client.request(builder.body(Full::new(Bytes::new())).unwrap()),
    )
    .await
    .expect("proxy request timed out")
    .expect("proxy request failed");
    let status = response.status();
    let x_cache = response
        .headers()
        .get("x-cache")
        .map(|v| v.to_str().unwrap().to_string());
    let mut body = response.into_body();
    let mut len = 0;
    while let Some(frame) = body.frame().await {
        if let Ok(data) = frame.unwrap().into_data() {
            len += data.len();
        }
    }
    (status, x_cache, len)
}

/// A proxy at one setting with the object cached on disk.
async fn warmed_proxy(dir: &TempDir, chunk_size: usize, queue_depth: usize) -> ProxyServer {
    let config = Arc::new(test_config(
        dir.path().to_path_buf(),
        chunk_size,
        queue_depth,
    ));
    let (cache_manager, range_handler) = make_cache_infra(&config).await;
    let body = incompressible_body(OBJECT_SIZE);
    let server = spawn_proxy_server(
        config,
        cache_manager,
        range_handler,
        StubS3Client::new()
            .with_default(
                StubResponse::ok(body)
                    .with_header("etag", "\"hit-stream-etag\"")
                    .with_header("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT")
                    .with_header("content-type", "application/octet-stream"),
            )
            .into_trait_object(),
    )
    .await;
    let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
    let (status, _, len) = get(&client, server.addr, None).await;
    assert!(status.is_success(), "warm GET: {status}");
    assert_eq!(len, OBJECT_SIZE);
    for _ in 0..100 {
        if let (StatusCode::PARTIAL_CONTENT, Some(hit), OBJECT_SIZE) =
            get(&client, server.addr, Some(&whole_range())).await
        {
            if hit == "HIT" {
                return server;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the cache never served {PATH} from disk after the warm GET");
}

fn whole_range() -> String {
    format!("bytes=0-{}", OBJECT_SIZE - 1)
}

/// MB/s (10^6 bytes) of `CLIENTS` × `REQUESTS` cache hits against `addr`.
async fn hit_throughput(addr: SocketAddr) -> f64 {
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(CLIENTS);
    for _ in 0..CLIENTS {
        tasks.push(tokio::spawn(async move {
            let client: HttpClient = Client::builder(TokioExecutor::new()).build_http();
            let range = whole_range();
            for _ in 0..REQUESTS {
                let (status, x_cache, len) = get(&client, addr, Some(&range)).await;
                assert_eq!(status, StatusCode::PARTIAL_CONTENT);
                assert_eq!(x_cache.as_deref(), Some("HIT"));
                assert_eq!(len, OBJECT_SIZE);
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    (CLIENTS * REQUESTS * OBJECT_SIZE) as f64 / started.elapsed().as_secs_f64() / 1e6
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

/// Incompressible bytes, like the JPEG frames a FruitFox client reads.
fn incompressible_body(len: usize) -> Bytes {
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    Bytes::from(out)
}

/// Cache hits at 256 KiB chunks and depth 2 are served at least 97% as fast as at the shipped
/// 1 MiB and depth 4 (the median of `ROUNDS` alternating runs each).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn hits_at_256_kib_depth_2_are_as_fast_as_at_1_mib_depth_4() {
    let shipped_dir = TempDir::new().unwrap();
    let small_dir = TempDir::new().unwrap();
    let shipped = warmed_proxy(&shipped_dir, MIB, 4).await;
    let small = warmed_proxy(&small_dir, 256 * KIB, 2).await;
    // One untimed run each, so connection setup and first reads are outside the comparison.
    hit_throughput(shipped.addr).await;
    hit_throughput(small.addr).await;

    let (mut shipped_runs, mut small_runs) = (Vec::new(), Vec::new());
    for _ in 0..ROUNDS {
        shipped_runs.push(hit_throughput(shipped.addr).await);
        small_runs.push(hit_throughput(small.addr).await);
    }
    let (shipped_mbps, small_mbps) = (median(shipped_runs.clone()), median(small_runs.clone()));
    println!(
        "THROUGHPUT clients={CLIENTS} requests={REQUESTS} object={OBJECT_SIZE} \
         1MiB/4 median={shipped_mbps:.0} MB/s runs={shipped_runs:.0?} \
         256KiB/2 median={small_mbps:.0} MB/s runs={small_runs:.0?} ratio={:.3}",
        small_mbps / shipped_mbps
    );
    assert!(
        small_mbps >= 0.97 * shipped_mbps,
        "256 KiB / depth 2 served {small_mbps:.0} MB/s against {shipped_mbps:.0} MB/s"
    );
}
