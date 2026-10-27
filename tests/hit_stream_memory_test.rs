//! Heap held by a cache hit streamed from disk to a client that is not reading.
//!
//! A hit served from one whole cached extent runs two queues between the disk and the
//! socket: the blocking reader's channel (`DiskCacheManager::stream_range_data_from_queued`)
//! and the response body's frame channel (`serve_range_from_cache`). Against a client that has
//! stopped reading, both fill, so the request holds about `(2 × depth + 4) × chunk` bytes: each
//! queue's chunks, the chunk each queue's sender is waiting to hand on, the reader's decode
//! buffer, and hyper's write queue, which takes frames until it holds more than
//! `DEFAULT_MAX_BUFFER_SIZE` (about 400 KiB, `hyper/src/proto/h1/io.rs`). At the shipped 1 MiB
//! chunk and depth 4 that is about 12 MiB per stalled hit;
//! `cache.disk_streaming_chunk_size` and `cache.disk_streaming_queue_depth` size it.
//!
//! The test opens `STREAMS` such hits against one proxy at each setting, waits until the heap
//! stops growing, and compares the live heap per hit. Harness copied from
//! `tests/buffered_serve_memory_bound_test.rs`. The allocator counters are process-global, so
//! this binary holds one test.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::sync::oneshot;

use s3_proxy::cache::{CacheEvictionAlgorithm, CacheManager};
use s3_proxy::config::Config;
use s3_proxy::http_proxy::HttpProxy;
use s3_proxy::inflight_tracker::InFlightTracker;
use s3_proxy::range_handler::RangeHandler;
use s3_proxy::S3ClientApi;

use common::{StubResponse, StubS3Client};

// ---------------------------------------------------------------------------
// Counting allocator
// ---------------------------------------------------------------------------

struct CountingAlloc;

static LIVE: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every method delegates to `System` with the caller's layout and only
// adjusts one atomic on success.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc_zeroed(layout);
        if !p.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = System.realloc(ptr, layout, new_size);
        if !p.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            LIVE.fetch_add(new_size, Ordering::Relaxed);
        }
        p
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;
/// Stalled hits held open at once at each setting.
const STREAMS: usize = 16;
/// Large enough that the socket buffers between proxy and client cannot absorb the object, so
/// every stalled hit fills its queues.
const OBJECT_SIZE: usize = 32 * MIB;
const PATH: &str = "/mybucket/hit-stream-memory.bin";

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

/// One request read to the end: status, `x-cache`, body length.
async fn full_request(
    addr: SocketAddr,
    range: Option<&str>,
) -> (StatusCode, Option<String>, usize) {
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
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
    let len = response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .len();
    (status, x_cache, len)
}

/// A GET of the whole cached extent whose client reads the response head and then stops
/// reading. Its receive buffer is shrunk so the kernel holds little of the body for it.
async fn stalled_hit(addr: SocketAddr) -> TcpStream {
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let mut stream = socket.connect(addr).await.unwrap();
    let request = format!(
        "GET {PATH} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: {AUTHORIZATION}\r\n\
         Range: bytes=0-{}\r\n\r\n",
        OBJECT_SIZE - 1
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        let n = tokio::time::timeout(Duration::from_secs(30), stream.read(&mut byte))
            .await
            .expect("response head timed out")
            .unwrap();
        assert_eq!(n, 1, "connection closed before the response head ended");
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
    assert!(head.starts_with("http/1.1 206"), "stalled GET: {head}");
    assert!(
        head.contains("x-cache: hit"),
        "stalled GET must be a hit: {head}"
    );
    stream
}

/// Wait until the live heap has not moved by more than 64 KiB for 500 ms, and return it.
async fn settled_live() -> usize {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut last = LIVE.load(Ordering::Relaxed);
    let mut stable_since = Instant::now();
    while Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let now = LIVE.load(Ordering::Relaxed);
        if now.abs_diff(last) > 64 * KIB {
            last = now;
            stable_since = Instant::now();
        } else if stable_since.elapsed() >= Duration::from_millis(500) {
            return now;
        }
    }
    LIVE.load(Ordering::Relaxed)
}

/// Live heap per stalled hit for one setting, on a proxy of its own.
async fn heap_per_stalled_hit(chunk_size: usize, queue_depth: usize) -> usize {
    let dir = TempDir::new().unwrap();
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

    let (status, _, len) = full_request(server.addr, None).await;
    assert!(status.is_success(), "warm GET: {status}");
    assert_eq!(len, OBJECT_SIZE);
    // The disk write may land just after the warm response ends.
    let whole = format!("bytes=0-{}", OBJECT_SIZE - 1);
    let mut cached = false;
    for _ in 0..100 {
        if let (StatusCode::PARTIAL_CONTENT, Some(hit), OBJECT_SIZE) =
            full_request(server.addr, Some(&whole)).await
        {
            if hit == "HIT" {
                cached = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        cached,
        "the cache never served {PATH} from disk after the warm GET"
    );

    let base = settled_live().await;
    let mut held = Vec::with_capacity(STREAMS);
    for _ in 0..STREAMS {
        held.push(stalled_hit(server.addr).await);
    }
    let growth = settled_live().await.saturating_sub(base);
    drop(held);
    growth / STREAMS
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

/// A stalled hit at 256 KiB chunks and depth 2 holds under a third of the heap it holds at the
/// shipped 1 MiB and depth 4.
///
/// Both figures are measured in the same process with the same object and client, so the
/// comparison does not depend on the host. The absolute figures are printed rather than
/// asserted: part of each chunk in flight sits in the kernel's socket buffer, so how many
/// chunks are on the heap at once varies a little with the chunk size and the host.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stalled_hit_holds_its_two_queues_of_the_configured_chunk() {
    let shipped = heap_per_stalled_hit(MIB, 4).await;
    let small = heap_per_stalled_hit(256 * KIB, 2).await;
    println!(
        "{STREAMS} stalled hits of a {} MiB extent: live heap per hit {} KiB at 1 MiB / depth 4, \
         {} KiB at 256 KiB / depth 2 ({:.1} x less)",
        OBJECT_SIZE / MIB,
        shipped / KIB,
        small / KIB,
        shipped as f64 / small.max(1) as f64
    );

    assert!(
        small * 3 < shipped,
        "a stalled hit held {} KiB at 256 KiB / depth 2 against {} KiB at 1 MiB / depth 4",
        small / KIB,
        shipped / KIB
    );
}
