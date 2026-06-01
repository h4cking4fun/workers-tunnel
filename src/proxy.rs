use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use worker::*;

use crate::config::OutboundTarget;
use crate::protocol::{read_tunnel_request, NETWORK_TYPE_MUX, NETWORK_TYPE_TCP, NETWORK_TYPE_UDP};
use crate::relay::{process_tcp_outbound, process_udp_outbound, process_xudp_outbound};
use crate::websocket::WebSocketStream;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const DIRECT_FAILURE_CACHE_TTL_MS: u64 = 30 * 60 * 1000;

static DIRECT_FAILURE_CACHE: OnceLock<Mutex<DirectFailureCache>> = OnceLock::new();

pub(crate) async fn run_tunnel(
    mut client_socket: WebSocketStream<'_>,
    user_id: [u8; 16],
    proxy_ip: &[OutboundTarget],
    debug_log: bool,
) -> Result<()> {
    let request = tokio::select! {
        result = read_tunnel_request(&mut client_socket, &user_id) => result?,
        _ = Delay::from(HANDSHAKE_TIMEOUT) => {
            return Err(Error::new(
                ErrorKind::TimedOut,
                "tunnel handshake timed out",
            ));
        }
    };

    match request.network_type {
        NETWORK_TYPE_TCP => {
            let mut last_error = None;
            let use_direct_failure_cache = !proxy_ip.is_empty();
            let original_target = OutboundTarget::Direct {
                host: request.remote_addr.clone(),
                port: Some(request.remote_port),
            };

            for target in std::iter::once(&original_target).chain(proxy_ip.iter()) {
                let direct_cache_key = target.direct_cache_key(request.remote_port);
                if use_direct_failure_cache {
                    if let Some(cache_key) = direct_cache_key.as_deref() {
                        if is_direct_failure_cached(cache_key) {
                            if debug_log {
                                console_log!("skip cached direct target: {}", cache_key);
                            }
                            last_error = Some(Error::new(
                                ErrorKind::ConnectionRefused,
                                format!("cached direct connect failure: {}", cache_key),
                            ));
                            continue;
                        }
                    }
                }

                match process_tcp_outbound(&mut client_socket, target, &request, debug_log).await {
                    Ok(_) => return Ok(()),
                    Err(e) if e.kind() == ErrorKind::ConnectionRefused => {
                        if use_direct_failure_cache {
                            if let Some(cache_key) = direct_cache_key {
                                cache_direct_failure(&cache_key);
                            }
                        }
                        last_error = Some(e);
                        continue;
                    }
                    Err(e) => return Err(e),
                }
            }

            Err(last_error.unwrap_or_else(|| {
                Error::new(ErrorKind::ConnectionRefused, "no target to connect")
            }))
        }
        NETWORK_TYPE_UDP => {
            process_udp_outbound(&mut client_socket, &request, proxy_ip, debug_log).await
        }
        NETWORK_TYPE_MUX => process_xudp_outbound(&mut client_socket, proxy_ip, debug_log).await,
        unknown => Err(Error::new(
            ErrorKind::InvalidData,
            format!("unsupported network type: {}", unknown),
        )),
    }
}

fn is_direct_failure_cached(cache_key: &str) -> bool {
    let now_ms = Date::now().as_millis();
    direct_failure_cache()
        .lock()
        .map(|mut cache| cache.is_cached(cache_key, now_ms))
        .unwrap_or(false)
}

fn cache_direct_failure(cache_key: &str) {
    let now_ms = Date::now().as_millis();
    if let Ok(mut cache) = direct_failure_cache().lock() {
        cache.record_failure(cache_key, now_ms, DIRECT_FAILURE_CACHE_TTL_MS);
    }
}

fn direct_failure_cache() -> &'static Mutex<DirectFailureCache> {
    DIRECT_FAILURE_CACHE.get_or_init(|| Mutex::new(DirectFailureCache::default()))
}

#[derive(Default)]
struct DirectFailureCache {
    expires_at_by_target: HashMap<String, u64>,
}

impl DirectFailureCache {
    fn is_cached(&mut self, cache_key: &str, now_ms: u64) -> bool {
        match self.expires_at_by_target.get(cache_key).copied() {
            Some(expires_at) if expires_at > now_ms => true,
            Some(_) => {
                self.expires_at_by_target.remove(cache_key);
                false
            }
            None => false,
        }
    }

    fn record_failure(&mut self, cache_key: &str, now_ms: u64, ttl_ms: u64) {
        self.expires_at_by_target
            .insert(cache_key.to_string(), now_ms.saturating_add(ttl_ms));
    }
}

#[cfg(test)]
mod tests {
    use super::DirectFailureCache;

    #[test]
    fn caches_new_direct_failures() {
        let mut cache = DirectFailureCache::default();
        cache.record_failure("example.com:443", 1000, 5000);

        assert!(cache.is_cached("example.com:443", 1001));
    }

    #[test]
    fn expires_cached_direct_failures() {
        let mut cache = DirectFailureCache::default();
        cache.record_failure("example.com:443", 1000, 5000);

        assert!(!cache.is_cached("example.com:443", 6000));
        assert!(!cache.is_cached("example.com:443", 6001));
    }

    #[test]
    fn direct_failure_cache_key_includes_host_and_port() {
        let mut cache = DirectFailureCache::default();
        cache.record_failure("example.com:443", 1000, 5000);

        assert!(cache.is_cached("example.com:443", 1001));
        assert!(!cache.is_cached("example.com:8443", 1001));
        assert!(!cache.is_cached("other.example.com:443", 1001));
    }
}
