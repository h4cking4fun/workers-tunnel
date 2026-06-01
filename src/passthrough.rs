use std::io::{Error, ErrorKind, Result};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use worker::*;

use crate::websocket::WebSocketStream;

const COPY_BUF_SIZE: usize = 32 * 1024;
const RELAY_TIMEOUT: Duration = Duration::from_secs(900);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn is_relay_path(path: &str) -> bool {
    path == "/relay"
}

pub(crate) async fn run_passthrough_tunnel(
    mut client_socket: WebSocketStream<'_>,
    backend_url: String,
    debug_log: bool,
) -> Result<()> {
    if backend_url.trim().is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "RELAY_BACKEND_URL is required for /relay",
        ));
    }

    let log_url = sanitize_backend_url(&backend_url);
    if debug_log {
        console_log!("relay backend connect: {}", log_url);
    }

    let backend_url = parse_backend_url(&backend_url)?;
    let backend = WebSocket::connect(backend_url)
        .await
        .map_err(|err| Error::new(ErrorKind::ConnectionRefused, err.to_string()))?;
    let backend_events = backend
        .events()
        .map_err(|err| Error::other(err.to_string()))?;
    backend
        .accept()
        .map_err(|err| Error::other(err.to_string()))?;

    if debug_log {
        console_log!("relay backend connected: {}", log_url);
    }

    let mut backend_socket = WebSocketStream::new(&backend, backend_events, None);
    relay_stream(&mut client_socket, &mut backend_socket, &log_url, debug_log).await
}

async fn relay_stream(
    client_socket: &mut WebSocketStream<'_>,
    backend_socket: &mut WebSocketStream<'_>,
    log_url: &str,
    debug_log: bool,
) -> Result<()> {
    let (mut cr, mut cw) = tokio::io::split(client_socket);
    let (mut br, mut bw) = tokio::io::split(backend_socket);

    let c2b = async {
        let mut buf = vec![0u8; COPY_BUF_SIZE];
        loop {
            let n = cr.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            bw.write_all(&buf[..n]).await?;
        }
        bw.shutdown().await?;
        Ok::<_, Error>(())
    };
    tokio::pin!(c2b);

    let b2c = async {
        let mut buf = vec![0u8; COPY_BUF_SIZE];
        loop {
            let n = br.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            cw.write_all(&buf[..n]).await?;
        }
        cw.flush().await?;
        cw.shutdown().await?;
        Ok::<_, Error>(())
    };
    tokio::pin!(b2c);

    let result = tokio::select! {
        result = &mut c2b => {
            tokio::select! {
                _ = &mut b2c => {}
                _ = Delay::from(DRAIN_TIMEOUT) => {}
            };
            result
        }
        result = &mut b2c => {
            tokio::select! {
                _ = &mut c2b => {}
                _ = Delay::from(DRAIN_TIMEOUT) => {}
            };
            result
        }
        _ = Delay::from(RELAY_TIMEOUT) => {
            if debug_log {
                console_log!("relay stream idle timeout: {}", log_url);
            }
            return Ok(());
        }
    };

    if let Err(err) = result {
        console_log!("relay stream ended: {} - {}", log_url, err);
    } else if debug_log {
        console_log!("relay stream ended: {}", log_url);
    }

    Ok(())
}

pub(crate) fn sanitize_backend_url(value: &str) -> String {
    let Ok(mut url) = Url::parse(value) else {
        return "<invalid relay backend url>".to_string();
    };
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.to_string()
}

fn parse_backend_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|err| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("invalid RELAY_BACKEND_URL: {}", err),
        )
    })?;
    if url.scheme() != "wss" {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "RELAY_BACKEND_URL must use wss://",
        ));
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::{is_relay_path, parse_backend_url, sanitize_backend_url};

    #[test]
    fn relay_path_requires_exact_match() {
        assert!(is_relay_path("/relay"));
        assert!(!is_relay_path("/relay/"));
        assert!(!is_relay_path("/mux"));
        assert!(!is_relay_path("/ws"));
        assert!(!is_relay_path("/"));
    }

    #[test]
    fn sanitized_backend_url_strips_userinfo() {
        assert_eq!(
            sanitize_backend_url("wss://user:pass@backend.example.com:8443/vless?x=1"),
            "wss://backend.example.com:8443/vless?x=1"
        );
    }

    #[test]
    fn sanitized_backend_url_handles_invalid_values() {
        assert_eq!(
            sanitize_backend_url("not a url"),
            "<invalid relay backend url>"
        );
    }

    #[test]
    fn backend_url_requires_wss() {
        assert!(parse_backend_url("wss://backend.example.com/vless").is_ok());
        assert!(parse_backend_url("ws://backend.example.com/vless").is_err());
        assert!(parse_backend_url("https://backend.example.com/vless").is_err());
    }
}
