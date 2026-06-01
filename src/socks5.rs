use std::io::{Error, ErrorKind, Result};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use worker::{Delay, Socket};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn connect(
    proxy_host: &str,
    proxy_port: u16,
    username: Option<&str>,
    password: Option<&str>,
    destination_host: &str,
    destination_port: u16,
) -> Result<Socket> {
    let socket = Socket::builder()
        .connect(proxy_host, proxy_port)
        .map_err(|e| {
            Error::new(
                ErrorKind::ConnectionRefused,
                format!("connect to SOCKS5 proxy failed: {}", e),
            )
        })?;
    let mut socket = wait_socket_opened(socket, "SOCKS5 proxy socket not opened").await?;

    let auth_method = if username.is_some() { 0x02 } else { 0x00 };
    socket.write_all(&[0x05, 0x01, auth_method]).await?;

    let mut response = [0u8; 2];
    socket.read_exact(&mut response).await?;
    if response != [0x05, auth_method] {
        return Err(Error::new(
            ErrorKind::ConnectionRefused,
            "SOCKS5 proxy rejected requested authentication method",
        ));
    }

    if let Some(username) = username {
        authenticate(&mut socket, username, password.unwrap_or_default()).await?;
    }

    let request = connect_request(destination_host, destination_port)?;
    socket.write_all(&request).await?;
    read_connect_response(&mut socket).await?;

    Ok(socket)
}

async fn wait_socket_opened(socket: Socket, error_message: &'static str) -> Result<Socket> {
    tokio::select! {
        result = socket.opened() => {
            result.map_err(|e| {
                Error::new(ErrorKind::ConnectionRefused, format!("{}: {}", error_message, e))
            })?;
            Ok(socket)
        }
        _ = Delay::from(CONNECT_TIMEOUT) => {
            Err(Error::new(ErrorKind::ConnectionRefused, "connect to SOCKS5 proxy timed out"))
        }
    }
}

async fn authenticate(socket: &mut Socket, username: &str, password: &str) -> Result<()> {
    let mut request = Vec::with_capacity(username.len() + password.len() + 3);
    request.push(0x01);
    request.push(username.len() as u8);
    request.extend_from_slice(username.as_bytes());
    request.push(password.len() as u8);
    request.extend_from_slice(password.as_bytes());
    socket.write_all(&request).await?;

    let mut response = [0u8; 2];
    socket.read_exact(&mut response).await?;
    if response != [0x01, 0x00] {
        return Err(Error::new(
            ErrorKind::ConnectionRefused,
            "SOCKS5 username/password authentication failed",
        ));
    }
    Ok(())
}

fn connect_request(destination_host: &str, destination_port: u16) -> Result<Vec<u8>> {
    let address = encode_address(destination_host)?;
    let mut request = Vec::with_capacity(address.len() + 6);
    request.extend_from_slice(&[0x05, 0x01, 0x00]);
    request.extend_from_slice(&address);
    request.extend_from_slice(&destination_port.to_be_bytes());
    Ok(request)
}

fn encode_address(host: &str) -> Result<Vec<u8>> {
    let host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);

    if let Ok(address) = host.parse::<Ipv4Addr>() {
        let mut encoded = Vec::with_capacity(5);
        encoded.push(0x01);
        encoded.extend_from_slice(&address.octets());
        return Ok(encoded);
    }

    if let Ok(address) = host.parse::<Ipv6Addr>() {
        let mut encoded = Vec::with_capacity(17);
        encoded.push(0x04);
        encoded.extend_from_slice(&address.octets());
        return Ok(encoded);
    }

    if host.len() > u8::MAX as usize {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "SOCKS5 destination domain cannot exceed 255 bytes",
        ));
    }

    let mut encoded = Vec::with_capacity(host.len() + 2);
    encoded.push(0x03);
    encoded.push(host.len() as u8);
    encoded.extend_from_slice(host.as_bytes());
    Ok(encoded)
}

async fn read_connect_response(socket: &mut Socket) -> Result<()> {
    let mut header = [0u8; 4];
    socket.read_exact(&mut header).await?;

    if header[0] != 0x05 {
        return Err(Error::new(
            ErrorKind::ConnectionRefused,
            "invalid SOCKS5 connect response version",
        ));
    }

    if header[1] != 0x00 {
        return Err(Error::new(
            ErrorKind::ConnectionRefused,
            format!("SOCKS5 connect request failed with status {}", header[1]),
        ));
    }

    match header[3] {
        0x01 => {
            let mut address = [0u8; 4];
            socket.read_exact(&mut address).await?;
        }
        0x03 => {
            let length = socket.read_u8().await? as usize;
            let mut address = vec![0u8; length];
            socket.read_exact(&mut address).await?;
        }
        0x04 => {
            let mut address = [0u8; 16];
            socket.read_exact(&mut address).await?;
        }
        _ => {
            return Err(Error::new(
                ErrorKind::ConnectionRefused,
                "invalid SOCKS5 connect response address type",
            ));
        }
    }

    let mut port = [0u8; 2];
    socket.read_exact(&mut port).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::encode_address;

    #[test]
    fn encodes_domain_destination() {
        assert_eq!(
            encode_address("example.com").unwrap(),
            b"\x03\x0bexample.com".to_vec()
        );
    }

    #[test]
    fn encodes_ipv4_destination() {
        assert_eq!(
            encode_address("192.0.2.1").unwrap(),
            vec![0x01, 192, 0, 2, 1]
        );
    }

    #[test]
    fn encodes_bracketed_ipv6_destination() {
        assert_eq!(
            encode_address("[2001:db8::1]").unwrap(),
            [
                vec![0x04],
                "2001:db8::1"
                    .parse::<std::net::Ipv6Addr>()
                    .unwrap()
                    .octets()
                    .to_vec()
            ]
            .concat()
        );
    }
}
