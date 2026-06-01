use std::io::{Error, ErrorKind, Result};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use worker::Socket;

use crate::socks5;

const UOT_V2_MAGIC_HOST: &str = "sp.v2.udp-over-tcp.arpa";
const UOT_V2_MAGIC_PORT: u16 = 0;

pub async fn connect(
    proxy_host: &str,
    proxy_port: u16,
    username: Option<&str>,
    password: Option<&str>,
    destination_host: &str,
    destination_port: u16,
) -> Result<Socket> {
    let mut socket = socks5::connect(
        proxy_host,
        proxy_port,
        username,
        password,
        UOT_V2_MAGIC_HOST,
        UOT_V2_MAGIC_PORT,
    )
    .await?;

    let request = connect_request(destination_host, destination_port)?;
    socket.write_all(&request).await?;

    Ok(socket)
}

pub fn connect_request(destination_host: &str, destination_port: u16) -> Result<Vec<u8>> {
    let address = socks5::encode_address(destination_host)?;
    let mut request = Vec::with_capacity(address.len() + 3);
    request.push(0x01);
    request.extend_from_slice(&address);
    request.extend_from_slice(&destination_port.to_be_bytes());
    Ok(request)
}

pub async fn read_datagram<R>(reader: &mut R, max_len: usize) -> Result<Option<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let len = match reader.read_u16().await {
        Ok(len) => len as usize,
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    };

    if len > max_len {
        return Err(Error::new(ErrorKind::InvalidData, "udp packet too large"));
    }

    let mut packet = vec![0u8; len];
    reader.read_exact(&mut packet).await?;
    Ok(Some(packet))
}

pub async fn write_datagram<W>(writer: &mut W, packet: &[u8]) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let len = u16::try_from(packet.len())
        .map_err(|_| Error::new(ErrorKind::InvalidData, "udp packet too large"))?;
    writer.write_u16(len).await?;
    writer.write_all(packet).await
}

#[cfg(test)]
mod tests {
    use super::connect_request;

    #[test]
    fn encodes_domain_connect_request() {
        assert_eq!(
            connect_request("example.com", 443).unwrap(),
            b"\x01\x03\x0bexample.com\x01\xbb".to_vec()
        );
    }

    #[test]
    fn encodes_ipv4_connect_request() {
        assert_eq!(
            connect_request("192.0.2.1", 53).unwrap(),
            vec![0x01, 0x01, 192, 0, 2, 1, 0, 53]
        );
    }

    #[test]
    fn encodes_bracketed_ipv6_connect_request() {
        assert_eq!(
            connect_request("[2001:db8::1]", 8443).unwrap(),
            [
                vec![0x01, 0x04],
                "2001:db8::1"
                    .parse::<std::net::Ipv6Addr>()
                    .unwrap()
                    .octets()
                    .to_vec(),
                vec![0x20, 0xfb],
            ]
            .concat()
        );
    }

    #[test]
    fn rejects_too_large_domain_connect_request() {
        let host = "a".repeat(256);
        assert!(connect_request(&host, 443).is_err());
    }
}
