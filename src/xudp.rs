use std::io::{Error, ErrorKind, Result};
use std::net::{Ipv4Addr, Ipv6Addr};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const STATUS_NEW: u8 = 1;
const STATUS_KEEP: u8 = 2;
const STATUS_END: u8 = 3;
const STATUS_KEEP_ALIVE: u8 = 4;
const OPTION_DATA: u8 = 1;
const OPTION_ERROR: u8 = 2;
const NETWORK_TCP: u8 = 1;
const NETWORK_UDP: u8 = 2;

const ADDRESS_TYPE_IPV4: u8 = 1;
const ADDRESS_TYPE_DOMAIN: u8 = 2;
const ADDRESS_TYPE_IPV6: u8 = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Destination {
    pub host: String,
    pub port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Datagram {
    pub session_id: u16,
    pub destination: Destination,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Frame {
    Datagram(Datagram),
    KeepAlive,
    End,
}

pub(crate) async fn read_frame<R>(
    reader: &mut R,
    current_destination: Option<&Destination>,
) -> Result<Frame>
where
    R: AsyncRead + Unpin,
{
    let frame_len = match reader.read_u16().await {
        Ok(len) => len as usize,
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(Frame::End),
        Err(err) => return Err(err),
    };
    if frame_len < 4 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "XUDP frame length is too small",
        ));
    }

    let session_id = reader.read_u16().await?;
    let status = reader.read_u8().await?;
    let option = reader.read_u8().await?;
    let remaining_header_len = frame_len - 4;

    if option & OPTION_ERROR == OPTION_ERROR {
        return Err(Error::new(ErrorKind::ConnectionAborted, "XUDP peer closed"));
    }

    let destination = match status {
        STATUS_NEW | STATUS_KEEP => {
            read_destination(reader, remaining_header_len, current_destination).await?
        }
        STATUS_END => {
            discard_exact(reader, remaining_header_len).await?;
            return Ok(Frame::End);
        }
        STATUS_KEEP_ALIVE => {
            discard_exact(reader, remaining_header_len).await?;
            if option & OPTION_DATA == OPTION_DATA {
                discard_payload(reader).await?;
            }
            return Ok(Frame::KeepAlive);
        }
        _ => {
            discard_exact(reader, remaining_header_len).await?;
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("unsupported XUDP frame status: {}", status),
            ));
        }
    };

    if option & OPTION_DATA != OPTION_DATA {
        return Ok(Frame::KeepAlive);
    }

    let payload_len = reader.read_u16().await? as usize;
    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload).await?;

    Ok(Frame::Datagram(Datagram {
        session_id,
        destination,
        payload,
    }))
}

pub(crate) async fn write_datagram<W>(
    writer: &mut W,
    session_id: u16,
    destination: &Destination,
    payload: &[u8],
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let address = encode_address(&destination.host, destination.port)?;
    let frame_len = 5usize
        .checked_add(address.len())
        .ok_or_else(|| Error::new(ErrorKind::InvalidData, "XUDP frame too large"))?;
    let frame_len = u16::try_from(frame_len)
        .map_err(|_| Error::new(ErrorKind::InvalidData, "XUDP frame too large"))?;
    let payload_len = u16::try_from(payload.len())
        .map_err(|_| Error::new(ErrorKind::InvalidData, "udp packet too large"))?;

    writer.write_u16(frame_len).await?;
    writer.write_u16(session_id).await?;
    writer.write_u8(STATUS_KEEP).await?;
    writer.write_u8(OPTION_DATA).await?;
    writer.write_u8(NETWORK_UDP).await?;
    writer.write_all(&address).await?;
    writer.write_u16(payload_len).await?;
    writer.write_all(payload).await
}

async fn read_destination<R>(
    reader: &mut R,
    header_len: usize,
    current_destination: Option<&Destination>,
) -> Result<Destination>
where
    R: AsyncRead + Unpin,
{
    if header_len == 0 {
        return current_destination.cloned().ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "XUDP keep frame is missing destination",
            )
        });
    }
    if header_len < 1 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "XUDP destination header is too small",
        ));
    }

    let network = reader.read_u8().await?;
    if network != NETWORK_UDP {
        discard_exact(reader, header_len - 1).await?;
        let network_name = if network == NETWORK_TCP {
            "tcp"
        } else {
            "unknown"
        };
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("unsupported XUDP {} mux frame", network_name),
        ));
    }

    let mut address = vec![0u8; header_len - 1];
    reader.read_exact(&mut address).await?;
    let (destination, consumed) = decode_address(&address)?;
    if consumed > address.len() {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "XUDP destination exceeds frame length",
        ));
    }
    Ok(destination)
}

async fn discard_payload<R>(reader: &mut R) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let payload_len = reader.read_u16().await? as usize;
    discard_exact(reader, payload_len).await
}

async fn discard_exact<R>(reader: &mut R, len: usize) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut remaining = len;
    let mut buf = [0u8; 256];
    while remaining > 0 {
        let n = remaining.min(buf.len());
        reader.read_exact(&mut buf[..n]).await?;
        remaining -= n;
    }
    Ok(())
}

fn encode_address(host: &str, port: u16) -> Result<Vec<u8>> {
    let host = host
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .unwrap_or(host);

    let mut encoded = Vec::new();
    encoded.extend_from_slice(&port.to_be_bytes());

    if let Ok(address) = host.parse::<Ipv4Addr>() {
        encoded.push(ADDRESS_TYPE_IPV4);
        encoded.extend_from_slice(&address.octets());
        return Ok(encoded);
    }

    if let Ok(address) = host.parse::<Ipv6Addr>() {
        encoded.push(ADDRESS_TYPE_IPV6);
        encoded.extend_from_slice(&address.octets());
        return Ok(encoded);
    }

    if host.len() > u8::MAX as usize {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "XUDP destination domain cannot exceed 255 bytes",
        ));
    }
    encoded.push(ADDRESS_TYPE_DOMAIN);
    encoded.push(host.len() as u8);
    encoded.extend_from_slice(host.as_bytes());
    Ok(encoded)
}

fn decode_address(data: &[u8]) -> Result<(Destination, usize)> {
    if data.len() < 3 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "XUDP destination address is too short",
        ));
    }

    let port = u16::from_be_bytes([data[0], data[1]]);
    let address_type = data[2];
    let mut consumed = 3;
    let host = match address_type {
        ADDRESS_TYPE_IPV4 => {
            if data.len() < consumed + 4 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "truncated XUDP IPv4 destination",
                ));
            }
            let address = Ipv4Addr::new(
                data[consumed],
                data[consumed + 1],
                data[consumed + 2],
                data[consumed + 3],
            );
            consumed += 4;
            address.to_string()
        }
        ADDRESS_TYPE_DOMAIN => {
            if data.len() < consumed + 1 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "truncated XUDP domain length",
                ));
            }
            let len = data[consumed] as usize;
            consumed += 1;
            if data.len() < consumed + len {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "truncated XUDP domain destination",
                ));
            }
            let host = String::from_utf8(data[consumed..consumed + len].to_vec()).map_err(|e| {
                Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid XUDP domain destination: {}", e),
                )
            })?;
            consumed += len;
            host
        }
        ADDRESS_TYPE_IPV6 => {
            if data.len() < consumed + 16 {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "truncated XUDP IPv6 destination",
                ));
            }
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&data[consumed..consumed + 16]);
            consumed += 16;
            format!("[{}]", Ipv6Addr::from(octets))
        }
        _ => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid XUDP address type: {}", address_type),
            ));
        }
    };

    Ok((Destination { host, port }, consumed))
}

#[cfg(test)]
mod tests {
    use super::{read_frame, write_datagram, Datagram, Destination, Frame};
    use std::future::Future;
    use std::io::Cursor;
    use std::pin::pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    fn block_on<T>(future: impl Future<Output = T>) -> T {
        fn raw_waker() -> RawWaker {
            fn clone(_: *const ()) -> RawWaker {
                raw_waker()
            }
            fn noop(_: *const ()) {}
            RawWaker::new(
                std::ptr::null(),
                &RawWakerVTable::new(clone, noop, noop, noop),
            )
        }

        let waker = unsafe { Waker::from_raw(raw_waker()) };
        let mut context = Context::from_waker(&waker);
        let mut future = pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("test future unexpectedly pending"),
        }
    }

    #[test]
    fn parses_new_udp_domain_frame() {
        let mut frame = vec![0, 20, 0x12, 0x34, 1, 1, 2, 0x01, 0xbb, 2, 11];
        frame.extend_from_slice(b"example.com");
        frame.extend_from_slice(&[0, 3, 1, 2, 3]);

        assert_eq!(
            block_on(read_frame(&mut Cursor::new(frame), None)).unwrap(),
            Frame::Datagram(Datagram {
                session_id: 0x1234,
                destination: Destination {
                    host: "example.com".to_string(),
                    port: 443,
                },
                payload: vec![1, 2, 3],
            })
        );
    }

    #[test]
    fn parses_keep_udp_ipv4_frame() {
        let frame = vec![0, 12, 0, 7, 2, 1, 2, 0, 53, 1, 192, 0, 2, 1, 0, 1, 9];

        assert_eq!(
            block_on(read_frame(&mut Cursor::new(frame), None)).unwrap(),
            Frame::Datagram(Datagram {
                session_id: 7,
                destination: Destination {
                    host: "192.0.2.1".to_string(),
                    port: 53,
                },
                payload: vec![9],
            })
        );
    }

    #[test]
    fn parses_keep_frame_reusing_current_destination() {
        let destination = Destination {
            host: "example.com".to_string(),
            port: 443,
        };
        let frame = vec![0, 4, 0, 1, 2, 1, 0, 2, 4, 5];

        assert_eq!(
            block_on(read_frame(&mut Cursor::new(frame), Some(&destination))).unwrap(),
            Frame::Datagram(Datagram {
                session_id: 1,
                destination,
                payload: vec![4, 5],
            })
        );
    }

    #[test]
    fn ignores_global_id_in_new_frame() {
        let mut frame = vec![0, 20, 0, 1, 1, 1, 2, 0, 53, 1, 192, 0, 2, 1];
        frame.extend_from_slice(&[8; 8]);
        frame.extend_from_slice(&[0, 1, 7]);

        assert_eq!(
            block_on(read_frame(&mut Cursor::new(frame), None)).unwrap(),
            Frame::Datagram(Datagram {
                session_id: 1,
                destination: Destination {
                    host: "192.0.2.1".to_string(),
                    port: 53,
                },
                payload: vec![7],
            })
        );
    }

    #[test]
    fn rejects_tcp_mux_frame() {
        let frame = vec![0, 12, 0, 1, 1, 1, 1, 0, 80, 1, 192, 0, 2, 1, 0, 1, 7];

        assert!(block_on(read_frame(&mut Cursor::new(frame), None)).is_err());
    }

    #[test]
    fn rejects_malformed_frame() {
        assert!(block_on(read_frame(&mut Cursor::new(vec![0, 2, 0, 1]), None)).is_err());
        assert!(block_on(read_frame(
            &mut Cursor::new(vec![0, 5, 0, 1, 1, 1, 2, 0]),
            None
        ))
        .is_err());
    }

    #[test]
    fn encodes_response_frame() {
        let mut output = Vec::new();
        block_on(write_datagram(
            &mut output,
            0x1234,
            &Destination {
                host: "example.com".to_string(),
                port: 443,
            },
            &[1, 2, 3],
        ))
        .unwrap();

        assert_eq!(
            output,
            b"\x00\x14\x12\x34\x02\x01\x02\x01\xbb\x02\x0bexample.com\x00\x03\x01\x02\x03".to_vec()
        );
    }

    #[test]
    fn encodes_ipv4_and_ipv6_response_frames() {
        let mut ipv4 = Vec::new();
        block_on(write_datagram(
            &mut ipv4,
            1,
            &Destination {
                host: "192.0.2.1".to_string(),
                port: 53,
            },
            &[9],
        ))
        .unwrap();
        assert_eq!(
            ipv4,
            vec![0, 12, 0, 1, 2, 1, 2, 0, 53, 1, 192, 0, 2, 1, 0, 1, 9]
        );

        let mut ipv6 = Vec::new();
        block_on(write_datagram(
            &mut ipv6,
            1,
            &Destination {
                host: "[2001:db8::1]".to_string(),
                port: 8443,
            },
            &[],
        ))
        .unwrap();
        assert_eq!(ipv6[0..9], [0, 24, 0, 1, 2, 1, 2, 0x20, 0xfb]);
        assert_eq!(ipv6[9], 3);
    }
}
