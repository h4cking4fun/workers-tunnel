use std::io::{Error, ErrorKind, Result};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use worker::*;

use crate::config::OutboundTarget;
use crate::protocol::{self, TunnelRequest};
use crate::socks5;
use crate::websocket::WebSocketStream;

const COPY_BUF_SIZE: usize = 32 * 1024;

const DIRECT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const RELAY_TIMEOUT: Duration = Duration::from_secs(900);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const DNS_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) async fn process_tcp_outbound(
    client_socket: &mut WebSocketStream<'_>,
    outbound_target: &OutboundTarget,
    request: &TunnelRequest,
    debug_log: bool,
) -> Result<()> {
    let log_target = outbound_target.log_target(request.remote_port);
    if debug_log {
        console_log!("connect to remote: {}", log_target);
    }

    let mut remote_socket = open_outbound_socket(outbound_target, request).await?;

    client_socket
        .write_all(&protocol::RESPONSE)
        .await
        .map_err(|e| {
            Error::new(
                ErrorKind::ConnectionAborted,
                format!("send response header failed: {}", e),
            )
        })?;
    client_socket.flush().await?;

    let (mut cr, mut cw) = tokio::io::split(client_socket);
    let (mut rr, mut rw) = tokio::io::split(&mut remote_socket);

    let c2r = async {
        let mut buf = vec![0u8; COPY_BUF_SIZE];
        loop {
            let n = cr.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            rw.write_all(&buf[..n]).await?;
        }
        rw.shutdown().await?;
        Ok::<_, Error>(())
    };
    tokio::pin!(c2r);

    let r2c = async {
        let mut buf = vec![0u8; COPY_BUF_SIZE];
        loop {
            let n = rr.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            cw.write_all(&buf[..n]).await?;
        }
        cw.flush().await?;
        cw.shutdown().await?;
        Ok::<_, Error>(())
    };
    tokio::pin!(r2c);

    let result = tokio::select! {
        result = &mut c2r => {
            tokio::select! {
                _ = &mut r2c => {}
                _ = Delay::from(DRAIN_TIMEOUT) => {}
            };
            result
        }
        result = &mut r2c => {
            tokio::select! {
                _ = &mut c2r => {}
                _ = Delay::from(DRAIN_TIMEOUT) => {}
            };
            result
        }
        _ = Delay::from(RELAY_TIMEOUT) => {
            console_log!("relay idle timeout: {}", log_target);
            return Ok(());
        }
    };

    if let Err(e) = result {
        console_log!("forward data ended: {} - {}", log_target, e);
    }

    Ok(())
}

pub(crate) async fn process_udp_outbound(
    client_socket: &mut WebSocketStream<'_>,
    request: &TunnelRequest,
    proxy_ip: &[OutboundTarget],
    debug_log: bool,
) -> Result<()> {
    if udp_route(request, proxy_ip) == UdpRoute::DnsOverHttps {
        return process_dns_udp_outbound(client_socket).await;
    }

    let mut last_error = None;
    for outbound_target in proxy_ip.iter().filter(|target| target.is_socks5()) {
        let log_target = outbound_target.log_target(request.remote_port);
        if debug_log {
            console_log!("connect udp via UoT: {}", log_target);
        }

        match open_uot_socket(outbound_target, request).await {
            Ok(remote_socket) => {
                if debug_log {
                    console_log!(
                        "uot connected: {} -> {}:{}",
                        log_target,
                        request.remote_addr,
                        request.remote_port
                    );
                }
                return relay_uot_outbound(client_socket, remote_socket, &log_target, debug_log)
                    .await;
            }
            Err(err) if err.kind() == ErrorKind::ConnectionRefused => {
                last_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }

    if let Some(err) = last_error {
        Err(err)
    } else {
        let target = format!("{}:{}", request.remote_addr, request.remote_port);
        console_log!("unsupported udp proxy request: {}", target);
        Err(Error::new(
            ErrorKind::InvalidData,
            format!("not supported udp proxy yet: {}", target),
        ))
    }
}

pub(crate) async fn process_xudp_outbound(
    client_socket: &mut WebSocketStream<'_>,
    proxy_ip: &[OutboundTarget],
    debug_log: bool,
) -> Result<()> {
    let first_datagram = read_first_xudp_datagram(client_socket).await?;
    let mut last_error = None;

    for outbound_target in proxy_ip.iter().filter(|target| target.is_socks5()) {
        let log_target = outbound_target.log_target(first_datagram.destination.port);
        if debug_log {
            console_log!(
                "xudp route selected: {} -> {}:{}",
                log_target,
                first_datagram.destination.host,
                first_datagram.destination.port
            );
        }

        match open_uot_destination(outbound_target, &first_datagram.destination).await {
            Ok(remote_socket) => {
                if debug_log {
                    console_log!(
                        "uot connected: {} -> {}:{}",
                        log_target,
                        first_datagram.destination.host,
                        first_datagram.destination.port
                    );
                }
                return relay_xudp_outbound(
                    client_socket,
                    remote_socket,
                    first_datagram,
                    &log_target,
                    debug_log,
                )
                .await;
            }
            Err(err) if err.kind() == ErrorKind::ConnectionRefused => {
                last_error = Some(err);
            }
            Err(err) => return Err(err),
        }
    }

    if let Some(err) = last_error {
        Err(err)
    } else {
        Err(Error::new(
            ErrorKind::InvalidData,
            "not supported XUDP proxy yet: no SOCKS5 UoT target configured",
        ))
    }
}

async fn read_first_xudp_datagram(
    client_socket: &mut WebSocketStream<'_>,
) -> Result<crate::xudp::Datagram> {
    let current_destination: Option<crate::xudp::Destination> = None;
    loop {
        match crate::xudp::read_frame(client_socket, current_destination.as_ref()).await? {
            crate::xudp::Frame::Datagram(datagram) => return Ok(datagram),
            crate::xudp::Frame::KeepAlive => {}
            crate::xudp::Frame::End => {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "XUDP stream ended before first datagram",
                ));
            }
        }
    }
}

async fn process_dns_udp_outbound(client_socket: &mut WebSocketStream<'_>) -> Result<()> {
    client_socket
        .write_all(&protocol::RESPONSE)
        .await
        .map_err(|e| {
            Error::new(
                ErrorKind::ConnectionAborted,
                format!("send response header failed: {}", e),
            )
        })?;
    client_socket.flush().await?;

    const MAX_DNS_PACKET: usize = 4096;
    let mut buf = [0u8; MAX_DNS_PACKET];

    loop {
        let Ok(len) = client_socket.read_u16().await else {
            return Ok(());
        };
        let len = len as usize;
        if len > MAX_DNS_PACKET {
            return Err(Error::new(ErrorKind::InvalidData, "dns packet too large"));
        }
        client_socket.read_exact(&mut buf[..len]).await?;

        let mut init = RequestInit::new();
        init.method = Method::Post;
        init.headers = Headers::new();
        init.body = Some(buf[..len].to_vec().into());
        _ = init.headers.set("Content-Type", "application/dns-message");

        let request = Request::new_with_init("https://1.1.1.1/dns-query", &init)
            .map_err(|e| Error::other(format!("create DNS request failed: {}", e)))?;

        let dns_fetch = async {
            let mut response = Fetch::Request(request).send().await.map_err(|e| {
                Error::new(
                    ErrorKind::ConnectionAborted,
                    format!("send DNS-over-HTTP request failed: {}", e),
                )
            })?;
            response.bytes().await.map_err(|e| {
                Error::new(
                    ErrorKind::ConnectionAborted,
                    format!("DNS-over-HTTP response body error: {}", e),
                )
            })
        };

        let data = tokio::select! {
            result = dns_fetch => result?,
            _ = Delay::from(DNS_TIMEOUT) => {
                return Err(Error::new(ErrorKind::TimedOut, "DNS query timed out"));
            }
        };

        client_socket.write_u16(data.len() as u16).await?;
        client_socket.write_all(&data).await?;
        client_socket.flush().await?;
    }
}

async fn relay_uot_outbound(
    client_socket: &mut WebSocketStream<'_>,
    mut remote_socket: Socket,
    log_target: &str,
    debug_log: bool,
) -> Result<()> {
    client_socket
        .write_all(&protocol::RESPONSE)
        .await
        .map_err(|e| {
            Error::new(
                ErrorKind::ConnectionAborted,
                format!("send response header failed: {}", e),
            )
        })?;
    client_socket.flush().await?;

    const MAX_UDP_PACKET: usize = u16::MAX as usize;

    let (mut cr, mut cw) = tokio::io::split(client_socket);
    let (mut rr, mut rw) = tokio::io::split(&mut remote_socket);

    let c2r = async {
        while let Some(packet) = crate::uot::read_datagram(&mut cr, MAX_UDP_PACKET).await? {
            crate::uot::write_datagram(&mut rw, &packet).await?;
        }
        rw.shutdown().await?;
        Ok::<_, Error>(())
    };
    tokio::pin!(c2r);

    let r2c = async {
        let mut first_response_logged = false;
        while let Some(packet) = crate::uot::read_datagram(&mut rr, MAX_UDP_PACKET).await? {
            if debug_log && !first_response_logged {
                console_log!(
                    "uot first response: {} bytes via {}",
                    packet.len(),
                    log_target
                );
                first_response_logged = true;
            }
            crate::uot::write_datagram(&mut cw, &packet).await?;
            cw.flush().await?;
        }
        cw.shutdown().await?;
        Ok::<_, Error>(())
    };
    tokio::pin!(r2c);

    let result = tokio::select! {
        result = &mut c2r => {
            tokio::select! {
                _ = &mut r2c => {}
                _ = Delay::from(DRAIN_TIMEOUT) => {}
            };
            result
        }
        result = &mut r2c => {
            tokio::select! {
                _ = &mut c2r => {}
                _ = Delay::from(DRAIN_TIMEOUT) => {}
            };
            result
        }
        _ = Delay::from(RELAY_TIMEOUT) => {
            console_log!("relay idle timeout: {}", log_target);
            return Ok(());
        }
    };

    if let Err(e) = result {
        console_log!("forward udp data ended: {} - {}", log_target, e);
    }

    Ok(())
}

async fn relay_xudp_outbound(
    client_socket: &mut WebSocketStream<'_>,
    mut remote_socket: Socket,
    first_datagram: crate::xudp::Datagram,
    log_target: &str,
    debug_log: bool,
) -> Result<()> {
    client_socket
        .write_all(&protocol::RESPONSE)
        .await
        .map_err(|e| {
            Error::new(
                ErrorKind::ConnectionAborted,
                format!("send response header failed: {}", e),
            )
        })?;
    client_socket.flush().await?;

    const MAX_UDP_PACKET: usize = u16::MAX as usize;

    let initial_destination = first_datagram.destination.clone();
    let latest_response_target = Arc::new(Mutex::new((
        first_datagram.session_id,
        first_datagram.destination.clone(),
    )));

    let (mut cr, mut cw) = tokio::io::split(client_socket);
    let (mut rr, mut rw) = tokio::io::split(&mut remote_socket);

    crate::uot::write_datagram(&mut rw, &first_datagram.payload).await?;

    let c2r_target = Arc::clone(&latest_response_target);
    let c2r = async {
        let mut current_destination = Some(initial_destination);
        loop {
            match crate::xudp::read_frame(&mut cr, current_destination.as_ref()).await? {
                crate::xudp::Frame::Datagram(datagram) => {
                    current_destination = Some(datagram.destination.clone());
                    if let Ok(mut target) = c2r_target.lock() {
                        *target = (datagram.session_id, datagram.destination.clone());
                    }
                    crate::uot::write_datagram(&mut rw, &datagram.payload).await?;
                }
                crate::xudp::Frame::KeepAlive => {}
                crate::xudp::Frame::End => break,
            }
        }
        rw.shutdown().await?;
        Ok::<_, Error>(())
    };
    tokio::pin!(c2r);

    let r2c_target = Arc::clone(&latest_response_target);
    let r2c = async {
        let mut first_response_logged = false;
        while let Some(packet) = crate::uot::read_datagram(&mut rr, MAX_UDP_PACKET).await? {
            let (session_id, destination) = r2c_target
                .lock()
                .map(|target| target.clone())
                .map_err(|_| Error::other("XUDP response target lock poisoned"))?;
            if debug_log && !first_response_logged {
                console_log!(
                    "xudp first response: {} bytes from {}:{} via {}",
                    packet.len(),
                    destination.host,
                    destination.port,
                    log_target
                );
                first_response_logged = true;
            }
            crate::xudp::write_datagram(&mut cw, session_id, &destination, &packet).await?;
            cw.flush().await?;
        }
        cw.shutdown().await?;
        Ok::<_, Error>(())
    };
    tokio::pin!(r2c);

    let result = tokio::select! {
        result = &mut c2r => {
            tokio::select! {
                _ = &mut r2c => {}
                _ = Delay::from(DRAIN_TIMEOUT) => {}
            };
            result
        }
        result = &mut r2c => {
            tokio::select! {
                _ = &mut c2r => {}
                _ = Delay::from(DRAIN_TIMEOUT) => {}
            };
            result
        }
        _ = Delay::from(RELAY_TIMEOUT) => {
            console_log!("relay idle timeout: {}", log_target);
            return Ok(());
        }
    };

    if let Err(e) = result {
        console_log!("forward XUDP data ended: {} - {}", log_target, e);
    }

    Ok(())
}

async fn open_outbound_socket(
    outbound_target: &OutboundTarget,
    request: &TunnelRequest,
) -> Result<Socket> {
    match outbound_target {
        OutboundTarget::Direct { host, port } => {
            connect_direct(host, port.unwrap_or(request.remote_port)).await
        }
        OutboundTarget::Socks5 {
            host,
            port,
            username,
            password,
        } => socks5::connect(
            host,
            *port,
            username.as_deref(),
            password.as_deref(),
            &request.remote_addr,
            request.remote_port,
        )
        .await
        .map_err(|err| {
            Error::new(
                ErrorKind::ConnectionRefused,
                format!(
                    "SOCKS5 outbound via socks5://{}:{} failed: {}",
                    host, port, err
                ),
            )
        }),
    }
}

async fn open_uot_socket(
    outbound_target: &OutboundTarget,
    request: &TunnelRequest,
) -> Result<Socket> {
    match outbound_target {
        OutboundTarget::Socks5 {
            host,
            port,
            username,
            password,
        } => crate::uot::connect(
            host,
            *port,
            username.as_deref(),
            password.as_deref(),
            &request.remote_addr,
            request.remote_port,
        )
        .await
        .map_err(|err| {
            Error::new(
                ErrorKind::ConnectionRefused,
                format!(
                    "SOCKS5 UoT outbound via socks5://{}:{} failed: {}",
                    host, port, err
                ),
            )
        }),
        OutboundTarget::Direct { .. } => Err(Error::new(
            ErrorKind::InvalidInput,
            "direct UDP outbound is not supported",
        )),
    }
}

async fn open_uot_destination(
    outbound_target: &OutboundTarget,
    destination: &crate::xudp::Destination,
) -> Result<Socket> {
    match outbound_target {
        OutboundTarget::Socks5 {
            host,
            port,
            username,
            password,
        } => crate::uot::connect(
            host,
            *port,
            username.as_deref(),
            password.as_deref(),
            &destination.host,
            destination.port,
        )
        .await
        .map_err(|err| {
            Error::new(
                ErrorKind::ConnectionRefused,
                format!(
                    "SOCKS5 XUDP outbound via socks5://{}:{} failed: {}",
                    host, port, err
                ),
            )
        }),
        OutboundTarget::Direct { .. } => Err(Error::new(
            ErrorKind::InvalidInput,
            "direct XUDP outbound is not supported",
        )),
    }
}

async fn connect_direct(host: &str, port: u16) -> Result<Socket> {
    let socket = Socket::builder().connect(host, port).map_err(|e| {
        Error::new(
            ErrorKind::ConnectionRefused,
            format!("connect to remote failed: {}", e),
        )
    })?;

    wait_socket_opened(socket, DIRECT_CONNECT_TIMEOUT, "remote socket not opened").await
}

async fn wait_socket_opened(
    socket: Socket,
    timeout: Duration,
    error_message: &'static str,
) -> Result<Socket> {
    tokio::select! {
        result = socket.opened() => {
            result.map_err(|e| {
                Error::new(ErrorKind::ConnectionRefused, format!("{}: {}", error_message, e))
            })?;
            Ok(socket)
        }
        _ = Delay::from(timeout) => {
            Err(Error::new(ErrorKind::ConnectionRefused, "connect to remote timed out"))
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UdpRoute {
    DnsOverHttps,
    Uot,
    Unsupported,
}

fn udp_route(request: &TunnelRequest, proxy_ip: &[OutboundTarget]) -> UdpRoute {
    if request.remote_port == 53 {
        UdpRoute::DnsOverHttps
    } else if proxy_ip.iter().any(OutboundTarget::is_socks5) {
        UdpRoute::Uot
    } else {
        UdpRoute::Unsupported
    }
}

#[cfg(test)]
mod tests {
    use super::{udp_route, UdpRoute};
    use crate::config::parse_outbound_targets;
    use crate::protocol::TunnelRequest;

    #[test]
    fn udp_dns_uses_dns_over_https() {
        let request = TunnelRequest {
            network_type: 2,
            remote_port: 53,
            remote_addr: "example.com".to_string(),
        };

        assert_eq!(udp_route(&request, &[]), UdpRoute::DnsOverHttps);
    }

    #[test]
    fn non_dns_udp_uses_socks5_uot_when_available() {
        let request = TunnelRequest {
            network_type: 2,
            remote_port: 443,
            remote_addr: "example.com".to_string(),
        };
        let targets = parse_outbound_targets("1.2.3.4 socks5://proxy.example.com:1080").unwrap();

        assert_eq!(udp_route(&request, &targets), UdpRoute::Uot);
    }

    #[test]
    fn non_dns_udp_without_socks5_is_unsupported() {
        let request = TunnelRequest {
            network_type: 2,
            remote_port: 443,
            remote_addr: "example.com".to_string(),
        };
        let targets = parse_outbound_targets("1.2.3.4 1.2.3.4:8443").unwrap();

        assert_eq!(udp_route(&request, &targets), UdpRoute::Unsupported);
    }
}
