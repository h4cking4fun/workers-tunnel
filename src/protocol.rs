use std::io::{Error, ErrorKind, Result};
use std::net::{Ipv4Addr, Ipv6Addr};

use crate::ext::ReadStringExt;
use crate::websocket::WebSocketStream;
use tokio::io::AsyncReadExt;

pub(crate) const VERSION: u8 = 0;
pub(crate) const RESPONSE: [u8; 2] = [0u8; 2];
pub(crate) const NETWORK_TYPE_TCP: u8 = 1;
pub(crate) const NETWORK_TYPE_UDP: u8 = 2;
pub(crate) const NETWORK_TYPE_MUX: u8 = 3;
const ADDRESS_TYPE_IPV4: u8 = 1;
const ADDRESS_TYPE_DOMAIN: u8 = 2;
const ADDRESS_TYPE_IPV6: u8 = 3;

pub(crate) struct TunnelRequest {
    pub(crate) network_type: u8,
    pub(crate) remote_port: u16,
    pub(crate) remote_addr: String,
}

pub(crate) async fn read_tunnel_request(
    client_socket: &mut WebSocketStream<'_>,
    user_id: &[u8; 16],
) -> Result<TunnelRequest> {
    if client_socket.read_u8().await? != VERSION {
        return Err(Error::new(ErrorKind::InvalidData, "invalid version"));
    }

    let mut id_buf = [0u8; 16];
    client_socket.read_exact(&mut id_buf).await?;
    if id_buf != *user_id {
        return Err(Error::new(ErrorKind::InvalidData, "invalid user id"));
    }

    let addon_len = client_socket.read_u8().await? as usize;
    if addon_len > 0 {
        let mut addon_buf = [0u8; 255];
        client_socket
            .read_exact(&mut addon_buf[..addon_len])
            .await?;
    }

    let network_type = client_socket.read_u8().await?;
    if network_type == NETWORK_TYPE_MUX {
        return Ok(TunnelRequest {
            network_type,
            remote_port: 0,
            remote_addr: String::new(),
        });
    }

    let remote_port = client_socket.read_u16().await?;
    let remote_addr = match client_socket.read_u8().await? {
        ADDRESS_TYPE_DOMAIN => {
            let length = client_socket.read_u8().await?;
            client_socket.read_string(length as usize).await?
        }
        ADDRESS_TYPE_IPV4 => Ipv4Addr::from_bits(client_socket.read_u32().await?).to_string(),
        ADDRESS_TYPE_IPV6 => format!(
            "[{}]",
            Ipv6Addr::from_bits(client_socket.read_u128().await?)
        ),
        _ => return Err(Error::new(ErrorKind::InvalidData, "invalid address type")),
    };

    Ok(TunnelRequest {
        network_type,
        remote_port,
        remote_addr,
    })
}
