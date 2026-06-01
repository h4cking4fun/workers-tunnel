use std::io::{Error, ErrorKind, Result};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OutboundTarget {
    Direct {
        host: String,
        port: Option<u16>,
    },
    Socks5 {
        host: String,
        port: u16,
        username: Option<String>,
        password: Option<String>,
    },
}

impl OutboundTarget {
    pub(crate) fn log_target(&self, default_port: u16) -> String {
        match self {
            Self::Direct { host, port } => format!("{}:{}", host, port.unwrap_or(default_port)),
            Self::Socks5 { host, port, .. } => format!("socks5://{}:{}", host, port),
        }
    }

    pub(crate) fn direct_cache_key(&self, default_port: u16) -> Option<String> {
        match self {
            Self::Direct { host, port } => {
                Some(format!("{}:{}", host, port.unwrap_or(default_port)))
            }
            Self::Socks5 { .. } => None,
        }
    }

    pub(crate) fn is_socks5(&self) -> bool {
        matches!(self, Self::Socks5 { .. })
    }
}

pub(crate) fn parse_early_data(data: Option<String>) -> Result<Option<Vec<u8>>> {
    if let Some(data) = data {
        if !data.is_empty() {
            let mut raw = Vec::with_capacity(data.len());
            raw.extend(data.bytes().filter(|&b| b != b'=').map(|b| match b {
                b'+' => b'-',
                b'/' => b'_',
                _ => b,
            }));
            match URL_SAFE_NO_PAD.decode(&raw) {
                Ok(early_data) => return Ok(Some(early_data)),
                Err(err) => return Err(Error::other(err.to_string())),
            }
        }
    }
    Ok(None)
}

pub(crate) fn parse_user_id(user_id: &str) -> [u8; 16] {
    let mut iter = user_id.as_bytes().iter().filter_map(|b| match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    });

    let mut bytes = [0u8; 16];
    for b in &mut bytes {
        let (Some(h), Some(l)) = (iter.next(), iter.next()) else {
            break;
        };
        *b = (h << 4) | l;
    }
    bytes
}

pub(crate) fn parse_outbound_targets(config: &str) -> Result<Vec<OutboundTarget>> {
    config
        .split_ascii_whitespace()
        .map(parse_outbound_target)
        .collect()
}

fn parse_outbound_target(value: &str) -> Result<OutboundTarget> {
    if let Some(uri) = value.strip_prefix("socks5://") {
        parse_socks5_target(uri)
    } else if value.contains("://") {
        Err(Error::new(
            ErrorKind::InvalidInput,
            format!("unsupported outbound target scheme: {}", value),
        ))
    } else {
        let (host, port) = parse_optional_host_port(value)?;
        Ok(OutboundTarget::Direct {
            host: host.to_string(),
            port,
        })
    }
}

fn parse_socks5_target(uri: &str) -> Result<OutboundTarget> {
    let (credentials, endpoint) = match uri.rsplit_once('@') {
        Some((credentials, endpoint)) => (Some(credentials), endpoint),
        None => (None, uri),
    };
    let (host, port) = parse_required_host_port(endpoint)?;

    let (username, password) = match credentials {
        Some(credentials) => {
            let (username, password) = credentials.split_once(':').ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "SOCKS5 credentials must use username:password",
                )
            })?;
            let username = percent_decode(username)?;
            let password = percent_decode(password)?;
            validate_auth_field("SOCKS5 username", &username)?;
            validate_auth_field("SOCKS5 password", &password)?;
            (Some(username), Some(password))
        }
        None => (None, None),
    };

    Ok(OutboundTarget::Socks5 {
        host: host.to_string(),
        port,
        username,
        password,
    })
}

fn parse_optional_host_port(value: &str) -> Result<(&str, Option<u16>)> {
    if value.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "outbound target cannot be empty",
        ));
    }

    if let Some(rest) = value.strip_prefix('[') {
        let (host, after_host) = rest.split_once(']').ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "missing closing bracket in IPv6 target",
            )
        })?;
        if host.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "target host cannot be empty",
            ));
        }
        return match after_host.strip_prefix(':') {
            Some(port) if !port.is_empty() => Ok((host, Some(parse_port(port)?))),
            Some(_) => Err(Error::new(ErrorKind::InvalidInput, "target port is empty")),
            None if after_host.is_empty() => Ok((host, None)),
            None => Err(Error::new(
                ErrorKind::InvalidInput,
                "unexpected text after bracketed IPv6 target",
            )),
        };
    }

    if value.matches(':').count() == 1 {
        let (host, port) = value.split_once(':').unwrap();
        if host.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "target host cannot be empty",
            ));
        }
        if port.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "target port is empty"));
        }
        Ok((host, Some(parse_port(port)?)))
    } else {
        Ok((value, None))
    }
}

fn parse_required_host_port(value: &str) -> Result<(&str, u16)> {
    let (host, port) = parse_optional_host_port(value)?;
    let Some(port) = port else {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "SOCKS5 proxy port is required",
        ));
    };
    Ok((host, port))
}

fn parse_port(value: &str) -> Result<u16> {
    value.parse::<u16>().map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("invalid target port: {}", value),
        )
    })
}

fn percent_decode(value: &str) -> Result<String> {
    let mut bytes = Vec::with_capacity(value.len());
    let mut iter = value.as_bytes().iter().copied();

    while let Some(byte) = iter.next() {
        if byte == b'%' {
            let Some(high) = iter.next() else {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid percent encoding",
                ));
            };
            let Some(low) = iter.next() else {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "invalid percent encoding",
                ));
            };
            bytes.push((hex_value(high)? << 4) | hex_value(low)?);
        } else {
            bytes.push(byte);
        }
    }

    String::from_utf8(bytes).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            "percent-decoded credentials must be valid UTF-8",
        )
    })
}

fn hex_value(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(Error::new(
            ErrorKind::InvalidInput,
            "invalid percent encoding",
        )),
    }
}

fn validate_auth_field(name: &str, value: &str) -> Result<()> {
    if value.len() > u8::MAX as usize {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("{} cannot exceed 255 bytes", name),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_outbound_target, parse_outbound_targets, OutboundTarget};

    #[test]
    fn parses_direct_targets() {
        assert_eq!(
            parse_outbound_target("example.com").unwrap(),
            OutboundTarget::Direct {
                host: "example.com".to_string(),
                port: None
            }
        );
        assert_eq!(
            parse_outbound_target("example.com:443").unwrap(),
            OutboundTarget::Direct {
                host: "example.com".to_string(),
                port: Some(443)
            }
        );
    }

    #[test]
    fn parses_socks5_targets() {
        assert_eq!(
            parse_outbound_target("socks5://proxy.example.com:1080").unwrap(),
            OutboundTarget::Socks5 {
                host: "proxy.example.com".to_string(),
                port: 1080,
                username: None,
                password: None
            }
        );
        assert_eq!(
            parse_outbound_target("socks5://user:p%40ss@proxy.example.com:1080").unwrap(),
            OutboundTarget::Socks5 {
                host: "proxy.example.com".to_string(),
                port: 1080,
                username: Some("user".to_string()),
                password: Some("p@ss".to_string())
            }
        );
    }

    #[test]
    fn rejects_invalid_targets() {
        assert!(parse_outbound_target("http://proxy.example.com:1080").is_err());
        assert!(parse_outbound_target("socks5://:1080").is_err());
        assert!(parse_outbound_target("socks5://proxy.example.com").is_err());
        assert!(parse_outbound_target("socks5://user@proxy.example.com:1080").is_err());
        assert!(parse_outbound_target("example.com:bad").is_err());
    }

    #[test]
    fn parses_target_lists() {
        assert_eq!(
            parse_outbound_targets("example.com socks5://proxy.example.com:1080")
                .unwrap()
                .len(),
            2
        );
    }
}
