use std::io::Result;
use tokio::io::AsyncReadExt;
pub trait ReadStringExt {
    async fn read_string(&mut self, n: usize) -> Result<String>;
}

impl<T: AsyncReadExt + Unpin + ?Sized> ReadStringExt for T {
    async fn read_string(&mut self, n: usize) -> Result<String> {
        let mut buffer = vec![0u8; n];
        self.read_exact(&mut buffer).await?;
        String::from_utf8(buffer).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid string: {}", e),
            )
        })
    }
}
