use futures_core::Stream;
use std::{
    future::Future,
    io::{Error, Result},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::{BufMut, BytesMut};
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use worker::{Delay, EventStream, WebSocket, WebsocketEvent};

const WRITE_BUFFER_HIGH_WATERMARK: u32 = 1024 * 1024;
const FLUSH_BUFFER_LOW_WATERMARK: u32 = 128 * 1024;
const BACKPRESSURE_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[pin_project]
pub struct WebSocketStream<'a> {
    ws: &'a WebSocket,
    #[pin]
    stream: EventStream<'a>,
    #[pin]
    write_delay: Option<Delay>,
    read_buffer: BytesMut,
    closed: bool,
}

impl<'a> WebSocketStream<'a> {
    pub fn new(ws: &'a WebSocket, stream: EventStream<'a>, early_data: Option<Vec<u8>>) -> Self {
        let mut read_buffer = BytesMut::new();
        if let Some(data) = early_data {
            read_buffer.put_slice(&data)
        }

        Self {
            ws,
            stream,
            write_delay: None,
            read_buffer,
            closed: false,
        }
    }

    fn poll_backpressure(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        max_buffered_amount: u32,
    ) -> Poll<Result<()>> {
        let mut this = self.project();

        loop {
            if this.ws.as_ref().buffered_amount() <= max_buffered_amount {
                this.write_delay.set(None);
                return Poll::Ready(Ok(()));
            }

            match this.write_delay.as_mut().as_pin_mut() {
                Some(delay) => match delay.poll(cx) {
                    Poll::Ready(()) => {
                        this.write_delay
                            .set(Some(Delay::from(BACKPRESSURE_POLL_INTERVAL)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                None => {
                    this.write_delay
                        .set(Some(Delay::from(BACKPRESSURE_POLL_INTERVAL)));
                }
            }
        }
    }
}

impl AsyncRead for WebSocketStream<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        let mut this = self.project();

        // If we already saw Close/None, return EOF immediately
        if *this.closed {
            return Poll::Ready(Ok(()));
        }

        // If buffer is empty, we must get at least one message (blocking)
        if this.read_buffer.is_empty() {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(WebsocketEvent::Message(msg)))) => {
                    if let Some(data) = msg.bytes() {
                        this.read_buffer.put_slice(&data);
                    }
                }
                Poll::Ready(Some(Ok(WebsocketEvent::Close(_)))) | Poll::Ready(None) => {
                    *this.closed = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Err(e))) => {
                    *this.closed = true;
                    return Poll::Ready(Err(Error::other(e.to_string())));
                }
            }
        }

        // Drain additional ready messages without blocking,
        // but stop on Close/Error to avoid consuming them
        while this.read_buffer.len() < buf.remaining() {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(WebsocketEvent::Message(msg)))) => {
                    if let Some(data) = msg.bytes() {
                        this.read_buffer.put_slice(&data);
                    }
                }
                Poll::Ready(Some(Ok(WebsocketEvent::Close(_)))) | Poll::Ready(None) => {
                    *this.closed = true;
                    break;
                }
                Poll::Ready(Some(Err(e))) => {
                    // If we already have data buffered, deliver it first;
                    // the error will surface on the next poll_read
                    if !this.read_buffer.is_empty() {
                        *this.closed = true;
                        break;
                    }
                    *this.closed = true;
                    return Poll::Ready(Err(Error::other(e.to_string())));
                }
                Poll::Pending => break,
            }
        }

        let amt = std::cmp::min(this.read_buffer.len(), buf.remaining());
        if amt > 0 {
            buf.put_slice(&this.read_buffer.split_to(amt));
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for WebSocketStream<'_> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        match self
            .as_mut()
            .poll_backpressure(cx, WRITE_BUFFER_HIGH_WATERMARK)
        {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        }

        if let Err(e) = self.ws.send_with_bytes(buf) {
            return Poll::Ready(Err(Error::other(e.to_string())));
        }

        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        self.as_mut()
            .poll_backpressure(cx, FLUSH_BUFFER_LOW_WATERMARK)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self
            .as_mut()
            .poll_backpressure(cx, FLUSH_BUFFER_LOW_WATERMARK)
        {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Pending => return Poll::Pending,
        }

        if let Err(e) = self.ws.close(Some(1000), Some("normal close")) {
            return Poll::Ready(Err(Error::other(e.to_string())));
        }

        Poll::Ready(Ok(()))
    }
}
