use std::io;

use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{StreamProtocol, request_response};
use prost::Message;
use pulsar_verifier_proto::v1::{GetProofRequest, GetProofResponse};

const MAX_REQUEST_BYTES: usize = 1024;

/// Length-bounded protobuf codec for direct proof exchange substreams.
#[derive(Clone)]
pub(crate) struct ProofExchangeCodec {
    max_response_bytes: usize,
}

impl ProofExchangeCodec {
    pub(crate) const fn new(max_proof_bytes: usize) -> Self {
        Self {
            max_response_bytes: max_proof_bytes + MAX_REQUEST_BYTES,
        }
    }
}

#[async_trait]
impl request_response::Codec for ProofExchangeCodec {
    type Protocol = StreamProtocol;
    type Request = GetProofRequest;
    type Response = GetProofResponse;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_message(io, MAX_REQUEST_BYTES).await
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_message(io, self.max_response_bytes).await
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        request: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_message(io, &request, MAX_REQUEST_BYTES).await
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        response: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_message(io, &response, self.max_response_bytes).await
    }
}

async fn read_message<T, M>(io: &mut T, maximum: usize) -> io::Result<M>
where
    T: AsyncRead + Unpin,
    M: Message + Default,
{
    let mut bytes = Vec::new();
    io.take((maximum + 1) as u64)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("protobuf frame exceeds {maximum} bytes"),
        ));
    }
    M::decode(bytes.as_slice()).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

async fn write_message<T, M>(io: &mut T, message: &M, maximum: usize) -> io::Result<()>
where
    T: AsyncWrite + Unpin,
    M: Message,
{
    if message.encoded_len() > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("protobuf frame exceeds {maximum} bytes"),
        ));
    }
    let bytes = message.encode_to_vec();
    io.write_all(&bytes).await?;
    io.close().await
}

#[cfg(test)]
mod tests {
    use futures::io::Cursor;
    use libp2p::request_response::Codec as _;

    use super::*;

    #[tokio::test]
    async fn rejects_oversized_response_before_decode() {
        let mut codec = ProofExchangeCodec::new(8);
        let mut input = Cursor::new(vec![0_u8; codec.max_response_bytes + 1]);

        let result = codec
            .read_response(&StreamProtocol::new("/test/1"), &mut input)
            .await;

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }
}
