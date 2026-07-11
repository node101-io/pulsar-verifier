/// Versioned verifier-to-verifier wire contracts.
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/pulsar.verifier.v1.rs"));
}

#[cfg(test)]
mod tests {
    use prost::Message as _;

    use super::v1::{
        AvailabilityAnnouncement, AvailabilityMessage, GetProofResponse, ProofContent,
        availability_message, get_proof_response,
    };

    #[test]
    fn availability_round_trip_preserves_payload() {
        let message = AvailabilityMessage {
            chain_id: "pulsar-test".to_owned(),
            payload: Some(availability_message::Payload::Announcement(
                AvailabilityAnnouncement {
                    proof_hash: vec![1; 32],
                },
            )),
        };

        assert_eq!(
            AvailabilityMessage::decode(message.encode_to_vec().as_slice()).unwrap(),
            message
        );
    }

    #[test]
    fn proof_response_round_trip_preserves_opaque_bytes() {
        let response = GetProofResponse {
            chain_id: "pulsar-test".to_owned(),
            result: Some(get_proof_response::Result::Content(ProofContent {
                proof_hash: vec![2; 32],
                proof: vec![3; 128],
            })),
        };

        assert_eq!(
            GetProofResponse::decode(response.encode_to_vec().as_slice()).unwrap(),
            response
        );
    }
}
