/// Chain-owned verification contract pinned by this crate.
pub mod pulsarchain {
    pub mod verification {
        pub mod v1 {
            #![allow(clippy::doc_markdown, clippy::must_use_candidate)]

            include!(concat!(env!("OUT_DIR"), "/pulsarchain.verification.v1.rs"));
        }
    }
}

/// Verifier service and verifier-to-verifier wire contracts.
pub mod pulsar {
    pub mod verifier {
        pub mod v1 {
            #![allow(
                clippy::default_trait_access,
                clippy::doc_markdown,
                clippy::missing_errors_doc,
                clippy::must_use_candidate,
                clippy::similar_names,
                clippy::too_many_lines
            )]

            include!(concat!(env!("OUT_DIR"), "/pulsar.verifier.v1.rs"));
        }
    }
}

pub use pulsar::verifier::v1;
pub use pulsarchain::verification::v1 as chain_v1;

#[cfg(test)]
mod tests {
    use prost::Message as _;

    use super::{
        chain_v1::ProofType,
        v1::{
            AvailabilityAnnouncement, AvailabilityMessage, GetProofResponse, Proof,
            availability_message, get_proof_response,
        },
    };

    #[test]
    fn availability_round_trip_preserves_payload() {
        let message = AvailabilityMessage {
            chain_id: "pulsar-test".to_owned(),
            payload: Some(availability_message::Payload::Announcement(
                AvailabilityAnnouncement {
                    verification_id: vec![1; 32],
                },
            )),
        };

        assert_eq!(
            AvailabilityMessage::decode(message.encode_to_vec().as_slice()).unwrap(),
            message
        );
    }

    #[test]
    fn proof_response_round_trip_preserves_complete_proof() {
        let response = GetProofResponse {
            chain_id: "pulsar-test".to_owned(),
            result: Some(get_proof_response::Result::Proof(Proof {
                proof_type: ProofType::MinaPickles.into(),
                proof: vec![3; 128],
                public_inputs: vec![4; 64],
                verification_key: vec![5; 256],
            })),
        };

        assert_eq!(
            GetProofResponse::decode(response.encode_to_vec().as_slice()).unwrap(),
            response
        );
    }

    #[test]
    fn chain_service_contract_is_generated() {
        let request = super::v1::GetProofStatusesRequest {
            verification_ids: vec![vec![7; 32]],
        };

        assert_eq!(request.verification_ids.len(), 1);
    }
}
