use std::{sync::Arc, time::Duration};

use moka::Expiry;

use crate::proof::VerificationId;

use super::record::{ProofRecord, VerificationState};

/// Starts retention only after verification reaches a terminal state.
#[derive(Clone, Copy)]
pub(crate) struct TerminalExpiry {
    retention: Duration,
}

impl TerminalExpiry {
    pub(crate) const fn new(retention: Duration) -> Self {
        Self { retention }
    }

    fn duration(&self, record: &ProofRecord) -> Option<Duration> {
        matches!(
            record.metadata.verification,
            VerificationState::Verified | VerificationState::Wrong
        )
        .then_some(self.retention)
    }
}

impl Expiry<VerificationId, Arc<ProofRecord>> for TerminalExpiry {
    fn expire_after_create(
        &self,
        _key: &VerificationId,
        value: &Arc<ProofRecord>,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        self.duration(value)
    }

    fn expire_after_read(
        &self,
        _key: &VerificationId,
        _value: &Arc<ProofRecord>,
        _read_at: std::time::Instant,
        duration_until_expiry: Option<Duration>,
        _last_modified_at: std::time::Instant,
    ) -> Option<Duration> {
        duration_until_expiry
    }

    fn expire_after_update(
        &self,
        _key: &VerificationId,
        value: &Arc<ProofRecord>,
        _updated_at: std::time::Instant,
        _duration_until_expiry: Option<Duration>,
    ) -> Option<Duration> {
        self.duration(value)
    }
}
