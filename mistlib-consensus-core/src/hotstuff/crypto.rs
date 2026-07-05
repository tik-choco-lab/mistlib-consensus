//! Pluggable signing for HotStuff quorum certificates.
//!
//! In untrusted P2P deployments use [`Ed25519Crypto`]; the caller is
//! responsible for distributing validator public keys (out of scope
//! here). [`NoopCrypto`] disables signing for tests and semi-trusted
//! meshes where Byzantine behaviour is not a concern but HotStuff's
//! deterministic leader rotation is still wanted.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use mistlib_core::types::{HostSendSync, NodeId};
use std::collections::HashMap;

/// Signs and verifies the 32-byte digests defined in
/// [`super::message`] (`vote_digest` / `proposal_digest`).
pub trait CryptoProvider: HostSendSync {
    fn sign(&self, digest: &[u8; 32]) -> Vec<u8>;
    fn verify(&self, signer: &NodeId, digest: &[u8; 32], sig: &[u8]) -> bool;
}

/// Accepts everything. Only sound when every peer is trusted to be
/// non-Byzantine (crash faults remain tolerated by the protocol).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopCrypto;

impl CryptoProvider for NoopCrypto {
    fn sign(&self, _digest: &[u8; 32]) -> Vec<u8> {
        Vec::new()
    }

    fn verify(&self, _signer: &NodeId, _digest: &[u8; 32], _sig: &[u8]) -> bool {
        true
    }
}

/// Ed25519 signatures with a static registry of validator public keys.
pub struct Ed25519Crypto {
    signing_key: SigningKey,
    registry: HashMap<NodeId, VerifyingKey>,
}

impl Ed25519Crypto {
    pub fn new(signing_key: SigningKey, registry: HashMap<NodeId, VerifyingKey>) -> Self {
        Self { signing_key, registry }
    }

    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    /// Registers (or replaces) a validator's public key.
    pub fn register(&mut self, node: NodeId, key: VerifyingKey) {
        self.registry.insert(node, key);
    }
}

impl CryptoProvider for Ed25519Crypto {
    fn sign(&self, digest: &[u8; 32]) -> Vec<u8> {
        self.signing_key.sign(digest).to_bytes().to_vec()
    }

    fn verify(&self, signer: &NodeId, digest: &[u8; 32], sig: &[u8]) -> bool {
        let Some(key) = self.registry.get(signer) else {
            return false;
        };
        let Ok(sig) = Signature::from_slice(sig) else {
            return false;
        };
        key.verify(digest, &sig).is_ok()
    }
}
