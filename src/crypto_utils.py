"""
Cryptographic utilities for PBFT message authentication.

PBFT requires that every protocol message be cryptographically signed so
that a Byzantine node cannot:
  1. Impersonate another node (forge messages).
  2. Equivocate without detection (send conflicting signed messages —
     the signatures become non-repudiable evidence).

We use Ed25519: small keys, fast signing, deterministic signatures.
"""

import json
import os
from typing import Dict, Optional, Tuple

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.exceptions import InvalidSignature


KEY_DIR = "/app/keys"  # Shared volume so every node can read every public key


def ensure_key_dir() -> None:
    os.makedirs(KEY_DIR, exist_ok=True)


def generate_and_store_keys(node_id: str) -> None:
    """
    Generate an Ed25519 keypair for `node_id` and write both keys to the
    shared key directory. Public keys are world-readable so peers can
    verify signatures; private keys stay associated with the node.
    """
    ensure_key_dir()
    private_path = os.path.join(KEY_DIR, f"{node_id}_private.pem")
    public_path = os.path.join(KEY_DIR, f"{node_id}_public.pem")

    # Idempotent: don't regenerate if keys already exist (survives restarts).
    if os.path.exists(private_path) and os.path.exists(public_path):
        return

    private_key = ed25519.Ed25519PrivateKey.generate()
    public_key = private_key.public_key()

    with open(private_path, "wb") as f:
        f.write(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ))

    with open(public_path, "wb") as f:
        f.write(public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        ))


def load_private_key(node_id: str) -> ed25519.Ed25519PrivateKey:
    path = os.path.join(KEY_DIR, f"{node_id}_private.pem")
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)


def load_public_key(node_id: str) -> Optional[ed25519.Ed25519PublicKey]:
    """Load a peer's public key. Returns None if not yet available."""
    path = os.path.join(KEY_DIR, f"{node_id}_public.pem")
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return serialization.load_pem_public_key(f.read())


def canonical(message: dict) -> bytes:
    """
    Canonical JSON serialisation. Sorted keys + no whitespace ensures
    that the same logical message always produces identical bytes,
    which is essential for signature verification.
    """
    return json.dumps(message, sort_keys=True, separators=(",", ":")).encode()


def sign_message(private_key: ed25519.Ed25519PrivateKey, message: dict) -> str:
    """Return a hex-encoded signature over the canonical message bytes."""
    return private_key.sign(canonical(message)).hex()


def verify_signature(public_key: ed25519.Ed25519PublicKey,
                     message: dict, signature_hex: str) -> bool:
    """Return True iff `signature_hex` is a valid signature for `message`."""
    if public_key is None:
        return False
    try:
        public_key.verify(bytes.fromhex(signature_hex), canonical(message))
        return True
    except (InvalidSignature, ValueError):
        return False


class KeyRing:
    """
    Per-node cache of peer public keys. Lazily loads keys from the shared
    volume the first time a signature from a given peer is verified.
    """

    def __init__(self, my_node_id: str):
        self.my_node_id = my_node_id
        self.private_key = load_private_key(my_node_id)
        self._peer_keys: Dict[str, ed25519.Ed25519PublicKey] = {}

    def sign(self, message: dict) -> str:
        return sign_message(self.private_key, message)

    def verify(self, sender_id: str, message: dict, signature_hex: str) -> bool:
        pk = self._peer_keys.get(sender_id)
        if pk is None:
            pk = load_public_key(sender_id)
            if pk is None:
                return False
            self._peer_keys[sender_id] = pk
        return verify_signature(pk, message, signature_hex)
