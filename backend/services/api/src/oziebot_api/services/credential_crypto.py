"""Fernet encryption for exchange API secrets at rest."""

from __future__ import annotations

from cryptography.fernet import Fernet, InvalidToken


class CredentialCrypto:
    """Encrypt/decrypt credential blobs using a single Fernet key (rotate by re-encrypting rows)."""

    def __init__(self, key_b64: str | None) -> None:
        self._fernet: Fernet | None
        if key_b64 and key_b64.strip():
            self._fernet = Fernet(key_b64.strip().encode())
        else:
            self._fernet = None

    @property
    def configured(self) -> bool:
        return self._fernet is not None

    def encrypt(self, plaintext: bytes) -> bytes:
        if not self._fernet:
            raise RuntimeError("EXCHANGE_CREDENTIALS_ENCRYPTION_KEY is not configured")
        return self._fernet.encrypt(plaintext)

    def decrypt(self, ciphertext: bytes) -> bytes:
        if not self._fernet:
            raise RuntimeError("EXCHANGE_CREDENTIALS_ENCRYPTION_KEY is not configured")
        try:
            return self._fernet.decrypt(ciphertext)
        except InvalidToken as e:
            raise ValueError("Credential decryption failed (wrong key or corrupt data)") from e
