from __future__ import annotations

from cryptography.fernet import Fernet, InvalidToken


class CredentialCrypto:
    def __init__(self, key_b64: str | None) -> None:
        self._fernet = (
            Fernet(key_b64.strip().encode()) if key_b64 and key_b64.strip() else None
        )

    @property
    def configured(self) -> bool:
        return self._fernet is not None

    def decrypt(self, ciphertext: bytes) -> bytes:
        if self._fernet is None:
            raise RuntimeError("EXCHANGE_CREDENTIALS_ENCRYPTION_KEY is not configured")
        try:
            return self._fernet.decrypt(ciphertext)
        except InvalidToken as exc:
            raise ValueError("Credential decryption failed") from exc
