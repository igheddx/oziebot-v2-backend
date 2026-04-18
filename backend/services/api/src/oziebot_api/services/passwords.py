from passlib.context import CryptContext

# pbkdf2_sha256: avoids bcrypt backend quirks (e.g. Py 3.13 / bcrypt 4.x); still strong with high rounds.
_pwd = CryptContext(
    schemes=["pbkdf2_sha256"],
    deprecated="auto",
    pbkdf2_sha256__default_rounds=390_000,
)


def hash_password(plain: str) -> str:
    return _pwd.hash(plain)


def verify_password(plain: str, password_hash: str) -> bool:
    return _pwd.verify(plain, password_hash)
