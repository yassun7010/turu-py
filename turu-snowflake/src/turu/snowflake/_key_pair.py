import base64
from typing import TYPE_CHECKING, Optional
from cryptography.hazmat.primitives.serialization import (
    load_der_private_key,
    load_pem_private_key,
    Encoding,
    PrivateFormat,
    NoEncryption,
)

if TYPE_CHECKING:
    from asn1crypto.keys import RSAPrivateKey


def load_private_key(
    data: bytes, private_key_passphrase: Optional[bytes] = None
) -> "RSAPrivateKey":
    if data.startswith(b"-----BEGIN "):
        return _load_private_key_from_pem(data, private_key_passphrase)
    else:
        return _load_private_key_from_der(data, private_key_passphrase)


def _load_private_key_from_pem(
    data: bytes, private_key_passphrase: Optional[bytes] = None
) -> "RSAPrivateKey":
    """
    Load a private key from a PEM encoded byte string.
    """
    p_key = load_pem_private_key(data, password=private_key_passphrase)
    private_key = p_key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )
    return load_der_private_key(
        data=private_key,
        password=None,
    )  # type: ignore[assignment]


def _load_private_key_from_der(
    data: bytes, private_key_passphrase: Optional[bytes] = None
) -> "RSAPrivateKey":
    """
    Load a private key from a DER encoded byte string.
    """
    try:
        data = base64.b64decode(data)
    except Exception:
        pass

    return load_der_private_key(
        data=data,
        password=private_key_passphrase,
    )  # type: ignore[assignment]
