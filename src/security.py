"""Encryption methods used by the Part 2 pipeline."""

from __future__ import annotations

import base64
import os
from typing import Literal, TypeAlias

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from dotenv import load_dotenv

EncryptionMethod: TypeAlias = Literal["aes-gcm", "aes-cbc", "fernet"]
GCM_NONCE_SIZE = 12
CBC_IV_SIZE = 16
AES_BLOCK_SIZE = 128


def get_encryption_key() -> bytes:
    """Load and validate the Base64-encoded AES key from .env."""

    load_dotenv()
    encoded_key = os.getenv("ENCRYPTION_KEY")
    if not encoded_key:
        raise RuntimeError("ENCRYPTION_KEY is missing from .env")

    try:
        key = base64.urlsafe_b64decode(encoded_key.encode())
    except Exception as error:
        raise RuntimeError("ENCRYPTION_KEY is not valid Base64") from error

    if len(key) not in {16, 24, 32}:
        raise RuntimeError("ENCRYPTION_KEY must decode to a valid AES key length")
    return key


def encrypt_aes_gcm(plaintext: bytes, key: bytes) -> bytes:
    """Encrypt bytes with AES-GCM and return nonce + ciphertext + tag."""

    nonce = os.urandom(GCM_NONCE_SIZE)
    return nonce + AESGCM(key).encrypt(nonce, plaintext, None)


def decrypt_aes_gcm(payload: bytes, key: bytes) -> bytes:
    """Decrypt a nonce + ciphertext + tag AES-GCM payload."""

    if len(payload) <= GCM_NONCE_SIZE:
        raise ValueError("AES-GCM payload is too short")
    nonce = payload[:GCM_NONCE_SIZE]
    return AESGCM(key).decrypt(nonce, payload[GCM_NONCE_SIZE:], None)


def encrypt_aes_cbc(plaintext: bytes, key: bytes) -> bytes:
    """Encrypt bytes with AES-CBC and PKCS7 padding."""

    iv = os.urandom(CBC_IV_SIZE)
    padder = padding.PKCS7(AES_BLOCK_SIZE).padder()
    padded = padder.update(plaintext) + padder.finalize()
    encryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
    return iv + encryptor.update(padded) + encryptor.finalize()


def decrypt_aes_cbc(payload: bytes, key: bytes) -> bytes:
    """Decrypt an IV + ciphertext AES-CBC payload."""

    if len(payload) <= CBC_IV_SIZE:
        raise ValueError("AES-CBC payload is too short")
    iv = payload[:CBC_IV_SIZE]
    decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
    padded = decryptor.update(payload[CBC_IV_SIZE:]) + decryptor.finalize()
    unpadder = padding.PKCS7(AES_BLOCK_SIZE).unpadder()
    return unpadder.update(padded) + unpadder.finalize()


def encrypt_fernet(plaintext: bytes, key: bytes) -> bytes:
    """Encrypt bytes with Fernet, which uses AES-CBC and HMAC."""

    fernet_key = base64.urlsafe_b64encode(key)
    return Fernet(fernet_key).encrypt(plaintext)


def decrypt_fernet(payload: bytes, key: bytes) -> bytes:
    """Decrypt and authenticate a Fernet payload."""

    fernet_key = base64.urlsafe_b64encode(key)
    return Fernet(fernet_key).decrypt(payload)


def encrypt(
    plaintext: bytes,
    method: EncryptionMethod,
    key: bytes,
) -> bytes:
    """Encrypt bytes with the selected method."""

    methods = {
        "aes-gcm": encrypt_aes_gcm,
        "aes-cbc": encrypt_aes_cbc,
        "fernet": encrypt_fernet,
    }
    return methods[method](plaintext, key)


def decrypt(
    payload: bytes,
    method: EncryptionMethod,
    key: bytes,
) -> bytes:
    """Decrypt bytes with the selected method."""

    methods = {
        "aes-gcm": decrypt_aes_gcm,
        "aes-cbc": decrypt_aes_cbc,
        "fernet": decrypt_fernet,
    }
    return methods[method](payload, key)
