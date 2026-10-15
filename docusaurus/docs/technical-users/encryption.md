---
sidebar_position: 4
title: Security & At-Rest Encryption
description: Detailed technical breakdown of client-side AES-256-GCM encryption.
---

# 🔒 Security & At-Rest Encryption

When `tgdcfs.encryption.enabled: true` is set, TGDCFS performs **client-side at-rest encryption** before transmitting any data over the network. Telegram channels, Discord channels, and GitHub metadata repositories store **only ciphertext**.

---

## Cryptographic Architecture

```
                       [ Master Passphrase ] + [ Master Salt (master.salt) ]
                                                |
                                                v  Argon2id
                                        [ Master Key ]
                                                |
                                                v  HKDF-SHA256 (per-file random salt)
                                        [ Per-File Key ]
                                                |
+-------------------+                           v
|   Plaintext Data  | ------> [ AES-256-GCM in 64 KiB Chunks ] ------> [ Ciphertext Message ]
+-------------------+            (Self-Describing 60-byte Header)     + [ Telegram / Discord ]
```

### 1. Key Derivation (Argon2id + HKDF)
- **Master Key**: Derived from your secret passphrase using **Argon2id** and a 16-byte random salt stored in `$TGDCFS_DATA_DIR/master.salt`.
- **Per-File Key**: Generated for each uploaded file using **HKDF-SHA256** from the Master Key and a 32-byte cryptographically secure random salt generated per file.

### 2. Cipher & Chunking
- **Cipher**: **AES-256-GCM** operating on **64 KiB chunks** (`chunk_size: 65536`).
- **Nonces & Authentication**: Each 64 KiB chunk has its own 12-byte initialization vector (nonce) and 16-byte authentication tag (GCM tag).
- **Random Access Decryption**: Because each 64 KiB chunk is encrypted independently, HTTP Range requests and video streaming decrypt only the requested byte ranges without needing to decrypt the entire file!

### 3. Inline Header Format
Each encrypted file begins with an inline **60-byte header** embedded inside the first message chunk:
- 4 bytes: Magic bytes (`TGCR`)
- 2 bytes: Version marker
- 32 bytes: Per-file HKDF salt
- 12 bytes: Header HMAC / Nonce
- 10 bytes: Metadata flags

> **Self-Healing Property**: Even if the metadata store (GitHub repo or pinned message) is completely lost, a file can be decrypted directly from the channel messages alone using your passphrase and `master.salt`!

---

## Filename & Metadata Obfuscation (`encrypt_names: true`)

When `encrypt_names: true` is configured:

1. **Telegram / Discord Messages**: Document attachments are renamed to random AES-GCM ciphertext tokens (e.g., `enc_a8f9c1...bin`).
2. **GitHub Metadata Repository**: Directory structures and filenames inside the GitHub repo are encrypted into tokenized paths (`a8f9c1/3d2b...json`).
3. **Commit Messages**: GitHub commit messages are obfuscated so repository observers cannot deduce directory activities.

Inside WebDAV, SFTP, the Manager API, and the Telegram Mini App, file and directory names are decrypted on the fly and displayed normally to authenticated users.

---

## Master Salt Management (`master.salt`)

The master salt file (`master.salt`) is created automatically on first boot in `$TGDCFS_DATA_DIR/master.salt` with mode `0600`.

- **Is it secret?** No, the salt is non-secret, but it is **strictly required** to re-derive the Master Key from your passphrase.
- **Backup Rule**: You **must** back up `master.salt` alongside your passphrase.
- **Rotation Warning**: Replacing or losing `master.salt` makes all previously uploaded encrypted files permanently unreadable!

---

## Setting Up Passphrase Input Methods

Passphrase resolution checks sources in this priority order:

1. `passphrase_env`: Environment variable name (e.g. `TGDCFS_MASTER_PASSPHRASE`). Recommended for Docker.
2. `passphrase_file`: File path containing passphrase. Recommended for systemd `LoadCredential`.
3. `passphrase`: Plaintext string in `config.yaml` (Development only).

Generating a strong 32-byte passphrase in Python:
```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```
