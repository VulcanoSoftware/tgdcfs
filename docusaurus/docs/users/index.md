---
sidebar_position: 1
title: Introduction for Users
description: A simple, friendly guide to understanding TGDCFS.
---

# 👤 Introduction for Users

Welcome to **TGDCFS**! This guide is designed for anyone who wants to use TGDCFS to store, access, and stream files without needing to understand complicated technical jargon.

---

## What is TGDCFS?

Imagine you have a private cloud storage service—similar to Google Drive or Dropbox—but instead of paying monthly subscription fees for storage, your files are stored safely inside private channels on **Telegram** or **Discord**.

**TGDCFS** (Telegram & Discord Cloud File System) acts as a smart bridge. It connects your computer, phone, or tablet directly to these cloud channels using standard protocols like **WebDAV** and **SFTP**.

```
+------------------+         +-------------------+         +-----------------------+
|  Your Computer   | WebDAV  |      TGDCFS       | Bot API |   Telegram Channel    |
|   / Phone App    | ------->|   Bridge Server   | ------->|           or          |
| (Cyberduck/etc.) |  SFTP   |                   |         |    Discord Channel    |
+------------------+         +-------------------+         +-----------------------+
```

---

## Why Use TGDCFS?

- 📁 **Unlimited Storage Capability**: Store files of any size without worrying about strict storage caps.
- 📱 **Access Anywhere**: Open your files from your computer, iPhone, iPad, or Android device using popular file apps.
- 🎬 **Video & Media Streaming**: Stream high-definition videos directly without downloading the whole file first.
- 🔒 **Privacy & Safety**: Your files can be automatically encrypted so nobody else—not even Telegram or Discord—can read them.
- 🔄 **Automatic Backups & Redundancy**: TGDCFS can mirror your files across multiple channels so you never lose data even if one channel is unavailable.

---

## Key Concepts Made Simple

Here are a few basic terms you might encounter:

- **Store**: A specific private Telegram channel or Discord channel where file chunks are stored.
- **File System**: A folder you see on your computer (like `Media` or `Documents`) that links to one primary store and optional mirror stores.
- **WebDAV**: A standard protocol that lets your computer or phone view TGDCFS like a network drive or folder.
- **SFTP**: A safe connection protocol (Secure File Transfer Protocol) used by file transfer programs like WinSCP or Cyberduck.
- **Mini App**: A built-in web interface inside Telegram that lets you browse, manage, and inspect your files visually.

---

## What Can You Do Next?

- 🌐 [Connect Your Apps](./connecting-clients.md) – Step-by-step instructions for WinSCP, Cyberduck, rclone, and mobile apps.
- 📱 [Use the Telegram Mini App](./mini-app.md) – Learn how to manage files directly inside Telegram.
- 📂 [Understand Files & Versioning](./versioning-and-files.md) – See how folders, infinite file versioning, and video streaming work.
- ❓ [Frequently Asked Questions](./faq.md) – Quick answers to common questions.
