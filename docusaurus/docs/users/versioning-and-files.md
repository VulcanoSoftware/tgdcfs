---
sidebar_position: 4
title: Files, Folders & Versioning
description: How TGDCFS handles unlimited file sizes, video streaming, and version history.
---

# 📂 Files, Folders, and Versioning

TGDCFS makes storing files in Telegram and Discord feel like using a normal hard drive or cloud folder. Here is how key file management features work behind the scenes in simple terms.

---

## 📦 Large File Support (Chunking)

Chat apps like Telegram and Discord limit the maximum size of a single uploaded file (for instance, 2 GiB on Telegram or 20 MB on Discord).

**How TGDCFS solves this for you:**
- When you upload a large file (e.g., a 10 GB movie), TGDCFS automatically splits the file into smaller **chunks** or parts.
- Each chunk is stored in your private channel.
- When you open or download the file, TGDCFS stitches the chunks back together instantly in real-time.
- To you and your WebDAV/SFTP apps, it appears as **one single seamless file**.

---

## 🎬 Video & Media Streaming

You do not need to wait for a 5 GB video file to download completely before watching it:

- TGDCFS supports **HTTP Range Requests**.
- When you press Play in players like VidHub, VLC, or Cyberduck, the player requests only the specific seconds or megabytes needed.
- TGDCFS fetches just those chunks from Telegram or Discord, enabling instant playback and video seeking.

---

## 🕒 Infinite File Versioning

Whenever you overwrite or save changes to an existing file (for example, editing a document or updating a picture):

- TGDCFS **does not overwrite or destroy** the old file on Telegram/Discord.
- Instead, it saves a new **version** of the file.
- The previous versions remain safely preserved in history.
- If you accidentally corrupt a file, earlier file versions can still be referenced and recovered.

---

## 📁 Folders & Directory Structure

- In TGDCFS, top-level folders correspond to your configured **File Systems** (e.g., `media`, `documents`, `backups`).
- Below the top level, you can create subfolders just like on a standard hard drive.
- Folders are kept synchronized with the metadata store.
