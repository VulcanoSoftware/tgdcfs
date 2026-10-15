---
sidebar_position: 3
title: Telegram Mini App & Explorer
description: Learn how to manage files using the Telegram Mini App interface.
---

# 📱 Telegram Mini App & Web Explorer

TGDCFS includes a built-in **Telegram Mini App** interface. This web interface allows you to view, explore, and manage your stored files directly from inside Telegram or via a browser.

---

## Accessing the Mini App

1. Open your custom Telegram Bot in your Telegram app.
2. Tap the **Menu Button** or the **Open App** button provided by the bot.
3. The Mini App will load directly inside Telegram!

---

## Features of the Mini App

### 📂 File Explorer
- **Browse Directories**: Navigate through folders in your configured file systems.
- **File Details**: View file size, last modified date, and current file version.
- **Media Preview & Streaming**: Preview images or stream videos directly without leaving Telegram.

### 🔄 Mirrors & Replication Status
Tap on the **Mirrors and replication** icon in the header bar to inspect the background health of your files:
- **Queue Depth**: See how many files are currently waiting to be mirrored to secondary Telegram or Discord channels.
- **Retry Failed Items**: If a temporary network issue caused a background replication to fail, tap **Retry failed** to immediately attempt copy operations again.
- **Backfill**: Trigger a full scan to ensure every single file has complete copies across all your configured channels.
- **Local Cache Management**: View current local cache usage and tap **Drop unpinned entries** if you need to free up disk space on your server.

### ⚙️ Background Tasks
- Track ongoing uploads, video processing, or backfill tasks in real-time.
- View clear progress percentages and status messages.

---

## Importing Pre-existing Files on Telegram

If you already have files uploaded to a Telegram channel before setting up TGDCFS, you can import them into your file system using the Mini App:

1. Copy the Telegram message link or ID of the file in your channel.
2. Open the Mini App and go to the target directory.
3. Use the **Import** feature to add the file directly into your TGDCFS virtual file system without downloading and re-uploading the data!
