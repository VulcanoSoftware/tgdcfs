---
sidebar_position: 2
title: Connecting Your Apps
description: Step-by-step instructions for connecting WebDAV and SFTP apps to TGDCFS.
---

# 🔌 Connecting Your Apps & Devices

TGDCFS works with almost any file client or media player that supports **WebDAV** or **SFTP**. This page provides step-by-step instructions for popular software across Windows, macOS, iOS, and Android.

---

## Supported & Tested Apps

The following clients are officially tested and verified:

- 🦆 **Cyberduck** (Windows / macOS) – WebDAV & SFTP
- 🦝 **WinSCP** (Windows) – SFTP & WebDAV
- 🔄 **rclone** (Windows / macOS / Linux) – Command-line WebDAV & SFTP
- 📄 **Documents by Readdle** (iOS / iPadOS) – WebDAV
- 🎥 **VidHub** (iOS / iPadOS / macOS) – WebDAV & SFTP media player

---

## 1. Connecting via WebDAV

WebDAV allows your computer or phone to mount TGDCFS like a cloud folder or external drive.

### Connection Details Needed
- **Server URL**: `http://your-server-ip:1900/webdav` (or HTTPS if configured)
- **Username**: Your configured username (or leave blank if anonymous access is enabled)
- **Password**: Your configured password

---

### Cyberduck (Windows & Mac)

1. Open **Cyberduck** and click **Open Connection**.
2. Select **WebDAV (HTTP)** or **WebDAV (HTTPS)** from the dropdown list.
3. In **Server**, enter your server domain or IP address (e.g., `192.168.1.50`).
4. In **Port**, enter `1900` (or your custom server port).
5. In **URL / Path**, append `/webdav`.
6. Enter your **Username** and **Password**, then click **Connect**.

---

### Documents by Readdle (iPhone / iPad)

1. Open **Documents** on your iOS device.
2. Tap the **+** (Add) button or go to the **Connections** tab.
3. Tap **Add Connection** and select **WebDAV Server**.
4. Set the **URL** to `http://your-server-ip:1900/webdav`.
5. Enter your **Login** and **Password**.
6. Tap **Done**. You can now browse files and stream videos directly!

---

### VidHub (macOS / iOS)

1. Open **VidHub**.
2. Go to **File Sources** and select **Add File Source**.
3. Choose **WebDAV**.
4. Enter the address `http://your-server-ip:1900/webdav`, username, and password.
5. Tap **Test Connection** and **Save**. Your media files will automatically index for video playback.

---

## 2. Connecting via SFTP

TGDCFS provides a full-featured SFTP server on port `2222` (or your configured port).

### Connection Details Needed
- **Protocol**: SFTP (SSH File Transfer Protocol)
- **Host**: `your-server-ip`
- **Port**: `2222`
- **Username**: Your configured username
- **Password**: Your configured password (or SSH Key)

---

### WinSCP (Windows)

1. Open **WinSCP**.
2. Set **File protocol** to **SFTP**.
3. Enter **Host name** (e.g., `192.168.1.50`) and **Port number** (`2222`).
4. Enter your **User name** and **Password**.
5. Click **Login**.

> **Note**: WinSCP will ask you to trust the SSH Host Key the first time you connect. Accept it to proceed.

---

### rclone (Command Line)

You can configure rclone interactively or add it directly to `~/.config/rclone/rclone.conf`:

```ini
[tgdcfs-webdav]
type = webdav
url = http://your-server-ip:1900/webdav
vendor = other
user = your_username
pass = your_encrypted_password

[tgdcfs-sftp]
type = sftp
host = your-server-ip
port = 2222
user = your_username
pass = your_encrypted_password
```

Test your connection with:
```bash
rclone ls tgdcfs-webdav:
rclone ls tgdcfs-sftp:
```

---

## Common Connection Troubleshooting

- **Connection Refused**: Check if your server firewall allows traffic on port `1900` (WebDAV) and port `2222` (SFTP).
- **Permission Denied**: If your user account is marked as `readonly: true`, you can download and view files, but attempting to upload or delete will return "Permission Denied".
