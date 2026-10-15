---
sidebar_position: 5
title: Frequently Asked Questions
description: Quick answers to common user questions about TGDCFS.
---

# ❓ Frequently Asked Questions (FAQ)

### Is my data private?
Yes! TGDCFS uses your private Telegram and Discord channels. Furthermore, if **At-Rest Encryption** is enabled on your server, every single byte is encrypted on your machine before being uploaded. Neither Telegram nor Discord (nor anyone else inspecting the channel) can read your files or filenames.

---

### What happens if Telegram or Discord deletes my channel?
If your server administrator has configured **Mirror Stores** (channel redundancy), TGDCFS keeps a complete secondary copy of your files in a backup channel or across backends (e.g., primary on Telegram, mirror on Discord). If one store goes down, downloads automatically fail over to the mirror store without interrupting you.

---

### Why does my upload take a few seconds after reaching 100%?
When uploading files over WebDAV or SFTP, your client finishes sending data to TGDCFS first. TGDCFS then finalizes the message parts on Telegram/Discord and updates metadata. If your server uses a local disk cache, uploads finish instantly and background workers handle the cloud transfers.

---

### Can I share files with others?
Yes! You can share access by providing a read-only WebDAV/SFTP user account, or by sharing direct file links through the Telegram Mini App.

---

### Are file names hidden on Telegram/Discord?
When **Name Obfuscation** (`encrypt_names: true`) is enabled by your administrator, the real file and folder names are hidden behind random encrypted tokens in the Telegram/Discord channels and GitHub repository. Inside WebDAV, SFTP, and the Mini App, you see the true plaintext file names.
