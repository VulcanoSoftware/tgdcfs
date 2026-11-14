// Shared shapes of the config generator's form state.

import { EncryptionConfig } from "./components/EncryptionField";

export type Backend = "telegram" | "discord";

export interface StoreConfig {
  // Name used inside the config only; never reaches the metadata.
  name: string;
  backend: Backend;
  channel: string;
}

export type MetadataType = "pinned_message" | "github_repo";
export type MirrorMode = "auto" | "forward" | "reupload";
export type SyncMode = "inline" | "background" | "tee";
// When a write is answered: once the primary store has it, or once the
// local cache has it and a worker moves it into the stores (write-back).
export type WriteAck = "primary" | "cache";

export interface FilesystemConfig {
  name: string;
  primary: string;
  mirrors: string[];
  mode: MirrorMode;
  sync: SyncMode;
  strict: boolean;
  write_ack: WriteAck;
  // Multi-source reads: spread one download over every store that holds
  // the version; read_sources limits the stores that take part.
  read_parallel: boolean;
  read_sources: string[];
  allow_shared_store: boolean;
  metadata: {
    type: MetadataType;
    github_repo: {
      repo: string;
      commit: string;
      access_token: string;
    };
  };
}

export interface DiscordConfig {
  enabled: boolean;
  bot_tokens: string[];
  max_file_size_bytes: number;
  delete_messages_on_remove: boolean;
}

export interface UserConfig {
  username: string;
  password: string;
  readonly: boolean;
}

export interface SftpConfig {
  enabled: boolean;
  host: string;
  port: number;
  host_key_file: string;
  authorized_keys_dir: string;
  upload_buffer_size_mb: number;
  write_behind: boolean;
  write_behind_max_mb: number;
  stream_segment_mb: number;
  // The generator keeps "automatic" as its own switch; the file stores 0.
  stream_window_auto: boolean;
  stream_window_mb: number;
}

export interface CacheConfig {
  enabled: boolean;
  dir: string;
  max_size_mb: number;
  max_files: number;
  max_file_size_mb: number;
  block_kb: number;
  stage_uploads: boolean;
  keep_for_reads: boolean;
  min_free_mb: number;
  max_age_hours: number;
  target_fill_percent: number;
}

// The loader's defaults; a value equal to its default is left out of the
// YAML so the server's default keeps applying.
export const CACHE_DEFAULTS: CacheConfig = {
  enabled: false,
  dir: "cache",
  max_size_mb: 20480,
  max_files: 0,
  max_file_size_mb: 4096,
  block_kb: 4096,
  stage_uploads: true,
  keep_for_reads: true,
  min_free_mb: 1024,
  max_age_hours: 0,
  target_fill_percent: 90,
};

export interface ListingConfig {
  stat_in_metadata: boolean;
  validate_on_list: boolean;
  message_cache_size: number;
}

// The loader's defaults; the block is written only with the values that
// differ from them.
export const LISTING_DEFAULTS: ListingConfig = {
  stat_in_metadata: false,
  validate_on_list: true,
  message_cache_size: 1024,
};

export interface TransferConfig {
  // UI only: when off, no transfer block is written at all and the
  // application falls back to its own defaults.
  enabled: boolean;
  upload_workers_small: number;
  upload_workers_big: number;
  upload_part_size_kb: number;
  download_piece_size_kb: number;
  download_pieces_in_flight: number;
  parallel_download_threshold_mb: number;
  connection_pool_size: number;
  chunk_cache_mb: number;
  chunk_cache_readahead: number;
  chunk_cache_block_kb: number;
  // sync: tee buffers this much per mirror between the primary's reads
  // of the upload stream and the mirror's upload.
  tee_buffer_mb: number;
}

export interface ConfigData {
  telegram: {
    // UI only: off leaves the whole backends.telegram block out.
    enabled: boolean;
    api_id: string;
    api_hash: string;
    lib: "pyrogram" | "telethon";
    account: {
      session_file: string;
    };
    bot: {
      session_file: string;
      tokens: string[];
    };
  };
  discord: DiscordConfig;
  stores: StoreConfig[];
  filesystems: FilesystemConfig[];
  tgdcfs: {
    users: UserConfig[];
    jwt: {
      secret: string;
      algorithm: string;
      life: number;
    };
    server: {
      host: string;
      port: number;
    };
    sftp: SftpConfig;
    transfer: TransferConfig;
    encryption: EncryptionConfig;
    cache: CacheConfig;
    listing: ListingConfig;
  };
}

export const newFilesystem = (
  name: string,
  primary: string
): FilesystemConfig => ({
  name,
  primary,
  mirrors: [],
  mode: "auto",
  sync: "inline",
  strict: false,
  write_ack: "primary",
  read_parallel: false,
  read_sources: [],
  allow_shared_store: false,
  metadata: {
    type: "pinned_message",
    github_repo: { repo: "", commit: "master", access_token: "" },
  },
});

// The form as it opens: one Telegram store, one file system on it, and
// the loader's defaults everywhere else. A fresh object every time, so a
// loaded config never shares nested state with the initial one.
export const defaultConfig = (): ConfigData => ({
  telegram: {
    enabled: true,
    api_id: "",
    api_hash: "",
    lib: "telethon",
    account: {
      session_file: "account.session",
    },
    bot: {
      session_file: "bot.session",
      tokens: [""],
    },
  },
  discord: {
    enabled: false,
    bot_tokens: [""],
    max_file_size_bytes: 10000000,
    delete_messages_on_remove: false,
  },
  stores: [{ name: "tg-main", backend: "telegram", channel: "" }],
  filesystems: [newFilesystem("default", "tg-main")],
  tgdcfs: {
    users: [
      {
        username: "user",
        password: "password",
        readonly: false,
      },
    ],
    jwt: {
      secret: "",
      algorithm: "HS256",
      life: 604800,
    },
    server: {
      host: "0.0.0.0",
      port: 1900,
    },
    sftp: {
      enabled: false,
      host: "0.0.0.0",
      port: 2222,
      host_key_file: "sftp_host_key",
      authorized_keys_dir: "",
      upload_buffer_size_mb: 64,
      write_behind: false,
      write_behind_max_mb: 2048,
      stream_segment_mb: 256,
      stream_window_auto: true,
      stream_window_mb: 128,
    },
    cache: { ...CACHE_DEFAULTS },
    listing: { ...LISTING_DEFAULTS },
    transfer: {
      enabled: false,
      upload_workers_small: 3,
      upload_workers_big: 8,
      upload_part_size_kb: 512,
      download_piece_size_kb: 4096,
      download_pieces_in_flight: 4,
      parallel_download_threshold_mb: 10,
      connection_pool_size: 1,
      chunk_cache_mb: 0,
      chunk_cache_readahead: 2,
      chunk_cache_block_kb: 1024,
      tee_buffer_mb: 64,
    },
    encryption: {
      enabled: false,
      encrypt_names: false,
      passphrase_source: "passphrase_env",
      passphrase: "",
      passphrase_env: "TGDCFS_MASTER_PASSPHRASE",
      passphrase_file: "secrets/master.passphrase",
      master_salt_file: "master.salt",
      chunk_size: 65536,
    },
  },
});

export const isValidDirectoryName = (name: string): boolean => {
  // Valid directory name: no / \ : * ? " < > | and not . or ..
  const invalidChars = /[\/\\:*?"<>|]/;
  return (
    !invalidChars.test(name) &&
    name !== "." &&
    name !== ".." &&
    name.trim().length > 0
  );
};

export const isValidStoreName = (name: string): boolean =>
  /^[A-Za-z0-9][A-Za-z0-9_.-]*$/.test(name);

// Whether some mirror of the file system can only be filled by
// re-uploading the bytes: another backend, Discord on either side (no
// server-side copy) or an explicit re-upload mode. Mirrors the loader's
// rule for the default sync mode.
export const needsReupload = (
  filesystem: FilesystemConfig,
  stores: StoreConfig[]
): boolean => {
  if (filesystem.mirrors.length === 0) return false;
  if (filesystem.mode === "reupload") return true;
  const primary = stores.find((s) => s.name === filesystem.primary);
  if (!primary) return false;
  return filesystem.mirrors.some((name) => {
    const store = stores.find((s) => s.name === name);
    return (
      store !== undefined &&
      (store.backend === "discord" || store.backend !== primary.backend)
    );
  });
};
