// Reads an existing config.yaml back into the form.
//
// Follows the loader (tgdcfs/config.py): the current layout with
// ``backends``, ``stores`` and ``filesystems``, and the tgfs layout with a
// top-level ``telegram`` block, ``private_file_channel``, ``tgfs.metadata``
// keyed by channel and ``telegram.redundancy``, which is translated into
// stores and file systems the same way the loader does it. Anything the
// form has no field for is reported in ``notes`` instead of being dropped
// silently.

import yaml from "js-yaml";
import {
  EncryptionConfig,
  PassphraseSource,
} from "./components/EncryptionField";
import {
  CACHE_DEFAULTS,
  CacheConfig,
  ConfigData,
  DiscordConfig,
  FilesystemConfig,
  LISTING_DEFAULTS,
  ListingConfig,
  MetadataType,
  MirrorMode,
  StoreConfig,
  SyncMode,
  TransferConfig,
  UserConfig,
  WriteAck,
  defaultConfig,
  needsReupload,
  newFilesystem,
} from "./types";

export interface ImportedConfig {
  config: ConfigData;
  withUserAccountUpload: boolean;
  withUserAccountDownload: boolean;
  // What the form could not take over, one line each.
  notes: string[];
}

type Mapping = Record<string, unknown>;

// The file is parsed with the failsafe schema, so every scalar arrives as
// a string (or null for an empty value). That keeps a Discord channel id
// above 2^53 intact, which the default schema would round as a number;
// the typed accessors below convert what the form stores as numbers and
// booleans. Truthy words follow PyYAML, which the server parses with.
const isMapping = (value: unknown): value is Mapping =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const asMapping = (value: unknown): Mapping =>
  isMapping(value) ? value : {};

const asString = (value: unknown, fallback = ""): string =>
  typeof value === "string" ? value : fallback;

const asNumber = (value: unknown, fallback: number): number => {
  const text = asString(value).trim();
  const number = Number(text);
  return text !== "" && Number.isFinite(number) ? number : fallback;
};

const asBoolean = (value: unknown, fallback: boolean): boolean => {
  const text = asString(value).trim().toLowerCase();
  if (["true", "yes", "on"].includes(text)) return true;
  if (["false", "no", "off"].includes(text)) return false;
  return fallback;
};

const asList = (value: unknown): string[] => {
  if (Array.isArray(value)) {
    return value.map((item) => asString(item).trim()).filter((s) => s !== "");
  }
  const single = asString(value).trim();
  return single !== "" ? [single] : [];
};

class Notes {
  readonly lines: string[] = [];

  add(line: string) {
    if (!this.lines.includes(line)) this.lines.push(line);
  }

  // Report every key the form has no field for.
  unknownKeys(path: string, mapping: Mapping, known: string[]) {
    Object.keys(mapping)
      .filter((key) => !known.includes(key))
      .forEach((key) =>
        this.add(`${path}.${key} is not part of this form and was left out`)
      );
  }

  // A choice outside the allowed values falls back and is reported.
  choice<T extends string>(
    path: string,
    value: unknown,
    allowed: readonly T[],
    fallback: T
  ): T {
    if (value === null || value === undefined) return fallback;
    const text = asString(value);
    if ((allowed as readonly string[]).includes(text)) return text as T;
    this.add(`${path}: unknown value '${text}', using '${fallback}'`);
    return fallback;
  }
}

const METADATA_TYPES: readonly MetadataType[] = ["pinned_message", "github_repo"];
const MIRROR_MODES: readonly MirrorMode[] = ["auto", "forward", "reupload"];
const SYNC_MODES: readonly SyncMode[] = ["inline", "background", "tee"];
const WRITE_ACKS: readonly WriteAck[] = ["primary", "cache"];

const readMetadata = (
  path: string,
  raw: unknown,
  notes: Notes
): FilesystemConfig["metadata"] => {
  const data = asMapping(raw);
  notes.unknownKeys(path, data, ["name", "type", "github_repo"]);
  const github = asMapping(data.github_repo);
  return {
    type: notes.choice(`${path}.type`, data.type, METADATA_TYPES, "pinned_message"),
    github_repo: {
      repo: asString(github.repo),
      commit: asString(github.commit, "master"),
      access_token: asString(github.access_token),
    },
  };
};

// ``stores`` and ``filesystems`` as written today.
const readStoresAndFilesystems = (
  doc: Mapping,
  notes: Notes
): { stores: StoreConfig[]; filesystems: FilesystemConfig[] } => {
  const stores: StoreConfig[] = Object.entries(asMapping(doc.stores)).map(
    ([name, raw]) => {
      const data = asMapping(raw);
      notes.unknownKeys(`stores.${name}`, data, ["backend", "channel"]);
      return {
        name,
        backend: notes.choice(
          `stores.${name}.backend`,
          data.backend,
          ["telegram", "discord"] as const,
          "telegram"
        ),
        channel: asString(data.channel).trim(),
      };
    }
  );

  const filesystems: FilesystemConfig[] = Object.entries(
    asMapping(doc.filesystems)
  ).map(([name, raw]) => {
    const data = asMapping(raw);
    const path = `filesystems.${name}`;
    notes.unknownKeys(path, data, [
      "primary",
      "mirrors",
      "mode",
      "sync",
      "strict",
      "write_ack",
      "read_parallel",
      "read_sources",
      "allow_shared_store",
      "metadata",
    ]);
    const fs = newFilesystem(name, asString(data.primary).trim());
    fs.mirrors = asList(data.mirrors);
    fs.mode = notes.choice(`${path}.mode`, data.mode, MIRROR_MODES, "auto");
    fs.strict = asBoolean(data.strict, false);
    fs.write_ack = notes.choice(
      `${path}.write_ack`,
      data.write_ack,
      WRITE_ACKS,
      "primary"
    );
    fs.read_parallel = asBoolean(data.read_parallel, false);
    fs.read_sources = asList(data.read_sources);
    fs.allow_shared_store = asBoolean(data.allow_shared_store, false);
    fs.metadata = readMetadata(`${path}.metadata`, data.metadata, notes);
    // Left out, sync is derived from the stores, as the loader does it.
    const derived: SyncMode =
      needsReupload(fs, stores) && !fs.strict ? "background" : "inline";
    fs.sync =
      data.sync === undefined
        ? derived
        : notes.choice(`${path}.sync`, data.sync, SYNC_MODES, derived);
    if (fs.strict && fs.sync !== "inline") {
      notes.add(
        `${path}: 'strict: true' requires 'sync: inline'; strict was turned off`
      );
      fs.strict = false;
    }
    if (fs.sync === "tee" && fs.write_ack === "cache") {
      notes.add(
        `${path}: 'sync: tee' cannot be combined with 'write_ack: cache'; write_ack was set to primary`
      );
      fs.write_ack = "primary";
    }
    if (fs.strict && fs.write_ack === "cache") {
      notes.add(
        `${path}: 'write_ack: cache' cannot be combined with 'strict: true'; write_ack was set to primary`
      );
      fs.write_ack = "primary";
    }
    return fs;
  });

  return { stores, filesystems };
};

// The tgfs layout: ``telegram.private_file_channel`` lists the primary
// channels, ``tgfs.metadata[<channel>]`` names each file system and
// ``telegram.redundancy.mirrors[<channel>]`` lists its mirrors.
const readLegacyStoresAndFilesystems = (
  telegram: Mapping,
  app: Mapping,
  notes: Notes
): { stores: StoreConfig[]; filesystems: FilesystemConfig[] } => {
  const channels = asList(telegram.private_file_channel);
  if (channels.length === 0) {
    notes.add(
      "telegram.private_file_channel is empty; the stores and file systems were left as they were"
    );
    const fresh = defaultConfig();
    return { stores: fresh.stores, filesystems: fresh.filesystems };
  }
  notes.add(
    "The tgfs layout (private_file_channel, metadata per channel, redundancy) was translated into stores and file systems"
  );

  const redundancy = asMapping(telegram.redundancy);
  notes.unknownKeys("telegram.redundancy", redundancy, [
    "mirrors",
    "mode",
    "strict",
  ]);
  const mirrorMap = asMapping(redundancy.mirrors);
  const redundancyMode = notes.choice(
    "telegram.redundancy.mode",
    redundancy.mode,
    ["forward", "reupload"] as const,
    "forward"
  );
  const strict = asBoolean(redundancy.strict, false);

  const stores: StoreConfig[] = [];
  const storeName = (channel: string): string => {
    const name = `tg-${channel}`;
    if (!stores.some((s) => s.name === name)) {
      stores.push({ name, backend: "telegram", channel });
    }
    return name;
  };

  const metadataMap = asMapping(app.metadata);
  // A very old single-channel config keeps ``metadata: {type, ...}``
  // without the channel key; it belongs to the only channel.
  const singleForm = "type" in metadataMap && channels.length === 1;
  const usedNames = new Set<string>();

  const filesystems = channels.map((channel) => {
    const meta = singleForm ? metadataMap : asMapping(metadataMap[channel]);
    if (!singleForm && !isMapping(metadataMap[channel])) {
      notes.add(`tgdcfs.metadata.${channel} is missing; the file system was named after the channel`);
    }
    let name = asString(meta.name, singleForm ? "default" : channel);
    if (usedNames.has(name)) name = `${name}-${channel}`;
    usedNames.add(name);
    const mirrors = asList(mirrorMap[channel]).filter((m) => m !== channel);
    const fs = newFilesystem(name, storeName(channel));
    fs.mirrors = mirrors.map(storeName);
    fs.mode = mirrors.length > 0 ? redundancyMode : "auto";
    fs.strict = mirrors.length > 0 && strict;
    // tgfs allowed a mirror to be another channel's primary; keep that
    // where it is in use.
    fs.allow_shared_store = mirrors.some((m) => channels.includes(m));
    fs.metadata = readMetadata(`tgdcfs.metadata.${channel}`, meta, notes);
    // tgfs always mirrored inside the write. The form leaves the choice
    // to the loader, which copies a re-uploading mirror in the background.
    fs.sync = needsReupload(fs, stores) && !fs.strict ? "background" : "inline";
    if (fs.sync === "background") {
      notes.add(
        `filesystems.${name}: re-uploading mirrors are copied in the background now; tgfs mirrored inside the write`
      );
    }
    return fs;
  });

  return { stores, filesystems };
};

const readUsers = (raw: unknown, notes: Notes): UserConfig[] => {
  const users = Object.entries(asMapping(raw)).map(([username, value]) => {
    const data = asMapping(value);
    notes.unknownKeys(`tgdcfs.users.${username}`, data, ["password", "readonly"]);
    return {
      username,
      password: asString(data.password),
      readonly: asBoolean(data.readonly, false),
    };
  });
  return users.length > 0
    ? users
    : [{ username: "", password: "", readonly: false }];
};

const readTransfer = (
  raw: unknown,
  fallback: TransferConfig,
  notes: Notes
): TransferConfig => {
  if (!isMapping(raw)) return fallback;
  const keys = (Object.keys(fallback) as (keyof TransferConfig)[]).filter(
    (key) => key !== "enabled"
  );
  notes.unknownKeys("tgdcfs.transfer", raw, keys);
  const transfer: TransferConfig = { ...fallback, enabled: true };
  keys.forEach((key) => {
    transfer[key] = asNumber(raw[key], fallback[key]);
  });
  return transfer;
};

const readListing = (raw: unknown, notes: Notes): ListingConfig => {
  if (!isMapping(raw)) return { ...LISTING_DEFAULTS };
  notes.unknownKeys("tgdcfs.listing", raw, Object.keys(LISTING_DEFAULTS));
  return {
    stat_in_metadata: asBoolean(
      raw.stat_in_metadata,
      LISTING_DEFAULTS.stat_in_metadata
    ),
    validate_on_list: asBoolean(
      raw.validate_on_list,
      LISTING_DEFAULTS.validate_on_list
    ),
    message_cache_size: asNumber(
      raw.message_cache_size,
      LISTING_DEFAULTS.message_cache_size
    ),
  };
};

const readCache = (raw: unknown, notes: Notes): CacheConfig => {
  if (!isMapping(raw)) return { ...CACHE_DEFAULTS };
  notes.unknownKeys("tgdcfs.cache", raw, Object.keys(CACHE_DEFAULTS));
  const cache: Record<string, string | number | boolean> = {};
  (Object.keys(CACHE_DEFAULTS) as (keyof CacheConfig)[]).forEach((key) => {
    const fallback = CACHE_DEFAULTS[key];
    if (typeof fallback === "boolean") {
      cache[key] = asBoolean(raw[key], fallback);
    } else if (typeof fallback === "number") {
      cache[key] = asNumber(raw[key], fallback);
    } else {
      cache[key] = asString(raw[key], fallback);
    }
  });
  return cache as unknown as CacheConfig;
};

const readEncryption = (
  raw: unknown,
  fallback: EncryptionConfig,
  notes: Notes
): EncryptionConfig => {
  if (!isMapping(raw)) return fallback;
  notes.unknownKeys("tgdcfs.encryption", raw, [
    "enabled",
    "encrypt_names",
    "passphrase",
    "passphrase_env",
    "passphrase_file",
    "master_salt_file",
    "chunk_size",
  ]);
  const passphrase = asString(raw.passphrase);
  const passphraseEnv = asString(raw.passphrase_env);
  const passphraseFile = asString(raw.passphrase_file);
  const source: PassphraseSource = passphrase
    ? "passphrase"
    : passphraseFile
    ? "passphrase_file"
    : "passphrase_env";
  return {
    enabled: asBoolean(raw.enabled, false),
    encrypt_names: asBoolean(raw.encrypt_names, false),
    passphrase_source: source,
    passphrase,
    passphrase_env: passphraseEnv || fallback.passphrase_env,
    passphrase_file: passphraseFile || fallback.passphrase_file,
    master_salt_file: asString(raw.master_salt_file, fallback.master_salt_file),
    chunk_size: asNumber(raw.chunk_size, fallback.chunk_size),
  };
};

const readDiscord = (
  raw: unknown,
  fallback: DiscordConfig,
  notes: Notes
): DiscordConfig => {
  if (!isMapping(raw)) return fallback;
  notes.unknownKeys("backends.discord", raw, [
    "bot_token",
    "bot_tokens",
    "max_file_size_bytes",
    "delete_messages_on_remove",
  ]);
  const tokens = asList(raw.bot_tokens);
  const single = asString(raw.bot_token).trim();
  if (single) tokens.unshift(single);
  return {
    enabled: true,
    bot_tokens: tokens.length > 0 ? tokens : [""],
    max_file_size_bytes: asNumber(
      raw.max_file_size_bytes,
      fallback.max_file_size_bytes
    ),
    delete_messages_on_remove: asBoolean(raw.delete_messages_on_remove, false),
  };
};

export const importConfig = (text: string): ImportedConfig => {
  const doc = yaml.load(text, { schema: yaml.FAILSAFE_SCHEMA });
  if (!isMapping(doc)) {
    throw new Error("The file does not hold a YAML mapping");
  }
  const notes = new Notes();
  const config = defaultConfig();

  // The application block: ``tgdcfs``, or ``tgfs`` from before the rename.
  let app: Mapping;
  if (isMapping(doc.tgdcfs)) {
    app = doc.tgdcfs;
  } else if (isMapping(doc.tgfs)) {
    app = doc.tgfs;
    notes.add("The top-level block 'tgfs' was read as 'tgdcfs'");
  } else {
    throw new Error("The configuration block 'tgdcfs' is missing");
  }

  const legacyLayout = !("stores" in doc) && !("filesystems" in doc);
  const backends = asMapping(doc.backends);
  notes.unknownKeys("backends", backends, ["telegram", "discord"]);
  notes.unknownKeys("", doc, [
    "backends",
    "stores",
    "filesystems",
    "tgdcfs",
    "tgfs",
    "telegram",
  ]);

  // Backend credentials: ``backends.telegram``, or top-level ``telegram``
  // in the tgfs layout.
  const telegramPath = isMapping(backends.telegram)
    ? "backends.telegram"
    : "telegram";
  const telegram = isMapping(backends.telegram)
    ? backends.telegram
    : isMapping(doc.telegram)
    ? doc.telegram
    : undefined;
  let withUserAccountUpload = false;
  let withUserAccountDownload = false;
  // No Telegram block means a Discord-only deployment: the backend is off
  // in the form, as Discord is when its block is missing.
  config.telegram.enabled = telegram !== undefined;
  if (telegram) {
    notes.unknownKeys(telegramPath, telegram, [
      "api_id",
      "api_hash",
      "lib",
      "account",
      "bot",
      ...(legacyLayout ? ["private_file_channel", "redundancy"] : []),
    ]);
    config.telegram.api_id = asString(telegram.api_id).trim();
    config.telegram.api_hash = asString(telegram.api_hash).trim();
    config.telegram.lib = notes.choice(
      `${telegramPath}.lib`,
      telegram.lib,
      ["pyrogram", "telethon"] as const,
      "telethon"
    );
    const bot = asMapping(telegram.bot);
    notes.unknownKeys(`${telegramPath}.bot`, bot, [
      "token",
      "tokens",
      "session_file",
    ]);
    const tokens = asList(bot.tokens);
    const single = asString(bot.token).trim();
    if (single) tokens.unshift(single);
    config.telegram.bot = {
      session_file: asString(bot.session_file, "bot.session"),
      tokens: tokens.length > 0 ? tokens : [""],
    };
    if (isMapping(telegram.account)) {
      notes.unknownKeys(`${telegramPath}.account`, telegram.account, [
        "session_file",
        "used_to_upload",
        "used_to_download",
      ]);
      withUserAccountUpload = asBoolean(telegram.account.used_to_upload, false);
      withUserAccountDownload = asBoolean(
        telegram.account.used_to_download,
        false
      );
    }
  }

  config.discord = readDiscord(backends.discord, config.discord, notes);

  const { stores, filesystems } = legacyLayout
    ? readLegacyStoresAndFilesystems(telegram ?? {}, app, notes)
    : readStoresAndFilesystems(doc, notes);
  if (stores.length > 0) config.stores = stores;
  if (filesystems.length > 0) config.filesystems = filesystems;
  if (!legacyLayout && stores.length === 0) {
    notes.add("No stores were found; the form keeps its empty one");
  }

  notes.unknownKeys("tgdcfs", app, [
    "users",
    "jwt",
    "server",
    "sftp",
    "transfer",
    "encryption",
    "cache",
    "listing",
    ...(legacyLayout ? ["metadata"] : []),
  ]);
  config.tgdcfs.users = readUsers(app.users, notes);

  const jwt = asMapping(app.jwt);
  notes.unknownKeys("tgdcfs.jwt", jwt, ["secret", "algorithm", "life"]);
  config.tgdcfs.jwt = {
    secret: asString(jwt.secret),
    algorithm: asString(jwt.algorithm, "HS256"),
    life: asNumber(jwt.life, 604800),
  };

  const server = asMapping(app.server);
  notes.unknownKeys("tgdcfs.server", server, ["host", "port"]);
  config.tgdcfs.server = {
    host: asString(server.host, config.tgdcfs.server.host),
    port: asNumber(server.port, config.tgdcfs.server.port),
  };

  if (isMapping(app.sftp)) {
    const sftp = app.sftp;
    notes.unknownKeys("tgdcfs.sftp", sftp, [
      "enabled",
      "host",
      "port",
      "host_key_file",
      "authorized_keys_dir",
      "upload_buffer_size_mb",
      "write_behind",
      "write_behind_max_mb",
      "stream_segment_mb",
      "stream_window_mb",
    ]);
    const fallback = config.tgdcfs.sftp;
    // 0 (or no value) is the automatic window; anything above is a fixed one.
    const windowMb = asNumber(sftp.stream_window_mb, 0);
    config.tgdcfs.sftp = {
      enabled: asBoolean(sftp.enabled, false),
      host: asString(sftp.host, fallback.host),
      port: asNumber(sftp.port, fallback.port),
      host_key_file: asString(sftp.host_key_file, fallback.host_key_file),
      authorized_keys_dir: asString(sftp.authorized_keys_dir),
      upload_buffer_size_mb: asNumber(
        sftp.upload_buffer_size_mb,
        fallback.upload_buffer_size_mb
      ),
      write_behind: asBoolean(sftp.write_behind, fallback.write_behind),
      write_behind_max_mb: asNumber(
        sftp.write_behind_max_mb,
        fallback.write_behind_max_mb
      ),
      stream_segment_mb: asNumber(
        sftp.stream_segment_mb,
        fallback.stream_segment_mb
      ),
      stream_window_auto: windowMb <= 0,
      stream_window_mb: windowMb > 0 ? windowMb : fallback.stream_window_mb,
    };
  }

  config.tgdcfs.transfer = readTransfer(
    app.transfer,
    config.tgdcfs.transfer,
    notes
  );
  config.tgdcfs.cache = readCache(app.cache, notes);
  config.tgdcfs.listing = readListing(app.listing, notes);
  config.tgdcfs.encryption = readEncryption(
    app.encryption,
    config.tgdcfs.encryption,
    notes
  );

  return {
    config,
    withUserAccountUpload,
    withUserAccountDownload,
    notes: notes.lines,
  };
};
