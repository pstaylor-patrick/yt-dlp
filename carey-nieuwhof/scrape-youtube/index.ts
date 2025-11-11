import { config as loadEnv } from "dotenv";
import { eq, sql } from "drizzle-orm";
import { spawn } from "node:child_process";
import { constants, promises as fsp } from "node:fs";
import * as fs from "node:fs";
import { tmpdir } from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";
import { closeDatabaseConnections, db, type Database } from "../db/db.ts";
import { channels, videos } from "../db/migrations/index.ts";
import {
  extractHandleFromUrl,
  getUploadedAtDate,
  isLiveVideo,
  isTruthy,
  normalizeChannelUrl,
  parsePositiveInt,
  parseUploadDate,
  resolveVideoUrl,
  shellJoin,
  splitArgs,
  toIntegerOrNull,
  toNonEmptyString,
  toOptionalString,
  type RawVideo,
} from "./utils.ts";

type VideoInsert = typeof videos.$inferInsert;
type ChannelInsert = typeof channels.$inferInsert;
type ChannelRow = typeof channels.$inferSelect;

type VideoHandler = (video: RawVideo) => Promise<void>;

type IngestionStats = {
  parsed: number;
  inserted: number;
  skippedExisting: number;
  invalid: number;
};

type IngestionContext = {
  database: Database;
  channelUrl: string;
  scrapedAt: Date;
  stats: IngestionStats;
  knownIds: Set<string>;
  channel?: ChannelRow;
};

type SortKeyInfo = {
  uploadDate?: string;
  uploadedAt?: Date;
};

type PlaylistCoverageInfo = {
  maxPlaylistIndex?: number;
  maxPlaylistCount?: number;
};

type ChannelBootstrapState = {
  channel?: ChannelRow;
  channelMatchStrategy?: "canonical" | "handle" | "raw_data" | "fallback";
  knownVideoIds: Set<string>;
  coverage?: PlaylistCoverageInfo;
  archiveEntries: DownloadArchiveEntry[];
};

type DownloadArchiveHandle = {
  path: string;
  cleanup: () => Promise<void>;
};

type DownloadArchiveEntry = {
  id: string;
  extractorKeys: string[];
};

type ScrapeOptions = {
  onVideo: VideoHandler;
  dateAfter?: string;
  reversePlaylist?: boolean;
  breakOnExisting?: boolean;
  downloadArchivePath?: string;
};

type RateLimitPreset = {
  preset: string;
  explicit: boolean;
};

loadLocalEnv();

const CHANNEL_URL =
  process.env.CAREY_NIEUWHOF_CHANNEL ??
  "https://www.youtube.com/@CareyNieuwhof/videos";
const PROGRESS_INTERVAL = Math.max(
  1,
  Number(process.env.CAREY_PROGRESS_INTERVAL ?? 25),
);
const HEARTBEAT_MS = Math.max(
  1000,
  Number(process.env.CAREY_HEARTBEAT_MS ?? 15000),
);
const PLAYLIST_END = parsePositiveInt(process.env.CAREY_MAX_VIDEOS);
const EXTRA_ARGS = splitArgs(process.env.CAREY_YTDLP_EXTRA_ARGS);
const YTDLP_VERBOSE = isTruthy(process.env.CAREY_YTDLP_VERBOSE);
const USER_SPECIFIED_DATEAFTER = EXTRA_ARGS.some((arg) =>
  arg.startsWith("--dateafter"),
);
const LOG_SKIPPED_VIDEOS = isTruthy(process.env.CAREY_LOG_SKIPPED_VIDEOS);
const RATE_LIMIT_PRESET_DISABLED_VALUES = new Set([
  "0",
  "false",
  "off",
  "none",
  "no",
  "disable",
  "disabled",
]);
const RATE_LIMIT_PRESET = determineRateLimitPreset(EXTRA_ARGS);
const DB_MAX_RETRIES = parsePositiveInt(process.env.CAREY_DB_MAX_RETRIES) ?? 5;
const DB_RETRY_BASE_MS =
  parsePositiveInt(process.env.CAREY_DB_RETRY_BASE_MS) ?? 500;
const DB_RETRY_MAX_MS =
  parsePositiveInt(process.env.CAREY_DB_RETRY_MAX_MS) ?? 5000;
const TRANSIENT_DB_ERROR_CODES = new Set([
  "ECONNRESET",
  "ETIMEDOUT",
  "ECONNREFUSED",
  "EPIPE",
  "57P01",
]);
const DEFAULT_ARCHIVE_EXTRACTOR_KEYS = ["youtubetab", "youtube"];

async function main() {
  const scriptPath = fs.realpathSync(process.argv[1] ?? __filename);
  const scriptDir = path.dirname(scriptPath);

  const ytDlpExecutable = await locateYtDlp(scriptDir);
  console.log(`Using yt-dlp executable at: ${ytDlpExecutable}`);
  console.log(`Scraping channel: ${CHANNEL_URL}`);

  const bootstrapState = await loadExistingChannelState(db, CHANNEL_URL);
  const knownVideoCount = bootstrapState.knownVideoIds.size;
  if (knownVideoCount > 0) {
    console.log(
      `[db] Found ${knownVideoCount} previously ingested video id${
        knownVideoCount === 1 ? "" : "s"
      } in the database.`,
    );
  }
  if (bootstrapState.channelMatchStrategy) {
    console.log(
      `[db] Matched existing channel via ${bootstrapState.channelMatchStrategy} lookup.`,
    );
  } else if (!bootstrapState.channel) {
    console.log(
      "[db] No existing channel record found; starting fresh scrape.",
    );
  }

  const downloadArchiveHandle = await createDownloadArchiveFile(
    bootstrapState.archiveEntries,
  );
  if (downloadArchiveHandle) {
    console.log(
      `[yt-dlp] Prepared download archive with ${knownVideoCount} entr${
        knownVideoCount === 1 ? "y" : "ies"
      } so duplicates are skipped immediately.`,
    );
  }

  const ingestionContext = createIngestionContext(db, CHANNEL_URL, {
    channel: bootstrapState.channel,
    knownVideoIds: bootstrapState.knownVideoIds,
  });
  const latestSortKey = await getLatestDatabaseSortKey(
    db,
    bootstrapState.channel?.id,
  );
  logSortKeyHint(latestSortKey);
  logPlaylistCoverageHint(bootstrapState.coverage);
  const backfillModeEnabled = needsPlaylistBackfill(bootstrapState);
  if (backfillModeEnabled) {
    console.log(
      "[yt-dlp] Playlist backfill mode enabled (processing oldest items first to resume where the previous run stopped).",
    );
  }
  const breakOnExistingEnabled = Boolean(
    downloadArchiveHandle &&
      bootstrapState.knownVideoIds.size > 0 &&
      !backfillModeEnabled,
  );
  if (breakOnExistingEnabled) {
    console.log(
      "[yt-dlp] Will stop early once an already ingested video is encountered (--break-on-existing).",
    );
  }

  const autoDateAfterEnabled =
    !isTruthy(process.env.CAREY_DISABLE_AUTO_DATEAFTER) &&
    !USER_SPECIFIED_DATEAFTER;
  const canUseAutoDateAfter = shouldUseAutoDateAfter(bootstrapState);

  if (USER_SPECIFIED_DATEAFTER) {
    console.log(
      "[yt-dlp] Using user supplied --dateafter value from CAREY_YTDLP_EXTRA_ARGS.",
    );
  } else if (autoDateAfterEnabled && !canUseAutoDateAfter) {
    console.log(
      "[yt-dlp] Auto --dateafter disabled because this channel still has older playlist items to backfill.",
    );
  }
  const dateAfter =
    autoDateAfterEnabled && canUseAutoDateAfter && latestSortKey
      ? computeDateAfter(latestSortKey)
      : undefined;

  if (dateAfter) {
    console.log(
      `[yt-dlp] Auto-applying '--dateafter ${dateAfter}' to skip previously ingested videos (set CAREY_DISABLE_AUTO_DATEAFTER=1 to disable).`,
    );
  }

  try {
    const parsedCount = await scrapeChannel(ytDlpExecutable, CHANNEL_URL, {
      dateAfter,
      reversePlaylist: backfillModeEnabled,
      breakOnExisting: breakOnExistingEnabled,
      downloadArchivePath: downloadArchiveHandle?.path,
      onVideo: async (video) => {
        ingestionContext.stats.parsed += 1;
        await ingestVideo(video, ingestionContext);
      },
    });

    console.log(
      `[yt-dlp] Finished parsing ${parsedCount} entr${
        parsedCount === 1 ? "y" : "ies"
      }.`,
    );
    logIngestionSummary(ingestionContext.stats);
  } finally {
    if (downloadArchiveHandle) {
      await downloadArchiveHandle.cleanup().catch((error: unknown) => {
        console.warn(
          `[fs] Failed to clean up temporary download archive: ${
            (error as Error).message
          }`,
        );
      });
    }
    await closeDatabaseConnections().catch((error: unknown) => {
      console.warn(
        `[db] Failed to close database connections: ${(error as Error).message}`,
      );
    });
  }
}

async function scrapeChannel(
  executable: string,
  url: string,
  options: ScrapeOptions,
) {
  const args = [
    "--ignore-errors",
    "--no-warnings",
    "--dump-json",
    "--skip-download",
    "--yes-playlist",
  ];

  if (YTDLP_VERBOSE) {
    args.unshift("--verbose");
  }

  if (Number.isFinite(PLAYLIST_END)) {
    args.push("--playlist-end", String(PLAYLIST_END));
  }

  if (RATE_LIMIT_PRESET) {
    args.push("-t", RATE_LIMIT_PRESET.preset);
    if (RATE_LIMIT_PRESET.explicit) {
      console.log(
        `[yt-dlp] Applying user-specified '-t ${RATE_LIMIT_PRESET.preset}' preset to throttle requests.`,
      );
    } else {
      console.log(
        `[yt-dlp] Applying default '-t ${RATE_LIMIT_PRESET.preset}' preset to reduce rate limiting (set CAREY_YTDLP_RATE_LIMIT_PRESET=off to disable).`,
      );
    }
  }

  if (EXTRA_ARGS.length > 0) {
    args.push(...EXTRA_ARGS);
  }

  if (options.dateAfter) {
    args.push("--dateafter", options.dateAfter);
  }

  if (options.reversePlaylist) {
    args.push("--playlist-reverse");
  }

  if (options.breakOnExisting) {
    args.push("--break-on-existing");
  }

  if (options.downloadArchivePath) {
    args.push("--download-archive", options.downloadArchivePath);
  }

  args.push(url);

  console.log(`[yt-dlp] Command: ${shellJoin(executable, args)}`);

  return new Promise<number>((resolve, reject) => {
    let parsedCount = 0;
    let stdoutBuffer = "";
    let stderrBuffer = "";
    let lastOutput = Date.now();
    const inFlightIngestions = new Set<Promise<void>>();
    let ingestionError: Error | null = null;

    const child = spawn(executable, args, {
      cwd: path.dirname(executable),
      stdio: ["ignore", "pipe", "pipe"],
      env: process.env,
    });
    const abortChild = () => {
      if (child.exitCode === null && !child.killed) {
        child.kill();
      }
    };
    const trackIngestionTask = (task: Promise<void>) => {
      inFlightIngestions.add(task);
      task.finally(() => {
        inFlightIngestions.delete(task);
      });
    };
    const heartbeat = setInterval(() => {
      const secondsSinceOutput = Math.round((Date.now() - lastOutput) / 1000);
      console.log(
        `[yt-dlp] still running... parsed ${parsedCount} entries so far (last output ${secondsSinceOutput}s ago)`,
      );
    }, HEARTBEAT_MS);

    const enqueueVideo = (video: RawVideo) => {
      if (ingestionError) {
        return;
      }

      const task = options.onVideo(video).catch((error) => {
        if (!ingestionError) {
          ingestionError =
            error instanceof Error ? error : new Error(String(error));
          abortChild();
        }
        throw ingestionError;
      });

      trackIngestionTask(task);
    };

    child.stdout?.setEncoding("utf8");
    child.stdout?.on("data", (chunk: string) => {
      lastOutput = Date.now();
      stdoutBuffer += chunk;
      let newlineIndex = stdoutBuffer.indexOf("\n");
      while (newlineIndex !== -1) {
        const line = stdoutBuffer.slice(0, newlineIndex).trim();
        stdoutBuffer = stdoutBuffer.slice(newlineIndex + 1);
        if (line.length > 0) {
          try {
            const parsed = JSON.parse(line) as RawVideo;
            parsedCount += 1;
            enqueueVideo(parsed);
            maybeLogProgress(parsedCount, parsed);
          } catch (error) {
            child.kill();
            reject(
              new Error(
                `Failed to parse yt-dlp output line: ${(error as Error).message}`,
              ),
            );
            return;
          }
        }
        newlineIndex = stdoutBuffer.indexOf("\n");
      }
    });

    child.stderr?.setEncoding("utf8");
    child.stderr?.on("data", (chunk: string) => {
      stderrBuffer += chunk;
      process.stderr.write(chunk);
      lastOutput = Date.now();
    });

    child.on("error", reject);

    child.on("close", async (code) => {
      clearInterval(heartbeat);

      const remaining = stdoutBuffer.trim();
      if (remaining.length > 0) {
        try {
          const parsed = JSON.parse(remaining) as RawVideo;
          parsedCount += 1;
          enqueueVideo(parsed);
          maybeLogProgress(parsedCount, parsed);
        } catch (error) {
          reject(
            new Error(
              `Failed to parse trailing yt-dlp output: ${(error as Error).message}`,
            ),
          );
          return;
        }
      }

      await Promise.allSettled(inFlightIngestions);

      if (ingestionError) {
        reject(ingestionError);
        return;
      }

      if (code !== 0 && parsedCount === 0) {
        reject(
          new Error(
            `yt-dlp exited with code ${code}. stderr:\n${stderrBuffer.trim()}`,
          ),
        );
        return;
      }

      if (stderrBuffer.trim().length > 0) {
        console.warn(stderrBuffer.trim());
      }

      resolve(parsedCount);
    });
  });
}

function createIngestionContext(
  database: Database,
  channelUrl: string,
  bootstrap?: Pick<ChannelBootstrapState, "channel" | "knownVideoIds">,
): IngestionContext {
  const normalizedChannelUrl = normalizeChannelUrl(channelUrl);
  return {
    database,
    channelUrl: normalizedChannelUrl,
    scrapedAt: new Date(),
    stats: {
      parsed: 0,
      inserted: 0,
      skippedExisting: 0,
      invalid: 0,
    },
    knownIds: new Set(bootstrap?.knownVideoIds ?? []),
    channel: bootstrap?.channel,
  };
}

async function loadExistingChannelState(
  database: Database,
  channelUrl: string,
): Promise<ChannelBootstrapState> {
  const normalizedChannelUrl = normalizeChannelUrl(channelUrl);
  const handleFromUrl = extractHandleFromUrl(normalizedChannelUrl);

  let channel: ChannelRow | undefined;
  let channelMatchStrategy: ChannelBootstrapState["channelMatchStrategy"];

  channel = await withDatabaseRetry(() =>
    database.query.channels.findFirst({
      where: (table) => eq(table.canonicalUrl, normalizedChannelUrl),
    }),
  );
  if (channel) {
    channelMatchStrategy = "canonical";
  }

  if (!channel && handleFromUrl) {
    channel = await withDatabaseRetry(() =>
      database.query.channels.findFirst({
        where: (table) => eq(table.handle, handleFromUrl),
      }),
    );
    if (!channel && handleFromUrl.startsWith("@")) {
      channel = await withDatabaseRetry(() =>
        database.query.channels.findFirst({
          where: (table) => eq(table.handle, handleFromUrl.slice(1)),
        }),
      );
    }
    if (channel) {
      channelMatchStrategy = "handle";
    }
  }

  const baseQuery = database
    .select({
      id: videos.id,
      rawData: videos.rawData,
      channelId: videos.channelId,
    })
    .from(videos);

  let rows = await withDatabaseRetry(() =>
    channel ? baseQuery.where(eq(videos.channelId, channel!.id)) : baseQuery,
  );

  if (!channel) {
    const rowsMatchingUrl = rows.filter((row) =>
      doesRawVideoMatchChannel(
        row.rawData as RawVideo | undefined,
        normalizedChannelUrl,
      ),
    );

    const candidateRows = rowsMatchingUrl.length > 0 ? rowsMatchingUrl : rows;
    const candidateChannelId = candidateRows.find(
      (row) => row.channelId,
    )?.channelId;

    if (candidateChannelId !== undefined) {
      channel = await withDatabaseRetry(() =>
        database.query.channels.findFirst({
          where: (table) => eq(table.id, candidateChannelId),
        }),
      );
      if (channel) {
        channelMatchStrategy =
          rowsMatchingUrl.length > 0 ? "raw_data" : "fallback";
        rows = candidateRows.filter((row) => row.channelId === channel!.id);
      }
    }
  }

  if (!channel) {
    return { knownVideoIds: new Set(), archiveEntries: [] };
  }

  const relevantRows = rows.filter((row) => row.channelId === channel!.id);
  const knownVideoIds = new Set<string>();
  let maxPlaylistIndex: number | undefined;
  let maxPlaylistCount: number | undefined;
  const archiveKeyMap = new Map<string, Set<string>>();

  for (const row of relevantRows) {
    knownVideoIds.add(row.id);
    let keySet = archiveKeyMap.get(row.id);
    if (!keySet) {
      keySet = new Set<string>();
      archiveKeyMap.set(row.id, keySet);
    }
    const rawVideo = row.rawData as RawVideo | undefined;
    if (rawVideo && typeof rawVideo === "object") {
      const playlistIndex = toIntegerOrNull(rawVideo.playlist_index);
      if (typeof playlistIndex === "number") {
        maxPlaylistIndex = Math.max(
          maxPlaylistIndex ?? playlistIndex,
          playlistIndex,
        );
      }

      const playlistCount = toIntegerOrNull(rawVideo.playlist_count);
      if (typeof playlistCount === "number") {
        maxPlaylistCount = Math.max(
          maxPlaylistCount ?? playlistCount,
          playlistCount,
        );
      }

      const extractorKeys = extractArchiveExtractorKeys(rawVideo);
      if (extractorKeys.length > 0) {
        for (const key of extractorKeys) {
          keySet.add(key);
        }
      }
    }
  }

  return {
    channel,
    channelMatchStrategy,
    knownVideoIds,
    archiveEntries: Array.from(archiveKeyMap.entries()).map(([id, keys]) => ({
      id,
      extractorKeys: Array.from(keys),
    })),
    coverage:
      maxPlaylistIndex !== undefined || maxPlaylistCount !== undefined
        ? { maxPlaylistIndex, maxPlaylistCount }
        : undefined,
  };
}

function doesRawVideoMatchChannel(
  rawVideo: RawVideo | undefined,
  normalizedChannelUrl: string,
) {
  if (!rawVideo || typeof rawVideo !== "object") {
    return false;
  }

  const candidateUrls = [
    toNonEmptyString(rawVideo.channel_url),
    toNonEmptyString(rawVideo.playlist_channel_url),
    toNonEmptyString(rawVideo.uploader_url),
  ];

  return candidateUrls.some((candidate) => {
    if (!candidate) {
      return false;
    }
    return normalizeChannelUrl(candidate) === normalizedChannelUrl;
  });
}

function extractArchiveExtractorKeys(rawVideo?: RawVideo) {
  if (!rawVideo || typeof rawVideo !== "object") {
    return [] as string[];
  }

  const normalized = new Set<string>();
  const candidates = [
    toNonEmptyString(rawVideo.extractor),
    toNonEmptyString(rawVideo.extractor_key),
    toNonEmptyString(rawVideo.ie_key),
  ];

  for (const candidate of candidates) {
    if (candidate) {
      normalized.add(candidate.toLowerCase());
    }
  }

  return Array.from(normalized);
}

async function createDownloadArchiveFile(
  entries: readonly DownloadArchiveEntry[],
): Promise<DownloadArchiveHandle | undefined> {
  if (entries.length === 0) {
    return undefined;
  }

  const tempDir = await fsp.mkdtemp(path.join(tmpdir(), "carey-yt-archive-"));
  const archivePath = path.join(tempDir, "download-archive.txt");
  const lines: string[] = [];
  const seen = new Set<string>();
  for (const entry of entries) {
    const extractorKeys =
      entry.extractorKeys.length > 0
        ? entry.extractorKeys
        : DEFAULT_ARCHIVE_EXTRACTOR_KEYS;
    for (const key of extractorKeys) {
      const normalized = key.toLowerCase();
      if (!normalized) {
        continue;
      }
      const line = `${normalized} ${entry.id}\n`;
      if (seen.has(line)) {
        continue;
      }
      seen.add(line);
      lines.push(line);
    }
  }

  if (lines.length === 0) {
    return undefined;
  }

  await fsp.writeFile(archivePath, lines.join(""), "utf8");

  return {
    path: archivePath,
    cleanup: async () => {
      await fsp.rm(tempDir, { recursive: true, force: true });
    },
  };
}

async function ingestVideo(video: RawVideo, context: IngestionContext) {
  const channel = await ensureChannelRecord(video, context);
  const mapped = mapRawVideoToInsert(video, channel.id, context.scrapedAt);
  if (!mapped) {
    context.stats.invalid += 1;
    console.warn("[db] Skipping malformed video entry (missing id).");
    return;
  }

  if (context.knownIds.has(mapped.id)) {
    context.stats.skippedExisting += 1;
    if (LOG_SKIPPED_VIDEOS) {
      console.log(`[db] Skipping already ingested video ${mapped.id}`);
    }
    return;
  }
  const insertedRows = await withDatabaseRetry(() =>
    context.database
      .insert(videos)
      .values(mapped)
      .onConflictDoNothing()
      .returning({ id: videos.id }),
  );

  if (insertedRows.length === 0) {
    context.stats.skippedExisting += 1;
    context.knownIds.add(mapped.id);
    if (LOG_SKIPPED_VIDEOS) {
      console.log(`[db] Skipping already ingested video ${mapped.id}`);
    }
    return;
  }

  context.stats.inserted += 1;
  context.knownIds.add(mapped.id);
  console.log(`[db] Inserted video ${mapped.id}: ${mapped.title}`);
}

async function ensureChannelRecord(
  video: RawVideo,
  context: IngestionContext,
): Promise<ChannelRow> {
  if (context.channel) {
    return context.channel;
  }

  const insertPayload = buildChannelInsertPayload(
    video,
    context.channelUrl,
    context.scrapedAt,
  );

  const [record] = await withDatabaseRetry(() =>
    context.database
      .insert(channels)
      .values(insertPayload)
      .onConflictDoUpdate({
        target: channels.canonicalUrl,
        set: {
          externalId: sql`excluded.external_id`,
          handle: sql`excluded.handle`,
          displayName: sql`excluded.display_name`,
          updatedAt: sql`excluded.updated_at`,
        },
      })
      .returning(),
  );

  if (!record) {
    throw new Error(
      `Failed to upsert channel ${insertPayload.canonicalUrl}; database returned no record.`,
    );
  }

  context.channel = record;
  return record;
}

async function getLatestDatabaseSortKey(
  database: Database,
  channelId?: number,
): Promise<SortKeyInfo | undefined> {
  const baseQuery = database
    .select({
      uploadDate: sql<string | null>`max(${videos.uploadDate})`,
      uploadedAt: sql<Date | null>`max(${videos.uploadedAt})`,
    })
    .from(videos);

  const targetQuery =
    typeof channelId === "number"
      ? baseQuery.where(eq(videos.channelId, channelId))
      : baseQuery;

  const [row] = await withDatabaseRetry(() => targetQuery);

  if (!row) {
    return undefined;
  }

  const info: SortKeyInfo = {};
  if (row.uploadDate) {
    info.uploadDate = row.uploadDate;
  }
  if (row.uploadedAt) {
    // Aggregates can return strings, so normalize to a Date instance.
    const uploadedAt = new Date(row.uploadedAt);
    if (!Number.isNaN(uploadedAt.getTime())) {
      info.uploadedAt = uploadedAt;
    }
  }

  if (!info.uploadDate && !info.uploadedAt) {
    return undefined;
  }

  return info;
}

function logSortKeyHint(sortKey?: SortKeyInfo) {
  if (!sortKey) {
    console.log("[db] No existing video rows found; full scrape will run.");
    return;
  }

  const parts: string[] = [];
  if (sortKey.uploadDate) {
    parts.push(`upload_date=${sortKey.uploadDate}`);
  }
  if (sortKey.uploadedAt) {
    parts.push(`uploaded_at=${sortKey.uploadedAt.toISOString()}`);
  }

  console.log(`[db] Latest stored video -> ${parts.join(" | ") || "unknown"}.`);
}

function logPlaylistCoverageHint(coverage?: PlaylistCoverageInfo) {
  if (!coverage) {
    return;
  }

  const { maxPlaylistIndex, maxPlaylistCount } = coverage;
  if (
    typeof maxPlaylistIndex === "number" &&
    typeof maxPlaylistCount === "number"
  ) {
    console.log(
      `[db] Playlist coverage -> processed ${maxPlaylistIndex} of ${maxPlaylistCount} reported entries.`,
    );
    return;
  }

  if (typeof maxPlaylistIndex === "number") {
    console.log(
      `[db] Playlist coverage -> highest playlist_index seen so far: ${maxPlaylistIndex}.`,
    );
  }
}

function shouldUseAutoDateAfter(state: ChannelBootstrapState) {
  if (state.knownVideoIds.size === 0) {
    return true;
  }

  const coverage = state.coverage;
  if (
    coverage &&
    typeof coverage.maxPlaylistCount === "number" &&
    coverage.maxPlaylistCount > 0 &&
    typeof coverage.maxPlaylistIndex === "number" &&
    coverage.maxPlaylistIndex >= coverage.maxPlaylistCount
  ) {
    return true;
  }

  return false;
}

function needsPlaylistBackfill(state: ChannelBootstrapState) {
  if (state.knownVideoIds.size === 0) {
    return false;
  }

  const coverage = state.coverage;
  if (
    !coverage ||
    typeof coverage.maxPlaylistCount !== "number" ||
    coverage.maxPlaylistCount <= 0
  ) {
    return false;
  }

  return state.knownVideoIds.size < coverage.maxPlaylistCount;
}

function logIngestionSummary(stats: IngestionStats) {
  console.log(
    `[db] Summary => parsed ${stats.parsed}, inserted ${stats.inserted}, skipped ${stats.skippedExisting}, invalid ${stats.invalid}.`,
  );
}

function computeDateAfter(sortKey?: SortKeyInfo) {
  if (!sortKey) {
    return undefined;
  }

  const baseline =
    (sortKey.uploadDate && uploadDateStringToDate(sortKey.uploadDate)) ??
    sortKey.uploadedAt ??
    null;

  if (!baseline) {
    return undefined;
  }

  const cutoff = new Date(baseline);
  cutoff.setUTCDate(cutoff.getUTCDate() - 1);
  return formatDateAsYyyymmdd(cutoff);
}

function uploadDateStringToDate(uploadDate?: string) {
  if (!uploadDate) {
    return null;
  }

  const seconds = parseUploadDate(uploadDate);
  if (Number.isNaN(seconds)) {
    return null;
  }

  return new Date(seconds * 1000);
}

function formatDateAsYyyymmdd(date: Date) {
  const year = date.getUTCFullYear();
  const month = `${date.getUTCMonth() + 1}`.padStart(2, "0");
  const day = `${date.getUTCDate()}`.padStart(2, "0");
  return `${year}${month}${day}`;
}

async function locateYtDlp(startDir: string) {
  const candidateFiles = ["yt-dlp.sh", "yt-dlp"];
  let currentDir = startDir;

  while (true) {
    for (const fileName of candidateFiles) {
      const candidatePath = path.join(currentDir, fileName);
      if (await isExecutable(candidatePath)) {
        return candidatePath;
      }
    }

    const parentDir = path.dirname(currentDir);
    if (parentDir === currentDir) {
      break;
    }
    currentDir = parentDir;
  }

  throw new Error(
    "Unable to locate yt-dlp or yt-dlp.sh. Please ensure you run this script inside the forked yt-dlp repository.",
  );
}

async function isExecutable(filePath: string) {
  try {
    const stats = await fsp.stat(filePath);
    if (!stats.isFile()) {
      return false;
    }
    await fsp.access(filePath, constants.X_OK);
    return true;
  } catch {
    try {
      await fsp.access(filePath, constants.F_OK);
      return true;
    } catch {
      return false;
    }
  }
}

function buildChannelInsertPayload(
  video: RawVideo,
  fallbackChannelUrl: string,
  scrapedAt: Date,
): ChannelInsert {
  const canonicalSource =
    toNonEmptyString(video.channel_url) ??
    toNonEmptyString(video.playlist_channel_url) ??
    toNonEmptyString(video.uploader_url) ??
    fallbackChannelUrl;
  const canonicalUrl = normalizeChannelUrl(canonicalSource);

  const externalId =
    toNonEmptyString(video.channel_id) ??
    toNonEmptyString(video.playlist_channel_id) ??
    toNonEmptyString(video.uploader_id);
  const displayName =
    toNonEmptyString(video.channel) ??
    toNonEmptyString(video.playlist_channel) ??
    toNonEmptyString(video.uploader);
  const handle =
    toNonEmptyString(video.channel_handle) ??
    extractHandleFromUrl(canonicalUrl);

  return {
    canonicalUrl,
    externalId: externalId ?? undefined,
    handle: handle ?? undefined,
    displayName,
    updatedAt: scrapedAt,
  };
}

function mapRawVideoToInsert(
  video: RawVideo,
  channelId: number,
  scrapedAt: Date,
): VideoInsert | null {
  const id = toNonEmptyString(video.id);
  if (!id) {
    return null;
  }

  const title = toNonEmptyString(video.title) ?? id;
  const description = toOptionalString(video.description);
  const durationSeconds = toIntegerOrNull(video.duration);
  const publishedTimestamp = toIntegerOrNull(video.timestamp);
  const uploadDate = toNonEmptyString(video.upload_date);
  const uploadedAt = getUploadedAtDate(video);

  return {
    id,
    channelId,
    videoUrl: resolveVideoUrl(video, id),
    title,
    description: description ?? null,
    durationSeconds,
    publishedTimestamp,
    uploadDate,
    uploadedAt,
    scrapedAt,
    isLive: isLiveVideo(video),
    rawData: video,
    updatedAt: new Date(),
  };
}

function loadLocalEnv() {
  const envFiles = [".env.local", ".env"];
  const visited = new Set<string>();
  let currentDir = path.dirname(fs.realpathSync(process.argv[1] ?? __filename));

  while (!visited.has(currentDir)) {
    visited.add(currentDir);

    for (const envFile of envFiles) {
      const candidate = path.resolve(currentDir, envFile);
      if (fs.existsSync(candidate)) {
        loadEnv({ path: candidate });
        return;
      }
    }

    const parent = path.dirname(currentDir);
    if (parent === currentDir) {
      break;
    }
    currentDir = parent;
  }

  loadEnv();
}

async function withDatabaseRetry<T>(operation: () => Promise<T>): Promise<T> {
  const maxAttempts = Math.max(1, DB_MAX_RETRIES);
  const baseDelay = Math.max(50, DB_RETRY_BASE_MS);
  const maxDelay = Math.max(baseDelay, DB_RETRY_MAX_MS);

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      const isLastAttempt = attempt === maxAttempts;
      if (isLastAttempt || !isTransientDatabaseError(error)) {
        throw error;
      }

      const delayMs = Math.min(baseDelay * 2 ** (attempt - 1), maxDelay);
      console.warn(
        `[db] Transient database error (${describeDatabaseError(error)}) -> retrying in ${delayMs}ms (attempt ${
          attempt + 1
        }/${maxAttempts}).`,
      );
      await sleep(delayMs);
    }
  }

  throw new Error("Database operation failed after retries");
}

function isTransientDatabaseError(error: unknown): boolean {
  const code = extractErrorCode(error);
  if (code && TRANSIENT_DB_ERROR_CODES.has(code)) {
    return true;
  }

  if (error && typeof error === "object" && error !== null) {
    const maybeCause = (error as { cause?: unknown }).cause;
    if (
      maybeCause &&
      maybeCause !== error &&
      isTransientDatabaseError(maybeCause)
    ) {
      return true;
    }
  }

  if (error instanceof Error) {
    const normalized = error.message.toLowerCase();
    return (
      normalized.includes("connection reset") ||
      normalized.includes("server closed the connection") ||
      normalized.includes("terminating connection")
    );
  }

  return false;
}

function extractErrorCode(error: unknown): string | undefined {
  if (!error || typeof error !== "object") {
    return undefined;
  }

  const code = (error as { code?: unknown }).code;
  if (typeof code === "string" && code.trim()) {
    return code.trim().toUpperCase();
  }

  const maybeCause = (error as { cause?: unknown }).cause;
  if (maybeCause && maybeCause !== error) {
    return extractErrorCode(maybeCause);
  }

  return undefined;
}

function describeDatabaseError(error: unknown) {
  const code = extractErrorCode(error);
  const message = error instanceof Error ? error.message : undefined;
  if (code && message) {
    return `${code} - ${message}`;
  }
  if (code) {
    return code;
  }
  return message ?? "unknown error";
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function maybeLogProgress(count: number, video: RawVideo) {
  if (count % PROGRESS_INTERVAL !== 0) {
    return;
  }

  const label = [video.title, video.id].filter(Boolean).join(" • ");
  console.log(
    `[yt-dlp] Parsed ${count} entries${label ? ` (latest: ${label})` : ""}...`,
  );
}

if (isDirectCliInvocation()) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}

function isDirectCliInvocation() {
  try {
    const entryPoint = process.argv[1];
    if (!entryPoint) {
      return false;
    }

    return import.meta.url === pathToFileURL(entryPoint).href;
  } catch {
    return false;
  }
}

function determineRateLimitPreset(
  extraArgs: string[],
): RateLimitPreset | undefined {
  const resolved = resolveRateLimitPreset(
    process.env.CAREY_YTDLP_RATE_LIMIT_PRESET,
  );
  if (!resolved) {
    return undefined;
  }

  if (!resolved.explicit && hasUserProvidedSleepArgs(extraArgs)) {
    return undefined;
  }

  return resolved;
}

function resolveRateLimitPreset(
  rawValue: string | undefined,
): RateLimitPreset | undefined {
  if (rawValue === undefined) {
    return { preset: "sleep", explicit: false };
  }

  const normalized = rawValue.trim();
  if (!normalized) {
    return undefined;
  }

  if (RATE_LIMIT_PRESET_DISABLED_VALUES.has(normalized.toLowerCase())) {
    return undefined;
  }

  return { preset: normalized, explicit: true };
}

function hasUserProvidedSleepArgs(extraArgs: string[]) {
  return extraArgs.some((arg) => {
    const normalized = arg.toLowerCase();
    if (normalized === "-t" || normalized.startsWith("-t")) {
      return true;
    }

    return normalized.startsWith("--sleep-");
  });
}
