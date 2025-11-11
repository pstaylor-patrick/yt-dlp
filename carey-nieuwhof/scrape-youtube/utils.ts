export type RawVideo = Record<string, unknown> & {
  id?: string;
  title?: string;
  timestamp?: number;
  upload_date?: string;
  description?: string;
  duration?: number;
  channel?: string;
  channel_id?: string;
  channel_url?: string;
  channel_handle?: string;
  playlist_channel?: string;
  playlist_channel_id?: string;
  playlist_channel_url?: string;
  playlist_index?: number | string;
  playlist_count?: number | string;
  uploader?: string;
  uploader_id?: string;
  uploader_url?: string;
  webpage_url?: string;
  url?: string;
  is_live?: boolean;
  live_status?: string;
  extractor?: string;
  extractor_key?: string;
  ie_key?: string;
};

export function parsePositiveInt(raw?: string) {
  if (!raw) {
    return undefined;
  }

  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    return undefined;
  }

  return Math.floor(value);
}

export function splitArgs(raw?: string) {
  if (!raw) {
    return [];
  }

  const result: string[] = [];
  const regex = /"([^"]*)"|'([^']*)'|(\S+)/g;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(raw)) !== null) {
    if (match[1] !== undefined) {
      result.push(match[1]);
    } else if (match[2] !== undefined) {
      result.push(match[2]);
    } else if (match[3] !== undefined) {
      result.push(match[3]);
    }
  }
  return result;
}

export function isTruthy(raw?: string) {
  if (!raw) {
    return false;
  }

  return ["1", "true", "yes", "on"].includes(raw.toLowerCase());
}

export function shellJoin(executable: string, args: string[]) {
  return [executable, ...args].map(shellQuote).join(" ");
}

export function shellQuote(value: string) {
  if (/^[\w@%+=:,./-]+$/i.test(value)) {
    return value;
  }

  return `'${value.replace(/'/g, `'\\''`)}'`;
}

export function toNonEmptyString(value: unknown) {
  if (typeof value !== "string") {
    return undefined;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

export function toOptionalString(value: unknown) {
  const normalized = toNonEmptyString(value);
  return normalized ?? undefined;
}

export function toIntegerOrNull(value: unknown) {
  if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
    return Math.trunc(value);
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed) && parsed >= 0) {
      return Math.trunc(parsed);
    }
  }

  return null;
}

export function parseUploadDate(uploadDate: string) {
  if (uploadDate.length !== 8) {
    return Number.NaN;
  }

  const year = Number(uploadDate.slice(0, 4));
  const month = Number(uploadDate.slice(4, 6));
  const day = Number(uploadDate.slice(6, 8));

  if (
    Number.isNaN(year) ||
    Number.isNaN(month) ||
    Number.isNaN(day) ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > 31
  ) {
    return Number.NaN;
  }

  return Date.UTC(year, month - 1, day) / 1000;
}

export function getUploadedAtDate(video: RawVideo) {
  if (
    typeof video.timestamp === "number" &&
    Number.isFinite(video.timestamp) &&
    video.timestamp > 0
  ) {
    return new Date(video.timestamp * 1000);
  }

  if (typeof video.upload_date === "string") {
    const parsed = parseUploadDate(video.upload_date);
    if (!Number.isNaN(parsed) && parsed > 0) {
      return new Date(parsed * 1000);
    }
  }

  return null;
}

export function resolveVideoUrl(video: RawVideo, fallbackId: string) {
  const candidates = [
    toNonEmptyString(video.webpage_url),
    toNonEmptyString(video.url),
    `https://www.youtube.com/watch?v=${fallbackId}`,
  ];

  for (const candidate of candidates) {
    if (candidate) {
      return candidate;
    }
  }

  return `https://www.youtube.com/watch?v=${fallbackId}`;
}

export function isLiveVideo(video: RawVideo) {
  if (typeof video.is_live === "boolean") {
    return video.is_live;
  }

  if (typeof video.live_status === "string") {
    const normalized = video.live_status.toLowerCase();
    return normalized === "is_live" || normalized === "live";
  }

  return false;
}

export function normalizeChannelUrl(rawUrl: string) {
  const trimmed = rawUrl.trim();
  if (!trimmed) {
    return rawUrl;
  }

  try {
    const parsed = new URL(trimmed);
    parsed.search = "";
    parsed.hash = "";
    parsed.pathname = parsed.pathname.replace(
      /(\/(?:videos|shorts|streams|featured|community))\/?$/i,
      "/",
    );
    parsed.pathname = parsed.pathname.replace(/\/{2,}/g, "/");
    if (!parsed.pathname.endsWith("/")) {
      parsed.pathname = `${parsed.pathname}/`;
    }
    const serialized = parsed.toString();
    return serialized.endsWith("/") ? serialized : `${serialized}/`;
  } catch {
    return trimmed;
  }
}

export function extractHandleFromUrl(url: string) {
  try {
    const parsed = new URL(url);
    const match = parsed.pathname.match(/@[^/]+/);
    if (match) {
      return match[0];
    }
  } catch {
    const fallbackMatch = url.match(/@[^/]+/);
    if (fallbackMatch) {
      return fallbackMatch[0];
    }
  }
  return undefined;
}
