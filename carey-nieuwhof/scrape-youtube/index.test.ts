import { describe, expect, it } from "vitest";
import {
  extractHandleFromUrl,
  getUploadedAtDate,
  isLiveVideo,
  isTruthy,
  normalizeChannelUrl,
  parsePositiveInt,
  resolveVideoUrl,
  shellJoin,
  shellQuote,
  splitArgs,
  toIntegerOrNull,
  toNonEmptyString,
  type RawVideo,
} from "./utils.ts";

describe("environment helpers", () => {
  it("parses positive integers safely", () => {
    expect(parsePositiveInt("42")).toBe(42);
    expect(parsePositiveInt("5.9")).toBe(5);
    expect(parsePositiveInt("0")).toBeUndefined();
    expect(parsePositiveInt("-2")).toBeUndefined();
    expect(parsePositiveInt(undefined)).toBeUndefined();
  });

  it("splits CLI args while respecting quotes", () => {
    const input = `--flag value "multi word" 'single quoted' loose`;
    expect(splitArgs(input)).toEqual([
      "--flag",
      "value",
      "multi word",
      "single quoted",
      "loose",
    ]);
  });

  it("identifies truthy string values", () => {
    expect(isTruthy("TRUE")).toBe(true);
    expect(isTruthy("yes")).toBe(true);
    expect(isTruthy("0")).toBe(false);
    expect(isTruthy(undefined)).toBe(false);
  });
});

describe("shell helpers", () => {
  it("quotes arguments with spaces and apostrophes", () => {
    expect(shellQuote("plain")).toBe("plain");
    expect(shellQuote("with space")).toBe("'with space'");
    expect(shellQuote("can't stop")).toBe("'can'\\''t stop'");
  });

  it("joins commands with shell-safe quoting", () => {
    expect(shellJoin("echo", ["hello world", "plain"])).toBe(
      "echo 'hello world' plain",
    );
  });
});

describe("video metadata helpers", () => {
  const baseVideo: RawVideo = { id: "abc123" };

  it("normalizes channel URLs and trims extra path segments", () => {
    expect(
      normalizeChannelUrl(" https://youtube.com/@carey/videos?view=0 "),
    ).toBe("https://youtube.com/@carey/");
  });

  it("extracts channel handles from URLs and plain strings", () => {
    expect(extractHandleFromUrl("https://youtube.com/@Carey/videos")).toBe(
      "@Carey",
    );
    expect(extractHandleFromUrl("Find us at @team/example")).toBe("@team");
    expect(extractHandleFromUrl("no handle here")).toBeUndefined();
  });

  it("prefers webpage URLs when resolving video links", () => {
    expect(
      resolveVideoUrl(
        {
          ...baseVideo,
          webpage_url: "https://youtube.com/watch?v=xyz",
        },
        "fallback",
      ),
    ).toBe("https://youtube.com/watch?v=xyz");

    expect(
      resolveVideoUrl(
        { ...baseVideo, url: "https://youtu.be/xyz" },
        "fallback",
      ),
    ).toBe("https://youtu.be/xyz");

    expect(resolveVideoUrl(baseVideo, "fallback")).toBe(
      "https://www.youtube.com/watch?v=fallback",
    );
  });

  it("derives uploaded dates from timestamps or upload_date strings", () => {
    const timestamp = 1_700_000_000;
    expect(getUploadedAtDate({ ...baseVideo, timestamp })?.getTime()).toBe(
      timestamp * 1000,
    );

    const uploadDateVideo = {
      ...baseVideo,
      upload_date: "20240115",
    };
    expect(getUploadedAtDate(uploadDateVideo)?.toISOString()).toBe(
      "2024-01-15T00:00:00.000Z",
    );

    expect(getUploadedAtDate(baseVideo)).toBeNull();
  });

  it("classifies live videos from flags or status", () => {
    expect(isLiveVideo({ ...baseVideo, is_live: true })).toBe(true);
    expect(isLiveVideo({ ...baseVideo, live_status: "LIVE" })).toBe(true);
    expect(isLiveVideo(baseVideo)).toBe(false);
  });

  it("coerces optional metadata safely", () => {
    expect(toNonEmptyString(" title ")).toBe("title");
    expect(toNonEmptyString("   ")).toBeUndefined();
    expect(toIntegerOrNull(42.9)).toBe(42);
    expect(toIntegerOrNull("17")).toBe(17);
    expect(toIntegerOrNull("NaN")).toBeNull();
  });
});
