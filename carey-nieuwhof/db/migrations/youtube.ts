import {
  boolean,
  index,
  integer,
  jsonb,
  pgSchema,
  serial,
  text,
  timestamp,
  uniqueIndex,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";

/**
 * The Carey Nieuwhof YouTube specific schema. Keeping it isolated from
 * the default `public` schema means we do not have to worry about naming
 * collisions with any future data models.
 */
export const cnYoutube = pgSchema("cn_youtube");

/**
 * Table for normalized YouTube channel metadata so the channel level
 * information is not redundantly stored alongside every video row.
 */
export const channels = cnYoutube.table(
  "channels",
  {
    id: serial("id").primaryKey(),
    canonicalUrl: text("canonical_url").notNull(),
    externalId: text("external_id"),
    handle: text("handle"),
    displayName: text("display_name"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .default(sql`now()`),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .default(sql`now()`),
  },
  (table) => ({
    canonicalUrlUnique: uniqueIndex("channels_canonical_url_idx").on(
      table.canonicalUrl,
    ),
    handleIdx: index("channels_handle_idx").on(table.handle),
  }),
);

/**
 * Table for the raw metadata that comes from the yt-dlp scraper.
 * We store a few searchable columns alongside the complete raw payload.
 */
export const videos = cnYoutube.table(
  "videos",
  {
    id: text("id").notNull().primaryKey(),
    channelId: integer("channel_id")
      .notNull()
      .references(() => channels.id, { onDelete: "restrict" }),
    videoUrl: text("video_url").notNull(),
    title: text("title").notNull(),
    description: text("description"),
    durationSeconds: integer("duration_seconds"),
    publishedTimestamp: integer("published_timestamp"),
    uploadDate: text("upload_date"),
    uploadedAt: timestamp("uploaded_at", { withTimezone: true }),
    scrapedAt: timestamp("scraped_at", { withTimezone: true })
      .notNull()
      .default(sql`now()`),
    isLive: boolean("is_live").notNull().default(false),
    rawData: jsonb("raw_data").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .default(sql`now()`),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .default(sql`now()`),
  },
  (table) => ({
    channelIdIdx: index("videos_channel_id_idx").on(table.channelId),
    uploadedAtIdx: index("videos_uploaded_at_idx").on(table.uploadedAt),
  }),
);
