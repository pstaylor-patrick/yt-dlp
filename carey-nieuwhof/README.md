## Carey Nieuwhof YouTube Metadata Scraper

This package turns the `yt-dlp` fork in the repo root into a purpose-built
pipeline that keeps metadata for Carey Nieuwhof’s YouTube channel in sync with
Postgres. The TypeScript entry point is `scrape-youtube/index.ts`; it shells out
to `yt-dlp`, normalizes the JSON stream, writes an artifact for auditing, and
upserts each video into `cn_youtube.videos` via Drizzle ORM.

### Requirements

- `pnpm` (the workspace already pins `pnpm@10.x` in `package.json`)
- Node.js 20+ (for native `tsx`/ESM features)
- A reachable Postgres instance plus credentials in `.env.local`
- An executable `yt-dlp` or `yt-dlp.sh` somewhere between this directory and the
  repo root (run commands from the repo so detection works)

### Setup

1. Copy `.env.example` to `.env.local` and set `POSTGRES_URL` (or `DATABASE_URL`)
   plus any Carey-specific overrides you need.
2. Install dependencies: `pnpm install`.
3. Hydrate the local Drizzle schema snapshot from the target database so the
   TypeScript code has the right table definitions: `pnpm db:pull`.

> This package no longer generates or applies migrations; another service owns
> schema changes and we simply mirror the definitions so Drizzle ORM stays
> type-safe.

### Running a scrape

- Full channel sync: `pnpm scrape:youtube`
- Quick smoke test (limits to two videos): `pnpm scrape:youtube:sample`

Both commands:

1. Load environment variables from the nearest `.env.local`/`.env`.
2. Walk up directories until a local `yt-dlp` executable is found.
3. Invoke `yt-dlp --dump-json --skip-download` against the configured playlist.
4. Stream/parse each JSON line, logging heartbeats so long runs stay chatty.
5. Sort entries by publish time, persist `videos.json` beside the script, and
   upsert batches into `cn_youtube.videos`.

The script is resumable—reruns update `scraped_at`, `updated_at`, and any mapped
fields while keeping historical raw payloads in `raw_data`.

### Data model

`db/migrations/youtube.ts` defines a `cn_youtube.videos` table with searchable
fields (`channel_url`, `title`, `uploaded_at`, etc.) and the full yt-dlp payload
(`raw_data`). Drizzle migrations live in `db/migrations/` and are configured via
`drizzle.config.ts`.

### Outputs

- `scrape-youtube/videos.json`: a timestamped snapshot of whatever yt-dlp just
  returned; handy for diffing scrapes or debugging parser issues.
- Postgres rows updated/inserted in batches (default 250) to keep long syncs
  from blowing transaction buffers.

### Environment variables

| Variable                        | Default                                         | Purpose                                                                                                                                      |
| ------------------------------- | ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `POSTGRES_URL` / `DATABASE_URL` | _required_                                      | Postgres connection string used by Drizzle and the scraper.                                                                                  |
| `POSTGRES_POOL_MAX`             | `5`                                             | Optional client pool size for long scrapes.                                                                                                  |
| `CAREY_NIEUWHOF_CHANNEL`        | `https://www.youtube.com/@CareyNieuwhof/videos` | Channel/playlist URL handed to yt-dlp.                                                                                                       |
| `CAREY_MAX_VIDEOS`              | _unset_                                         | When set, passes `--playlist-end` to limit rows (useful for tests).                                                                          |
| `CAREY_PROGRESS_INTERVAL`       | `25`                                            | Log every N parsed entries.                                                                                                                  |
| `CAREY_HEARTBEAT_MS`            | `15000`                                         | Interval for “still running” messages.                                                                                                       |
| `CAREY_YTDLP_VERBOSE`           | `false`                                         | Adds `--verbose` to yt-dlp for debugging.                                                                                                    |
| `CAREY_YTDLP_EXTRA_ARGS`        | _unset_                                         | Space-delimited extra flags forwarded to yt-dlp.                                                                                             |
| `CAREY_YTDLP_RATE_LIMIT_PRESET` | `sleep`                                         | Applies `-t <preset>` to spread out yt-dlp requests; set to `off`/blank to disable (auto-skipped if extra args already include sleep flags). |
| `CAREY_DB_BATCH_SIZE`           | `250`                                           | Number of rows per upsert batch.                                                                                                             |

### Troubleshooting

- **Cannot find yt-dlp**: ensure you run the command from somewhere inside the
  fork so `scrape-youtube/index.ts` can locate `./yt-dlp` or `./yt-dlp.sh`.
- **No data written**: confirm the Postgres URL is reachable and `videos.json`
  exists—lack of JSON output usually means yt-dlp could not access the channel.
- **Schema drift**: run `pnpm db:pull` to rehydrate the schema snapshot from the
  source database—this project intentionally does not generate or apply
  migrations anymore.

That’s it—hit `pnpm scrape:youtube` whenever you want a fresh Carey Nieuwhof
metadata snapshot.
