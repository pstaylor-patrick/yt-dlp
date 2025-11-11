import * as youtube from "./youtube.ts";

export const schema = {
  ...youtube,
};

export type AppSchema = typeof schema;

export { youtube };

export * from "./youtube.ts";
