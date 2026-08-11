import { runCli } from "./cli.mjs";

await runCli("publish", process.argv.slice(2));
