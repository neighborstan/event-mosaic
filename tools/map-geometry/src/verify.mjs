import { runCli } from "./cli.mjs";

await runCli("verify", process.argv.slice(2));
