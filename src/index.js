import { importCsv } from "./importer.js";

const file = process.argv[2];

if (!file) {
  console.error("Usage: node src/index.js data/file.csv");
  process.exit(1);
}

await importCsv(file);
process.exit(0);
