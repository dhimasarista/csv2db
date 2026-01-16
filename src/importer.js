import fs from "fs";
import { parse } from "csv-parse";
import { Transform } from "stream";
import { uuidv7 } from "uuidv7";
import { db } from "./db.js";

/* ================= CONFIG ================= */
const SCHEMA = process.env.DB_SCHEMA || "public";
const TABLE = "keanggotaan"; // sesuai error log kamu
const BATCH_SIZE = 300;

/* ================= STREAM ================= */
const stripBom = new Transform({
  transform(chunk, _, cb) {
    cb(null, chunk.toString("utf8").replace(/^\uFEFF/, ""));
  },
});

/* ================= UTIL ================= */
function normalizeGender(v) {
  if (!v) return null;
  const x = v.toLowerCase();
  if (x.startsWith("l")) return "L";
  if (x.startsWith("p")) return "P";
  return null;
}

function parseDate(v) {
  if (!v) return null;
  const d = new Date(v);
  return isNaN(d.getTime()) ? null : d;
}

/* ================= IMPORTER ================= */
export async function importCsv(filePath) {
  const table = db.withSchema(SCHEMA).table(TABLE);

  const parser = fs
    .createReadStream(filePath)
    .pipe(stripBom)
    .pipe(
      parse({
        columns: true,
        delimiter: ";",
        quote: '"',
        escape: '"',
        trim: true,
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
      })
    );

  let inserted = 0;
  let skipped = 0;
  let failed = 0;

  let batch = [];

  async function flushBatch() {
    if (batch.length === 0) return;

    try {
      const result = await table
        .insert(batch)
        .onConflict("no_kta") // 🔥 KUNCI UTAMA
        .ignore();

      // Postgres: jumlah row yg benar-benar masuk
      inserted += result?.rowCount ?? batch.length;
      skipped += batch.length - (result?.rowCount ?? batch.length);
    } catch (err) {
      // 🔴 fallback: batch error → degrade ke row-level
      for (const row of batch) {
        try {
          await table
            .insert(row)
            .onConflict("no_kta")
            .ignore();
          inserted++;
        } catch {
          failed++;
        }
      }
    } finally {
      batch = [];
    }
  }

  for await (const row of parser) {
    if (!row["NIK"] || !row["No. KTA"]) {
      skipped++;
      continue;
    }

    batch.push({
      id: uuidv7(),
      nik: row["NIK"],
      no_kta: row["No. KTA"],
      nama_lengkap: row["Nama Lengkap"],
      tempat_lahir: row["Kota Lahir"] ?? null,
      tanggal_lahir: parseDate(row["Tgl Lahir"]),
      jenis_kelamin: normalizeGender(row["Jenis Kelamin"]),
      agama: row["Agama"] ?? null,
      golongan_darah: row["Golongan Darah"] ?? null,
      status_perkawinan: row["Status"] ?? null,
      alamat_ktp: row["Alamat"] ?? null,
      rt_ktp: row["RT"] ?? null,
      rw_ktp: row["RW"] ?? null,
      kode_pos_ktp: row["Kode Pos"] ?? null,
      foto_formal: row["Photo"] ?? null,
      scan_ktp: row["Scan KTP"] ?? null,
      tanggal_daftar: parseDate(row["tglentri"]) ?? new Date(),
      created_at: new Date(),
      created_by: row["users"] ?? "system",
      kta_status: "active",
      is_verified: false,
    });

    if (batch.length >= BATCH_SIZE) {
      await flushBatch();
    }
  }

  await flushBatch();

  console.log("=== IMPORT SELESAI ===");
  console.log("Inserted :", inserted);
  console.log("Skipped  :", skipped);
  console.log("Failed   :", failed);
}
