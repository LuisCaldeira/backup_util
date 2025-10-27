#!/usr/bin/env python3
# GPL v2 License
# (c) 2024 EvilWarning <...> + contributors
# Post-backup JSON metadata generator for raidtape chunks
import argparse, json, os, sqlite3, sys, time
from pathlib import Path
from typing import Dict, List, Tuple

CAT_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS settings (
  k TEXT PRIMARY KEY,
  v TEXT
);
CREATE TABLE IF NOT EXISTS volumes (
  volume_id TEXT PRIMARY KEY,
  mount_path TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('active','sealed','missing')),
  reserve_percent INTEGER NOT NULL CHECK(reserve_percent >= 0 AND reserve_percent <= 100),
  created_ts INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS chunks (
  chunk_id TEXT PRIMARY KEY, -- hex hash
  size INTEGER NOT NULL CHECK(size >= 0),
  CHECK(chunk_id <> '')
);
CREATE TABLE IF NOT EXISTS chunk_locations (
  chunk_id TEXT NOT NULL,
  volume_id TEXT NOT NULL,
  PRIMARY KEY (chunk_id, volume_id),
  FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id) ON DELETE CASCADE,
  FOREIGN KEY (volume_id) REFERENCES volumes(volume_id) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS files (
  file_id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT UNIQUE NOT NULL,
  size INTEGER NOT NULL CHECK(size >= 0),
  mtime_ns INTEGER NOT NULL,
  file_hash TEXT NOT NULL CHECK(file_hash <> '')
);
CREATE TABLE IF NOT EXISTS file_chunks (
  file_id INTEGER NOT NULL,
  seq INTEGER NOT NULL CHECK(seq >= 0),
  chunk_id TEXT NOT NULL,
  PRIMARY KEY (file_id, seq),
  FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE,
  FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS runs (
  run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_ts INTEGER NOT NULL,
  finished_ts INTEGER
);
CREATE TABLE IF NOT EXISTS run_changes (
  run_id INTEGER NOT NULL,
  path TEXT NOT NULL,
  change TEXT NOT NULL CHECK(change IN ('added','modified','deleted','unchanged')),
  PRIMARY KEY (run_id, path),
  FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS file_versions (
  file_id INTEGER NOT NULL,
  run_id INTEGER NOT NULL,
  path TEXT NOT NULL,
  size INTEGER NOT NULL,
  mtime_ns INTEGER NOT NULL,
  file_hash TEXT NOT NULL,
  PRIMARY KEY (file_id, run_id),
  FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE,
  FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS file_chunk_history (
  file_id INTEGER NOT NULL,
  run_id INTEGER NOT NULL,
  seq INTEGER NOT NULL,
  chunk_id TEXT NOT NULL,
  PRIMARY KEY (file_id, run_id, seq),
  FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE CASCADE,
  FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE,
  FOREIGN KEY (chunk_id) REFERENCES chunks(chunk_id) ON DELETE CASCADE
) WITHOUT ROWID;
"""


def connect(cat: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(cat), timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA busy_timeout=5000;")
    return con


def _retry_locked(fn, *, retries=6, base_sleep=0.05):
    import time as _t, sqlite3 as _sq

    for i in range(retries):
        try:
            return fn()
        except _sq.OperationalError as e:
            if "database is locked" not in str(e).lower():
                raise
            _t.sleep(base_sleep * (2**i))
    return fn()


def get_hash_algo(con: sqlite3.Connection) -> str:
    row = con.execute("SELECT v FROM settings WHERE k='hash_algo'").fetchone()
    return row[0] if row else "sha256"


def generate_chunk_jsons(cat: Path):
    con = connect(cat)
    # Grab all active mappings
    files = con.execute("""
        SELECT f.path, f.size, f.mtime_ns, f.file_hash, fc.seq, fc.chunk_id
        FROM files f JOIN file_chunks fc ON f.file_id = fc.file_id
        ORDER BY f.path, fc.seq
    """).fetchall()
    if not files:
        print("No files? Backup something first! 😏")
        return
    # Group by chunk_id
    chunk_meta: Dict[str, List[Dict]] = {}
    for path, size, mtime_ns, file_hash, seq, cid in files:
        if cid not in chunk_meta:
            chunk_meta[cid] = []
        chunk_meta[cid].append(
            {
                "file_path": path,
                "seq": seq,
                "file_size": size,
                "file_mtime_ns": mtime_ns,
                "file_hash": file_hash,
                "run_id": None,  # Add if versioning needed
            }
        )
    # Find volumes for chunk paths
    vols = {
        row[0]: row[1]
        for row in con.execute(
            "SELECT volume_id, mount_path FROM volumes WHERE status != 'missing'"
        )
    }
    chunk_locs = con.execute(
        "SELECT chunk_id, volume_id FROM chunk_locations"
    ).fetchall()
    loc_map: Dict[str, str] = {cid: vols[vid] for cid, vid in chunk_locs}

    generated = 0
    for cid, metas in chunk_meta.items():
        if cid not in loc_map:
            continue
        mount = Path(loc_map[cid])
        meta_dir = mount / "meta"
        meta_dir.mkdir(parents=True, exist_ok=True)
        json_path = meta_dir / f"{cid}.meta.json"
        with open(json_path, "w") as jf:
            json.dump({"chunk_id": cid, "mappings": metas}, jf, indent=2)
        generated += 1
    con.close()
    print(
        f"Generated {generated} JSON metas in dedicated meta folders—chunks now whisper their file secrets! 🗣️"
    )


def main():
    ap = argparse.ArgumentParser(prog="raidtape-genmeta")
    ap.add_argument("--catalog", required=True, type=Path)
    args = ap.parse_args()
    generate_chunk_jsons(args.catalog)


if __name__ == "__main__":
    main()
