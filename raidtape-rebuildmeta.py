#!/usr/bin/env python3
# GPL v2 License
# (c) 2024 EvilWarning <...> + contributors
# Emergency rebuild from JSON metas + chunks
import argparse, json, os, sqlite3, sys
from pathlib import Path
from typing import Dict, List, Tuple
import time

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


def rebuild_from_jsons(cat: Path, volumes: List[Path]):
    con = connect(cat)
    # Init schema
    with con:
        for stmt in [s for s in CAT_SCHEMA.strip().split(";") if s.strip()]:
            con.execute(stmt)
    # Scan all JSONs across volumes' meta folders
    all_mappings: List[Dict] = []
    chunk_locs: List[
        Tuple[str, str]
    ] = []  # cid, vol_path (fake vol_id as path for simplicity)
    for vol in volumes:
        meta_dir = vol / "meta"
        if not meta_dir.exists():
            continue
        for root, _, files in os.walk(meta_dir):
            for name in files:
                if name.endswith(".meta.json"):
                    cid = name[:-10]  # Strip .meta.json
                    json_p = Path(root) / name
                    with open(json_p, "r") as jf:
                        meta = json.load(jf)
                    all_mappings.extend(meta["mappings"])
                    chunk_locs.append((meta["chunk_id"], str(vol)))
    if not all_mappings:
        print("No metas? Run genmeta first! 🤷")
        return
    # Rebuild volumes (use paths as IDs)
    vol_map: Dict[str, int] = {}  # path -> fake_id
    unique_vols = set(p[1] for p in chunk_locs)
    for i, vpath in enumerate(unique_vols):
        vol_id = f"vol_{i}_{vpath.replace('/', '_')}"
        vol_map[vpath] = i
        with con:
            con.execute(
                "INSERT OR REPLACE INTO volumes(volume_id,mount_path,status,reserve_percent,created_ts) VALUES(?,?,?,?,?)",
                (vol_id, vpath, "active", 10, int(time.time())),
            )

    # Insert chunks/locs
    seen_chunks: set = set()
    for cid, vpath in chunk_locs:
        if cid in seen_chunks:
            continue
        chunk_p = Path(vpath) / "chunks" / cid[:2] / cid[2:4] / cid
        size = chunk_p.stat().st_size if chunk_p.exists() else 0
        with con:
            con.execute(
                "INSERT OR IGNORE INTO chunks(chunk_id,size) VALUES(?,?)", (cid, size)
            )
            vol_id = f"vol_{vol_map[vpath]}_{vpath.replace('/', '_')}"
            con.execute(
                "INSERT OR IGNORE INTO chunk_locations(chunk_id,volume_id) VALUES(?,?)",
                (cid, vol_id),
            )
        seen_chunks.add(cid)

    # Rebuild files (dedup by path)
    path_to_file: Dict[
        str, Tuple[int, int, int, str]
    ] = {}  # path -> (size, mtime, hash, chunks list)
    for mapping in all_mappings:
        path = mapping["file_path"]
        if path not in path_to_file:
            path_to_file[path] = (
                mapping["file_size"],
                mapping["file_mtime_ns"],
                mapping["file_hash"],
                [],
            )
        path_to_file[path][3].append((mapping["seq"], mapping["chunk_id"]))

    for path, (size, mtime_ns, fhash, chunk_list) in path_to_file.items():
        with con:
            con.execute(
                "INSERT OR IGNORE INTO files(path,size,mtime_ns,file_hash) VALUES(?,?,?,?)",
                (path, size, mtime_ns, fhash),
            )
            file_id = con.execute(
                "SELECT rowid FROM files WHERE path=?", (path,)
            ).fetchone()[0]
            for seq, cid in sorted(chunk_list):  # Sort by seq
                con.execute(
                    "INSERT OR IGNORE INTO file_chunks(file_id,seq,chunk_id) VALUES(?,?,?)",
                    (file_id, seq, cid),
                )
    con.close()
    print(
        f"Rebuilt catalog from {len(all_mappings)} mappings—files ready to restore! 🎊"
    )


def main():
    ap = argparse.ArgumentParser(prog="raidtape-rebuildmeta")
    ap.add_argument("--catalog", required=True, type=Path)
    ap.add_argument(
        "--volumes", nargs="+", type=Path, required=True, help="List of volume paths"
    )
    args = ap.parse_args()
    rebuild_from_jsons(args.catalog, args.volumes)


if __name__ == "__main__":
    main()
