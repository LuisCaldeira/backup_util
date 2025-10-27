#!/usr/bin/env python3
# GPL v2 License
# (c) 2024 EvilWarning <...> + contributors
# ZFS Optimized
import argparse, os, sqlite3, sys, time, uuid, shutil, hashlib, random
from pathlib import Path
from typing import Any, Optional, List, Tuple, Dict
from queue import Queue
import queue
import threading
import gc
import psutil
from tqdm import tqdm  # For fun progress bar! 🚀
# ---- external threadpool module (yours) ----
from threadpool import ThreadPool
try:
    import blake3
except ImportError:
    blake3 = None
# ===============================
# Schema & constants
# ===============================
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
CHUNK_BYTES = 128 * 1024 * 1024 # 128 MiB
VOLUME_INDEX_NAME = "chunks.idx.sqlite"
VOLUME_INDEX_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS chunks (
  chunk_id TEXT PRIMARY KEY,
  size INTEGER NOT NULL,
  first_seen_ts INTEGER NOT NULL
);
"""
def check_memory_limit(max_mb: float = 16384): # 16GB
    mem = psutil.Process().memory_info().rss / 1024**2
    while mem > max_mb:
        print(f"Mem: {mem:.1f} MB > {max_mb} MB, pausing...", file=sys.stderr)
        time.sleep(1)
        mem = psutil.Process().memory_info().rss / 1024**2
    return mem
# ===============================
# SQLite helpers
# ===============================
def connect(cat: Path) -> sqlite3.Connection:
    con = sqlite3.connect(str(cat), timeout=30, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA busy_timeout=5000;")
    return con
def _retry_locked(fn, *, retries=6, base_sleep=0.05):
    for i in range(retries):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if "database is locked" not in str(e).lower():
                raise
            time.sleep(base_sleep * (2**i))
    return fn()
def get_hash_algo(con: sqlite3.Connection) -> str:
    row = con.execute("SELECT v FROM settings WHERE k='hash_algo'").fetchone()
    return row[0] if row else "sha256"
def get_reserve_percent(con: sqlite3.Connection) -> int:
    row = con.execute("SELECT v FROM settings WHERE k='reserve_percent'").fetchone()
    return int(row[0]) if row else 10
# ===============================
# Hash helpers
# ===============================
def get_hasher(algo: str):
    if algo == "sha256":
        return hashlib.sha256()
    elif algo == "blake3":
        if blake3 is None:
            raise ImportError("BLAKE3 not installed: pip install blake3")
        return blake3.blake3()
    else:
        raise ValueError(f"Unknown algo: {algo}")
def hash_of(data: bytes, algo: str) -> str:
    h = get_hasher(algo)
    h.update(data)
    return h.hexdigest()
def hash_file(path: Path, algo: str, chunk_size=1024 * 1024) -> str:
    h = get_hasher(algo)
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()
def read_chunk(path: Path, offset: int, size: int) -> bytes:
    with open(path, "rb") as f:
        f.seek(offset)
        return f.read(size)
# ===============================
# Catalog ops
# ===============================
def init_catalog(cat: Path, reserve_percent=10, hash_algo="sha256"):
    cat.parent.mkdir(parents=True, exist_ok=True)
    con = connect(cat)
    with con:
        for stmt in [s for s in CAT_SCHEMA.strip().split(";") if s.strip()]:
            con.execute(stmt)
        con.execute("INSERT OR REPLACE INTO settings(k,v) VALUES(?,?)",
                    ("reserve_percent", str(reserve_percent)))
        con.execute("INSERT OR REPLACE INTO settings(k,v) VALUES(?,?)", ("hash_algo", hash_algo))
    con.close()
    print(f"Catalog initialized at {cat} (reserve {reserve_percent}%, hash {hash_algo}).")
def add_volume(cat: Path, mount_path: Path, reserve_percent=None):
    mount_path.mkdir(parents=True, exist_ok=True)
    (mount_path / "chunks").mkdir(parents=True, exist_ok=True)
    vdb = mount_path / VOLUME_INDEX_NAME
    with sqlite3.connect(str(vdb)) as vcon:
        for stmt in [s for s in VOLUME_INDEX_SCHEMA.strip().split(";") if s.strip()]:
            vcon.execute(stmt)
    con = connect(cat)
    vol_id = str(uuid.uuid4())
    if reserve_percent is None:
        reserve_percent = get_reserve_percent(con)
    with con:
        con.execute(
            "INSERT INTO volumes(volume_id,mount_path,status,reserve_percent,created_ts) VALUES(?,?,?,?,?)",
            (vol_id, str(mount_path), "active", reserve_percent, int(time.time())),
        )
    (mount_path / ".rt_volume_manifest").write_text(f"volume_id={vol_id}\nstatus=active\n")
    con.close()
    print(f"Added volume {vol_id} @ {mount_path}")
def get_active_volume(con: sqlite3.Connection) -> Optional[Tuple[str, str, int]]:
    row = con.execute(
        "SELECT volume_id,mount_path,reserve_percent FROM volumes WHERE status='active' ORDER BY created_ts ASC"
    ).fetchone()
    return row
def free_bytes(path: Path) -> int:
    st = shutil.disk_usage(path)
    return st.free
def ensure_space_or_rotate(con: sqlite3.Connection, volume_id: str, mount_path: Path, needed_bytes: int):
    row = con.execute("SELECT reserve_percent FROM volumes WHERE volume_id=?", (volume_id,)).fetchone()
    reserve_percent = row[0]
    du = shutil.disk_usage(mount_path)
    reserve_abs = du.total * reserve_percent // 100
    if free_bytes(mount_path) - needed_bytes <= reserve_abs:
        with con:
            con.execute("UPDATE volumes SET status='sealed' WHERE volume_id=?", (volume_id,))
        man = mount_path / ".rt_volume_manifest"
        try:
            text = man.read_text().splitlines()
        except FileNotFoundError:
            text = []
        with open(man, "w") as f:
            for line in text:
                if line.startswith("status="):
                    continue
                f.write(line + "\n")
            f.write("status=sealed\n")
        raise RuntimeError(f"Volume {volume_id} sealed (free space below reserve). Please add a new volume.")
def _record_volume_index(mount_path: Path, chunk_id: str, size: int):
    vdb = mount_path / VOLUME_INDEX_NAME
    with sqlite3.connect(str(vdb)) as vcon:
        for stmt in [s for s in VOLUME_INDEX_SCHEMA.strip().split(";") if s.strip()]:
            vcon.execute(stmt)
        vcon.execute("INSERT OR IGNORE INTO chunks(chunk_id,size,first_seen_ts) VALUES(?,?,?)",
                    (chunk_id, size, int(time.time())))
    try:
        with open(mount_path / ".rt_volume_index.jsonl", "a", encoding="utf-8") as j:
            j.write(f'{{"chunk_id":"{chunk_id}","size":{size},"ts":{int(time.time())}}}\n')
    except Exception:
        pass
def _async_write_worker(write_q: queue.Queue, status_q: queue.Queue):
    while True:
        try:
            task = write_q.get(timeout=1)
            if task is None:
                status_q.put("write_worker_done")
                write_q.task_done()
                break
            vol_id, mount_path, cid, payload = task
            status_q.put(f"writing_chunk_start:{cid}")
            if isinstance(payload, bytes):
                data = payload
                sz = len(data)
            elif isinstance(payload, tuple) and len(payload) == 3:
                path, off, sz = payload
                data = read_chunk(Path(path), off, sz)
            else:
                raise ValueError("Invalid payload")
            chunks_dir = mount_path / "chunks" / cid[:2] / cid[2:4]
            chunks_dir.mkdir(parents=True, exist_ok=True)
            target = chunks_dir / cid
            if not target.exists():
                with open(target, "wb") as f:
                    f.write(data)
                _record_volume_index(mount_path, cid, sz)
                time.sleep(0.1)
            status_q.put(f"writing_chunk_done:{cid}")
            write_q.task_done()
        except queue.Empty:
            status_q.put("write_worker_idle")
            continue
        except Exception as e:
            status_q.put(f"write_error:{e}")
            print(f"Write worker error: {e}", file=sys.stderr)
            write_q.task_done()
# ===============================
# Chunk computation
# ===============================
def chunk_file(path: Path, algo: str, chunk_bytes: int, max_buffer_mb: float = 512):
    size = path.stat().st_size
    file_hasher = get_hasher(algo)
    chunk_ids: List[str] = []
    chunk_sizes: List[int] = []
    chunk_offsets: List[int] = []
    chunks_data: List[Optional[bytes]] = []
    pos = 0
    avail_mem_mb = psutil.virtual_memory().available / 1024**2
    mem_buffer_limit = min(size * 0.25, avail_mem_mb * 0.25, max_buffer_mb * 1024 * 1024)
    mem_buffer_limit = int(max(mem_buffer_limit, 1024 * 1024))
    if size <= mem_buffer_limit:
        with path.open("rb") as f:
            full_data = f.read()
        while pos < size:
            end = min(pos + chunk_bytes, size)
            chunk = full_data[pos:end]
            file_hasher.update(chunk)
            cid = hash_of(chunk, algo)
            chunk_ids.append(cid)
            chunk_sizes.append(len(chunk))
            chunk_offsets.append(pos)
            chunks_data.append(chunk)
            pos = end
    else:
        with path.open("rb") as f:
            while True:
                buf = f.read(chunk_bytes)
                if not buf:
                    break
                file_hasher.update(buf)
                cid = hash_of(buf, algo)
                chunk_ids.append(cid)
                chunk_sizes.append(len(buf))
                chunk_offsets.append(pos)
                chunks_data.append(None)
                pos += len(buf)
    chunk_dict = {
        "file_hash": file_hasher.hexdigest(),
        "chunk_ids": chunk_ids,
        "chunk_sizes": chunk_sizes,
        "chunk_offsets": chunk_offsets,
        "chunks_data": chunks_data,
    }
    return chunk_dict
# ===============================
# Co-location async writes
# ===============================
def store_chunks_colocated(
    con: sqlite3.Connection,
    abs_path: Path,
    chunk_dict: Dict[str, Any],
    num_write_workers: int = 4,
    status_q: queue.Queue = None,
):
    row = get_active_volume(con)
    if not row:
        raise RuntimeError("No active volume. Add a volume first.")
    vol_id, mount_str, _ = row
    mount_path = Path(mount_str)
    chunk_ids = chunk_dict["chunk_ids"]
    chunk_sizes = chunk_dict["chunk_sizes"]
    chunk_offsets = chunk_dict["chunk_offsets"]
    chunks_data = chunk_dict["chunks_data"]
    if not chunk_ids:
        if status_q:
            status_q.put("store_chunks_done")
        return
    existing_on_target = set()
    qmarks = ",".join("?" * len(chunk_ids))
    for (cid,) in con.execute(
        f"SELECT chunk_id FROM chunk_locations WHERE volume_id=? AND chunk_id IN ({qmarks})",
        (vol_id, *chunk_ids),
    ):
        existing_on_target.add(cid)
    missing_cids = [cid for cid in chunk_ids if cid not in existing_on_target]
    if not missing_cids:
        if status_q:
            status_q.put("store_chunks_done")
        return
    sizes_dict = dict(zip(chunk_ids, chunk_sizes))
    needed = sum(sizes_dict[cid] for cid in missing_cids)
    ensure_space_or_rotate(con, vol_id, mount_path, needed)
    write_q = queue.Queue()
    workers = []
    for _ in range(num_write_workers):
        t = threading.Thread(target=_async_write_worker, args=(write_q, status_q), daemon=True)
        t.start()
        workers.append(t)
    for i, cid in enumerate(chunk_ids):
        if cid not in missing_cids:
            continue
        off = chunk_offsets[i]
        sz = chunk_sizes[i]
        data = chunks_data[i]
        if data is not None:
            write_q.put((vol_id, mount_path, cid, data))
        else:
            write_q.put((vol_id, mount_path, cid, (str(abs_path), off, sz)))
    write_q.join()
    for _ in range(num_write_workers):
        write_q.put(None)
    for t in workers:
        t.join()
    def _db_write():
        with con:
            con.executemany(
                "INSERT OR IGNORE INTO chunks(chunk_id,size) VALUES(?,?)",
                [(cid, sizes_dict[cid]) for cid in missing_cids],
            )
            con.executemany(
                "INSERT OR IGNORE INTO chunk_locations(chunk_id,volume_id) VALUES(?,?)",
                [(cid, vol_id) for cid in missing_cids],
            )
            con.commit()  # Explicit commit
    _retry_locked(_db_write)
    if status_q:
        status_q.put("store_chunks_db_done")
# ===============================
# Worker job: compute file hash & chunk ids
# ===============================
def _compute_file_job(
    src_path: str,
    source_dir: str,
    known_meta: Dict[str, Tuple[str, int, int, str]],
    algo: str,
    chunk_bytes: int,
    max_buffer_mb: float = 512,
    status_q: queue.Queue = None,
):
    p = Path(src_path)
    rel = str(p.relative_to(Path(source_dir)))
    if status_q:
        status_q.put(f"hashing_start:{rel}")
    st = p.stat()
    size = st.st_size
    mtime_ns = st.st_mtime_ns
    prev = known_meta.get(rel)
    if prev and prev[1] == size and prev[2] == mtime_ns:
        if status_q:
            status_q.put(f"hashing_unchanged:{rel}")
        return {"kind": "unchanged", "rel": rel, "size": size, "mtime_ns": mtime_ns}
    chunk_dict = chunk_file(p, algo, chunk_bytes, max_buffer_mb)
    chunk_dict.update(
        {
            "kind": "changed",
            "rel": rel,
            "size": size,
            "mtime_ns": mtime_ns,
            "abs_path": str(p),
        }
    )
    if status_q:
        status_q.put(f"hashing_done:{rel}")
    return chunk_dict
# ===============================
# Single-threaded legacy backup
# ===============================
def scan_and_backup_singlethread(cat: Path, source_dir: Path):
    con = connect(cat)
    algo = get_hash_algo(con)
    with con:
        con.execute("INSERT INTO runs(started_ts) VALUES(?)", (int(time.time()),))
        run_id = con.execute("SELECT last_insert_rowid()").fetchone()[0]
    all_paths = []
    for root, _, files in os.walk(source_dir):
        for name in files:
            all_paths.append(Path(root) / name)
    total_files = len(all_paths)
    known = {
        row[0]: row
        for row in con.execute("SELECT path,size,mtime_ns,file_hash FROM files")
    }
    seen = set()
    start_time = time.time()
    for i, p in enumerate(all_paths):
        rel = str(p.relative_to(source_dir))
        seen.add(rel)
        st = p.stat()
        size, mtime_ns = st.st_size, st.st_mtime_ns
        prev = known.get(rel)
        if prev and prev[1] == size and prev[2] == mtime_ns:
            with con:
                con.execute(
                    "INSERT OR REPLACE INTO run_changes(run_id,path,change) VALUES(?,?,?)",
                    (run_id, rel, "unchanged"),
                )
        else:
            chunk_dict = chunk_file(p, algo, CHUNK_BYTES, 512)
            store_chunks_colocated(con, p, chunk_dict, num_write_workers=1)
            file_hash = chunk_dict["file_hash"]
            chunk_ids = chunk_dict["chunk_ids"]
            with con:
                if prev:
                    file_id = con.execute("SELECT rowid FROM files WHERE path=?", (rel,)).fetchone()[0]
                    con.execute("UPDATE files SET size=?, mtime_ns=?, file_hash=? WHERE path=?",
                                (size, mtime_ns, file_hash, rel))
                    con.execute("DELETE FROM file_chunks WHERE file_id=?", (file_id,))
                    change = "modified"
                else:
                    con.execute("INSERT INTO files(path,size,mtime_ns,file_hash) VALUES(?,?,?,?)",
                                (rel, size, mtime_ns, file_hash))
                    file_id = con.execute("SELECT last_insert_rowid()").fetchone()[0]
                    change = "added"
                for seq, cid in enumerate(chunk_ids):
                    con.execute("INSERT INTO file_chunks(file_id,seq,chunk_id) VALUES(?,?,?)",
                                (file_id, seq, cid))
                con.execute(
                    "INSERT OR REPLACE INTO file_versions(file_id,run_id,path,size,mtime_ns,file_hash) VALUES(?,?,?,?,?,?)",
                    (file_id, run_id, rel, size, mtime_ns, file_hash),
                )
                con.executemany(
                    "INSERT OR REPLACE INTO file_chunk_history(file_id,run_id,seq,chunk_id) VALUES(?,?,?,?)",
                    ((file_id, run_id, seq, cid) for seq, cid in enumerate(chunk_ids)),
                )
                con.execute(
                    "INSERT OR REPLACE INTO run_changes(run_id,path,change) VALUES(?,?,?)",
                    (run_id, rel, change),
                )
                con.commit()  # Explicit commit
        elapsed = time.time() - start_time
        processed = i + 1
        pct = (processed / total_files) * 100 if total_files else 100.0
        eta = int((total_files - processed) / (processed / elapsed)) if processed > 1 else 0
        sys.stdout.write(
            f"\rScanning files: {processed}/{total_files} ({pct:.1f}%) | Elapsed: {int(elapsed)}s | ETA: {eta}s"
        )
        sys.stdout.flush()
    print()
    for rel in set(known.keys()) - seen:
        with con:
            con.execute("INSERT OR REPLACE INTO run_changes(run_id,path,change) VALUES(?,?,?)",
                        (run_id, rel, "deleted"))
            con.commit()
    with con:
        con.execute("UPDATE runs SET finished_ts=? WHERE run_id=?", (int(time.time()), run_id))
        con.commit()
    rows = con.execute("SELECT change, COUNT(*) FROM run_changes WHERE run_id=? GROUP BY change",
                      (run_id,)).fetchall()
    summary = dict(rows)
    con.close()
    print(f"Backup run {run_id} complete. Incremental stats: {summary} 🎉")
    for change in ("added", "modified", "deleted"):
        count = summary.get(change, 0)
        if count > 0:
            print(f"\n== {change.upper()} ({count}) ==")
            for (pth,) in sqlite3.connect(str(cat)).execute(
                "SELECT path FROM run_changes WHERE run_id=? AND change=? ORDER BY path LIMIT 5",
                (run_id, change),
            ):
                print(f" {pth}")
            if count > 5:
                print(f" ... and {count - 5} more")
# ===============================
# Multithreaded backup
# ===============================
def _progress_monitor(status_q: Queue, results_q: Queue, total_files: int, done_event: threading.Event, num_write_workers: int):
    pbar = tqdm(total=total_files, desc="Hashing files", dynamic_ncols=True, colour="GREEN")
    hashed = 0
    writes_done = 0
    db_writes_done = 0
    write_workers_done = 0
    errors = []
    timeout_start = None
    max_timeout = 60
    while not done_event.is_set():
        try:
            msg = status_q.get(timeout=0.5)
            if msg == "all_hashing_done":
                pbar.set_description("All hashing done! Wrapping writes & DB... 🛠️")
            elif msg.startswith("hashing_start:"):
                pass
            elif msg.startswith("hashing_done:") or msg.startswith("hashing_unchanged:"):
                hashed += 1
                pbar.update(1)
            elif msg.startswith("writing_chunk_done:"):
                writes_done += 1
            elif msg == "write_worker_done":
                write_workers_done += 1
            elif msg == "store_chunks_db_done":
                db_writes_done += 1
            elif msg == "write_worker_idle":
                pass
            elif msg.startswith("write_error:") or msg.startswith("hash_error:") or msg.startswith("store_error:"):
                errors.append(msg)
            elif msg.startswith("processed_file:"):
                pass
            # Check completion
            if (hashed >= total_files and write_workers_done >= num_write_workers and
                results_q.qsize() == 0 and status_q.qsize() == 0):
                done_event.set()
                pbar.set_description("Process complete! 🚀")
            status_q.task_done()
            timeout_start = None
        except queue.Empty:
            if hashed >= total_files and timeout_start is None:
                timeout_start = time.time()
            if timeout_start and (time.time() - timeout_start) > max_timeout:
                print("Timeout: No activity for 60s, forcing completion!", file=sys.stderr)
                done_event.set()
                pbar.set_description("Timed out, forcing complete! ⏰")
            continue
        pbar.set_postfix({
            "res_q": results_q.qsize(),
            "stat_q": status_q.qsize(),
            "writes": writes_done,
            "db_writes": db_writes_done,
            "writers": f"{write_workers_done}/{num_write_workers}"
        })
    pbar.close()
    if errors:
        print(f"Errors: {errors}", file=sys.stderr)
    print(f"Process complete! {hashed}/{total_files} files, {writes_done} chunks, {db_writes_done} DB writes. 🎉")

def scan_and_backup_threaded(cat: Path, source_dir: Path, max_workers: int, max_write_workers: int = 2):
    max_workers = int(max_workers)
    max_write_workers = int(max_write_workers)
    con = connect(cat)
    algo = get_hash_algo(con)
    with con:
        con.execute("INSERT INTO runs(started_ts) VALUES(?)", (int(time.time()),))
        run_id = con.execute("SELECT last_insert_rowid()").fetchone()[0]
        con.commit()
    all_paths = []
    for root, _, files in os.walk(source_dir):
        for name in files:
            all_paths.append(Path(root) / name)
    total_files = len(all_paths)
    known = {
        row[0]: row
        for row in con.execute("SELECT path,size,mtime_ns,file_hash FROM files")
    }
    con.close()
    status_q = Queue()
    results_q = Queue()
    params = {"DEBUG_MODE": False}
    tp = ThreadPool(
        num_threads=max_workers,
        max_threads=max_workers,
        min_threads=max_workers,
        exception_handler=lambda e: status_q.put(f"hash_error:{e}"),
        name="raidtape_pool",
        monitor_interval=2,
        params=params,
    )
    done_event = threading.Event()
    monitor_t = threading.Thread(
        target=_progress_monitor,
        args=(status_q, results_q, total_files, done_event, max_write_workers),
        daemon=True
    )
    monitor_t.start()
    submitted = 0
    for p in all_paths:
        mem = check_memory_limit()
        tp.add_task(
            _compute_file_job,
            results_q.put,
            str(p),
            str(source_dir),
            known,
            algo,
            CHUNK_BYTES,
            512,
            status_q,
        )
        submitted += 1
    conw = connect(cat)
    processed = 0
    seen = set()
    batch_changes: List[Tuple[int, str, str]] = []
    BATCH_SIZE = 150
    def flush_batch_changes():
        if not batch_changes:
            return
        def _do():
            with conw:
                conw.executemany(
                    "INSERT OR REPLACE INTO run_changes(run_id,path,change) VALUES(?,?,?)",
                    batch_changes,
                )
                conw.commit()  # Explicit commit
                if status_q:
                    status_q.put("batch_changes_db_done")
        _retry_locked(_do)
        batch_changes.clear()
    def record_versioning(file_id: int, rel: str, size: int, mtime_ns: int, file_hash: str, chunk_ids: List[str]):
        def _do():
            with conw:
                conw.execute(
                    "INSERT OR REPLACE INTO file_versions(file_id,run_id,path,size,mtime_ns,file_hash) VALUES(?,?,?,?,?,?)",
                    (file_id, run_id, rel, size, mtime_ns, file_hash),
                )
                conw.executemany(
                    "INSERT OR REPLACE INTO file_chunk_history(file_id,run_id,seq,chunk_id) VALUES(?,?,?,?)",
                    ((file_id, run_id, seq, cid) for seq, cid in enumerate(chunk_ids)),
                )
                conw.commit()
                if status_q:
                    status_q.put("versioning_db_done")
        _retry_locked(_do)
    while processed < submitted:
        try:
            res = results_q.get(timeout=300)
            processed += 1
            mem = check_memory_limit()
            status_q.put(f"processed_file:{processed}/{submitted}")
            if res.get("kind") == "unchanged":
                rel = res["rel"]
                seen.add(rel)
                batch_changes.append((run_id, rel, "unchanged"))
                if len(batch_changes) >= BATCH_SIZE:
                    flush_batch_changes()
                continue
            rel = res["rel"]
            seen.add(rel)
            abs_path = Path(res["abs_path"])
            size = res["size"]
            mtime_ns = res["mtime_ns"]
            file_hash = res["file_hash"]
            chunk_ids = res["chunk_ids"]
            try:
                store_chunks_colocated(conw, abs_path, res, num_write_workers=max_write_workers, status_q=status_q)
            except RuntimeError as e:
                status_q.put(f"store_error:{e}")
                print(f"\nERROR {rel}: {e}", file=sys.stderr)
                tp.shutdown()
                gc.collect()
                conw.close()
                done_event.set()
                monitor_t.join()
                sys.exit(1)
            def _upsert_file_and_chunks():
                with conw:
                    prev = conw.execute("SELECT file_id,size,mtime_ns,file_hash FROM files WHERE path=?", (rel,)).fetchone()
                    if prev:
                        file_id = prev[0]
                        conw.execute("UPDATE files SET size=?, mtime_ns=?, file_hash=? WHERE path=?",
                                    (size, mtime_ns, file_hash, rel))
                        conw.execute("DELETE FROM file_chunks WHERE file_id=?", (file_id,))
                        change = "modified"
                    else:
                        conw.execute("INSERT INTO files(path,size,mtime_ns,file_hash) VALUES(?,?,?,?)",
                                    (rel, size, mtime_ns, file_hash))
                        file_id = conw.execute("SELECT last_insert_rowid()").fetchone()[0]
                        change = "added"
                    conw.executemany(
                        "INSERT INTO file_chunks(file_id,seq,chunk_id) VALUES(?,?,?)",
                        [(file_id, seq, cid) for seq, cid in enumerate(chunk_ids)],
                    )
                    conw.commit()
                    if status_q:
                        status_q.put("file_chunks_db_done")
                return file_id, change
            file_id, change = _retry_locked(_upsert_file_and_chunks)
            record_versioning(file_id, rel, size, mtime_ns, file_hash, chunk_ids)
            batch_changes.append((run_id, rel, change))
            if len(batch_changes) >= BATCH_SIZE:
                flush_batch_changes()
        except queue.Empty:
            status_q.put("results_queue_timeout")
            print("Results queue timeout, checking completion...", file=sys.stderr)
            if processed >= submitted:
                break
    status_q.put("all_hashing_done")
    flush_batch_changes()
    tp.wait_completion()
    tp.shutdown()
    gc.collect()
    conw2 = connect(cat)
    known_now = {row[0] for row in conw2.execute("SELECT path FROM files")}
    for rel in known.keys() - seen:
        def _do():
            with conw2:
                conw2.execute("INSERT OR REPLACE INTO run_changes(run_id,path,change) VALUES(?,?,?)",
                             (run_id, rel, "deleted"))
                conw2.commit()
                if status_q:
                    status_q.put("deleted_db_done")
        _retry_locked(_do)
    with conw2:
        conw2.execute("UPDATE runs SET finished_ts=? WHERE run_id=?", (int(time.time()), run_id))
        conw2.commit()
        if status_q:
            status_q.put("run_update_db_done")
    rows = conw2.execute("SELECT change, COUNT(*) FROM run_changes WHERE run_id=? GROUP BY change",
                        (run_id,)).fetchall()
    summary = dict(rows)
    conw2.close()
    conw.close()
    done_event.set()
    monitor_t.join()
    print(f"Backup run {run_id} complete. Incremental stats: {summary} 🎉")
    for change in ("added", "modified", "deleted"):
        count = summary.get(change, 0)
        if count > 0:
            print(f"\n== {change.upper()} ({count}) ==")
            for (pth,) in sqlite3.connect(str(cat)).execute(
                "SELECT path FROM run_changes WHERE run_id=? AND change=? ORDER BY path LIMIT 5",
                (run_id, change),
            ):
                print(f" {pth}")
            if count > 5:
                print(f" ... and {count - 5} more")
# ===============================
# Reporting & validation
# ===============================
def list_changes(cat: Path, run_id: int):
    con = connect(cat)
    rows = con.execute("SELECT change, COUNT(*) FROM run_changes WHERE run_id=? GROUP BY change",
                      (run_id,)).fetchall()
    print("Summary:", dict(rows))
    for change in ("added", "modified", "deleted"):
        print(f"\n== {change.upper()} ==")
        for (p,) in con.execute("SELECT path FROM run_changes WHERE run_id=? AND change=? ORDER BY path",
                               (run_id, change)):
            print(p)
    con.close()
def validate_blocks(cat: Path):
    con = connect(cat)
    algo = get_hash_algo(con)
    chunks = con.execute("SELECT chunk_id, size FROM chunks").fetchall()
    total = len(chunks)
    good, bad = 0, 0
    start_time = time.time()
    for i, (cid, expected_size) in enumerate(chunks, 1):
        loc = con.execute(
            """
            SELECT mount_path
            FROM volumes v
            JOIN chunk_locations cl ON v.volume_id=cl.volume_id
            WHERE cl.chunk_id=? AND v.status!='missing'
            LIMIT 1
            """,
            (cid,),
        ).fetchone()
        if not loc:
            bad += 1
            print(f"Missing: {cid}")
        else:
            mount = Path(loc[0])
            path = mount / "chunks" / cid[:2] / cid[2:4] / cid
            if not path.exists():
                bad += 1
                print(f"Not found on disk: {cid}")
            else:
                actual_size = path.stat().st_size
                if actual_size != expected_size:
                    bad += 1
                    print(f"Size mismatch {cid}: expected {expected_size}, got {actual_size}")
                else:
                    with open(path, "rb") as f:
                        data = f.read()
                    computed = hash_of(data, algo)
                    if computed != cid:
                        bad += 1
                        print(f"Hash mismatch {cid}: expected {cid}, got {computed}")
                    else:
                        good += 1
        elapsed = time.time() - start_time
        percent = (i / total) * 100 if total else 100.0
        eta_secs = int((total - i) / (i / elapsed)) if i > 1 and elapsed > 0 else 0
        eta_str = f"{eta_secs}s" if i > 1 else "N/A"
        status = f"Validating chunks: {i}/{total} ({percent:.1f}%) | Elapsed: {int(elapsed)}s | ETA: {eta_str} | Bad: {bad}"
        sys.stdout.write(f"\r{status}")
        sys.stdout.flush()
    print()
    con.close()
    print(f"Validation: {good} good, {bad} bad chunks. Your blocks are {'' if bad == 0 else 'mostly'} solid! 💪")
# ===============================
# Restore & Revert
# ===============================
def _restore_file(con: sqlite3.Connection, src_path: str, dest_dir: Path, algo: str):
    row = con.execute("SELECT file_id,size,file_hash FROM files WHERE path=?", (src_path,)).fetchone()
    if not row:
        print(f"Skipped: {src_path} (not in catalog)")
        return False
    file_id, size, fhash = row
    chunks = list(con.execute("SELECT seq,chunk_id FROM file_chunks WHERE file_id=? ORDER BY seq", (file_id,)))
    rel_path = Path(src_path)
    dest_path = dest_dir / rel_path
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Restoring: {src_path} -> {dest_path}")
    try:
        with open(dest_path, "wb") as out:
            for _, cid in chunks:
                vrow = con.execute(
                    """
                    SELECT v.mount_path
                    FROM chunk_locations cl
                    JOIN volumes v ON v.volume_id=cl.volume_id
                    WHERE cl.chunk_id=? AND v.status!='missing'
                    LIMIT 1
                    """,
                    (cid,),
                ).fetchone()
                if not vrow:
                    raise RuntimeError(f"Chunk {cid} missing!")
                mount = Path(vrow[0])
                chunk_path = mount / "chunks" / cid[:2] / cid[2:4] / cid
                with open(chunk_path, "rb") as f:
                    out.write(f.read())
        if hash_file(dest_path, algo) != fhash:
            raise RuntimeError("Hash fail!")
        print(f"✓ Done: {src_path}")
        return True
    except Exception as e:
        print(f"✗ Failed {src_path}: {e}")
        return False
def restore_file(cat: Path, src_file: str, dest: Path):
    con = connect(cat)
    algo = get_hash_algo(con)
    success = _restore_file(con, src_file, Path(dest).parent, algo)
    con.close()
    if not success:
        sys.exit(1)
    print(f"Restored single file to {dest}")
def restore_folder(cat: Path, src_folder: str, dest_dir: Path):
    con = connect(cat)
    algo = get_hash_algo(con)
    prefix = src_folder.rstrip("/") + "/"
    files = con.execute(
        "SELECT path FROM files WHERE path LIKE ? ESCAPE '\\' ORDER BY path",
        (prefix.replace("\\", "\\\\").replace("%", "\\%") + "%",),
    ).fetchall()
    print(f"Found {len(files)} files in {src_folder} for restore.")
    good, bad = 0, 0
    total = len(files)
    dest_dir.mkdir(parents=True, exist_ok=True)
    start_time = time.time()
    for i, (sp,) in enumerate(files):
        if _restore_file(con, sp, dest_dir, algo):
            good += 1
        else:
            bad += 1
        elapsed = time.time() - start_time
        processed = i + 1
        percent = (processed / total) * 100 if total else 100.0
        eta = int((total - processed) / (processed / elapsed)) if processed > 1 else 0
        sys.stdout.write(
            f"\rRestoring files: {processed}/{total} ({percent:.1f}%) | Elapsed: {int(elapsed)}s | ETA: {eta}s | Good: {good} | Bad: {bad}"
        )
        sys.stdout.flush()
    print()
    con.close()
    print(f"Folder restore wrap: {good} good, {bad} borked. Party's on! 🎉")
def revert_to_run(cat: Path, run_id: int, dest_root: Path):
    con = connect(cat)
    algo = get_hash_algo(con)
    dest_root.mkdir(parents=True, exist_ok=True)
    rows = con.execute(
        """
        SELECT fv.file_id, fv.path, fv.size, fv.file_hash
        FROM file_versions fv
        WHERE fv.run_id=?
        ORDER BY fv.path
        """,
        (run_id,),
    ).fetchall()
    print(f"Reverting {len(rows)} files from run {run_id} into {dest_root}")
    good = bad = 0
    start = time.time()
    for file_id, path, size, fhash in rows:
        chunks = list(
            con.execute(
                """
                SELECT seq, chunk_id
                FROM file_chunk_history
                WHERE file_id=? AND run_id=?
                ORDER BY seq
                """,
                (file_id, run_id),
            )
        )
        out_path = dest_root / path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(out_path, "wb") as out:
                for _, cid in chunks:
                    vrow = con.execute(
                        """
                        SELECT v.mount_path
                        FROM chunk_locations cl
                        JOIN volumes v ON v.volume_id=cl.volume_id
                        WHERE cl.chunk_id=? AND v.status!='missing'
                        LIMIT 1
                        """,
                        (cid,),
                    ).fetchone()
                    if not vrow:
                        raise RuntimeError(f"Missing chunk {cid}")
                    mount = Path(vrow[0])
                    chunk_path = mount / "chunks" / cid[:2] / cid[2:4] / cid
                    with open(chunk_path, "rb") as f:
                        out.write(f.read())
            if hash_file(out_path, algo) != fhash:
                raise RuntimeError("Hash mismatch after rebuild")
            good += 1
        except Exception as e:
            bad += 1
            print(f"✗ {path}: {e}")
        done = good + bad
        if done % 50 == 0 or done == len(rows):
            elapsed = int(time.time() - start)
            print(f"... {done}/{len(rows)} done in {elapsed}s (ok={good}, bad={bad})")
    con.close()
    print(f"Revert complete. Good: {good}, Bad: {bad}")
# ===============================
# Rebuild catalog
# ===============================
def rebuild_catalog(cat: Path):
    con = connect(cat)
    vols = con.execute("SELECT volume_id, mount_path FROM volumes WHERE status!='missing'").fetchall()
    added = 0
    for vid, mnt in vols:
        mnt = Path(mnt)
        vdb = mnt / VOLUME_INDEX_NAME
        if vdb.exists():
            with sqlite3.connect(str(vdb)) as vcon:
                rows = vcon.execute("SELECT chunk_id, size FROM chunks").fetchall()
                def _do():
                    with con:
                        for cid, size in rows:
                            con.execute("INSERT OR IGNORE INTO chunks(chunk_id,size) VALUES(?,?)", (cid, size))
                            con.execute("INSERT OR IGNORE INTO chunk_locations(chunk_id,volume_id) VALUES(?,?)", (cid, vid))
                        con.commit()
                _retry_locked(_do)
                added += len(rows)
        else:
            root = mnt / "chunks"
            if not root.exists():
                continue
            for r, _, files in os.walk(root):
                for name in files:
                    cid = name
                    p = Path(r) / name
                    size = p.stat().st_size
                    def _do():
                        with con:
                            con.execute("INSERT OR IGNORE INTO chunks(chunk_id,size) VALUES(?,?)", (cid, size))
                            con.execute("INSERT OR IGNORE INTO chunk_locations(chunk_id,volume_id) VALUES(?,?)", (cid, vid))
                            con.commit()
                    _retry_locked(_do)
                    added += 1
    con.close()
    print(f"Rebuild complete. Registered/confirmed {added} chunk locations.")
# ===============================
# CLI
# ===============================
def main():
    ap = argparse.ArgumentParser(prog="raidtape")
    ap.add_argument("--catalog", required=True, type=Path)
    sp = ap.add_subparsers(dest="cmd", required=True)
    p_init = sp.add_parser("init", help="Initialize catalog database")
    p_init.add_argument("--reserve", type=int, default=10)
    p_init.add_argument("--hash-algo", choices=["sha256", "blake3"], default="blake3")
    p_addv = sp.add_parser("add-volume", help="Register a new backup volume")
    p_addv.add_argument("mount", type=Path)
    p_addv.add_argument("--reserve", type=int)
    p_backup = sp.add_parser("backup", help="Run backup over a source directory")
    p_backup.add_argument("source", type=Path)
    p_backup.add_argument("--max-workers", type=int, default=8, help="Worker threads for hashing (default: 12)")
    p_backup.add_argument("--max-write-workers", type=int, default=4, help="Worker threads for writing chunks (default: 8)")
    p_backup.add_argument("--single-threaded", action="store_true", help="Use the legacy single-threaded backup path")
    p_changes = sp.add_parser("list-changes", help="List changes for a run")
    p_changes.add_argument("run_id", type=int)
    sp.add_parser("validate", help="Validate all chunks on disk")
    p_restore_file = sp.add_parser("restore", help="Restore a single file to a destination path")
    p_restore_file.add_argument("src_file", type=str)
    p_restore_file.add_argument("dest", type=Path)
    p_restore_folder = sp.add_parser("restore-folder", help="Restore an entire folder subtree")
    p_restore_folder.add_argument("src_folder", type=str)
    p_restore_folder.add_argument("dest_dir", type=Path)
    p_revert = sp.add_parser("revert-to-run", help="Materialize files as of a specific run into a dest directory")
    p_revert.add_argument("run_id", type=int)
    p_revert.add_argument("dest_root", type=Path)
    sp.add_parser("rebuild-catalog", help="Rebuild chunks & locations from per-volume indexes")
    args = ap.parse_args()
    cat = args.catalog
    if args.cmd == "init":
        init_catalog(cat, args.reserve, args.hash_algo)
    elif args.cmd == "add-volume":
        add_volume(cat, args.mount, args.reserve)
    elif args.cmd == "backup":
        if getattr(args, "single_threaded", False):
            scan_and_backup_singlethread(cat, args.source)
        else:
            scan_and_backup_threaded(cat, args.source, args.max_workers, args.max_write_workers)
    elif args.cmd == "list-changes":
        list_changes(cat, args.run_id)
    elif args.cmd == "validate":
        validate_blocks(cat)
    elif args.cmd == "restore":
        restore_file(cat, args.src_file, args.dest)
    elif args.cmd == "restore-folder":
        restore_folder(cat, args.src_folder, args.dest_dir)
    elif args.cmd == "revert-to-run":
        revert_to_run(cat, args.run_id, args.dest_root)
    elif args.cmd == "rebuild-catalog":
        rebuild_catalog(cat)
if __name__ == "__main__":
    main()
