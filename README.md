
````markdown
# ⚡ RaidTape Suite 🚀  

> **ZFS-vibed backup beast** — chunks files into deduped **128 MiB blocks**, spreads across volumes, versions runs, and whispers metadata secrets.  
> Like **RAID on steroids** meets **tape archives** — fast, fun, and *“oh snap, my data’s safe”* reliable.  
> Handles big dirs, low mem, and disaster recovery like a pro.

---

## 🧩 Why RaidTape?

- **Dedup saves space**
- **Incrementals** track changes  
- **Restores** rewind time  
- **Validations** ensure integrity  

No more data ghosts — *scan, store, and spill the tea on chunks!* ☕

---

## 🧰 The Trio: Tools in Sync

| Tool | Role |
|------|------|
| 🧠 `raidtape.py` | Core backup/restore engine — scans, hashes, stores, reverts |
| 💬 `raidtape-genmeta.py` | Post-backup chunk gossip — JSON metas spill file mappings |
| ☠️ `raidtape-rebuildmeta.py` | Emergency resurrection — rebuilds catalog from metas + chunks |

🪪 **License:** GPL v2 — © 2024 *EvilWarning & crew*  
🧪 *Fork it, tweak it, break it, fix it.*

---

## ⚙️ Quick Setup

**Dependencies:**
```bash
pip install psutil tqdm blake3
````

*(blake3 optional; sha256 fallback)*

**Run help:**

```bash
python raidtape.py --help
```

All commands require:

```bash
--catalog /path/to/backup.cat   # SQLite brain 🧠
```

---

## 🚀 Blast-Off Guide

### 🧱 1️⃣ Init & Volumes (`raidtape.py`)

```bash
raidtape.py init --catalog ~/raid.cat --reserve 10 --hash-algo blake3
```

Boots the DB.
Reserve % guards space; **blake3** zips faster than **sha256**.

```bash
raidtape.py add-volume --catalog ~/raid.cat /mnt/bigdisk --reserve 5
```

Adds a volume — auto-makes `/chunks` & index DB.
Seals at low space → rotate volumes like a boss.

---

### 🌀 2️⃣ Backup Magic (`raidtape.py`)

```bash
raidtape.py backup --catalog ~/raid.cat /home/me/stuff \
  --max-workers 8 --max-write-workers 4
```

Walks dirs, chunks/hashes in threads, dedups to active volume,
logs run changes (added / modified / deleted / unchanged).

Progress-bar party! 📊

Use `--single-threaded` for chill debugging.
Spits **run ID + stats**.

> 💡 *Pro tip:* Volumes auto-seal when tight — add new ones pronto.

---

### 🔍 3️⃣ Peek & Validate

```bash
raidtape.py list-changes --catalog ~/raid.cat 42
```

Run 42’s flip summary + file lists — quick change autopsy.

```bash
raidtape.py validate --catalog ~/raid.cat
```

Hashes all chunks → sizes, existence, integrity.
**“Solid! 💪”** or **“Fix me!”**

---

### 🧯 4️⃣ Restore Shenanigans

```bash
raidtape.py restore --catalog ~/raid.cat docs/readme.md /tmp/save/readme.md
```

Pulls one file, reassembles chunks, hash-checks — boom, back! 💥

```bash
raidtape.py restore-folder --catalog ~/raid.cat docs/ /tmp/full-docs
```

Grabs subtree — progress + tally. Logs bad boys if found.

```bash
raidtape.py revert-to-run --catalog ~/raid.cat 42 /tmp/timewarp
```

Rebuilds all files from **run 42** — snapshot rewind, baby! 🕰️

---

### 🗣️ 5️⃣ Chunk Gossip (`raidtape-genmeta.py`)

```bash
raidtape-genmeta.py --catalog ~/raid.cat
```

Post-backup: scans DB, dumps
`volume/meta/chunkid.meta.json` with file mappings (path, seq, size, mtime, hash).

Why? Forensics fun — see dedup drama across files.

> 🧾 *“Generated X metas — chunks spilling secrets!”*

---

### 🧟 6️⃣ Disaster Rebuild (`raidtape-rebuildmeta.py`)

```bash
raidtape-rebuildmeta.py --catalog ~/raid.cat --volumes /mnt/disk1 /mnt/disk2
```

DB nuked? No sweat — scans metas + chunks, rebuilds files/chunks/vols.
Fake vol IDs from paths.

> “Rebuilt from X mappings — restore party on! 🎊”

Use when: 🧨 *catalog crash* → run this, then restore away.

---

### 💣 7️⃣ Nuke & Rebuild (`raidtape.py`)

```bash
raidtape.py rebuild-catalog --catalog ~/raid.cat
```

DB funky? Rescan volumes’ indexes / chunks, re-register.
Safety net engaged. 🛡️

---

## 🕵️ Sneaky Hacks

* 🧩 **Chunk Path:** `/chunks/xx/yy/hash` (first 4 hex)
* ⚙️ **Default Size:** 128 MiB (`CHUNK_BYTES`)
* 🧠 **Mem Cap:** ~16 GB — throttles when hot (`check_memory_limit()`)
* 🕰️ **Run Intel:** query `runs` for timestamps, `file_versions` for history
* 🔒 **Locks:** WAL + retries = chill multi-run vibes
* 🧵 **Scale:** Threads for hash/write, batched DB ops — big dirs = crank workers

---

<p align="center">
  <b>✨ RaidTape — Because “backup” shouldn’t feel like 1998. ✨</b>
</p>
```

---

Would you like me to add **syntax-colored highlights** (teal/orange accent boxes for CLI vs tips) — i.e., make it look like a *fintech-grade README visual* with colored `<div>` blocks and icons for Confluence?
