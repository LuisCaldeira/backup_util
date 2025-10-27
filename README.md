RaidTape 🚀

ZFS-inspired backup beast: chunks files into 128MiB deduped blocks, spreads 'em across volumes, and versions everything. 
Like RAID meets tape, but faster and funner. No more "where's my data?" panic—validate it all!
Why Bother?

Deduplication magic: Same chunk? Stored once. Saves space like a boss.
Incremental vibes: Tracks changes per run, reverts to any snapshot.
Threaded turbo: Hash in parallel, async writes—blitzes your dirs.
Volume juggling: Auto-seals full disks, rotates to new ones.
Restore roulette: Grab files/folders or rewind to old runs.
Validation party: Hashes & checks blocks so nothing sneaks bad data.

Built for big files, low mem (smart buffering), and chill admins.
Setup Shenanigans

Deps: pip install psutil tqdm blake3 (blake3 optional—defaults to sha256).

Needs a custom threadpool module (grab from contribs or roll your own).


Run it: python raidtape.py (or alias to raidtape).

Usage Blast-Off
Prefix all with --catalog /path/to/backup.cat (SQLite db for the magic).
Core Commands

init --reserve 10 --hash-algo blake3
Kick off the catalog. Reserve % keeps space free; blake3 > sha256 for speed.
add-volume /mnt/mybigdisk --reserve 5
Mount a volume (makes /chunks dirs). Auto-creates index db.
backup /home/me/stuff --max-workers 8 --max-write-workers 4
Scans, hashes, chunks, dedups, writes. --single-threaded for debug vibes.
Spits run ID & change stats (added/modified/deleted). Progress bar for the win! 📊
list-changes 42
Peek at what flipped in run 42. Counts + lists files.
validate
Scans all chunks: hashes, sizes, existence. "All good" or "Uh oh, bad boy detected."
restore docs/readme.md /tmp/recover/readme.md
Pulls one file from catalog to dest. Hashes to confirm—boom, restored.
restore-folder docs/ /tmp/full-restore
Grabs whole subtree. Progress + success tally.
revert-to-run 42 /tmp/as-of-42
Rebuilds every file from that run's snapshot. Time machine style.
rebuild-catalog
If db's funky (crashes?), rescan volumes & re-register chunks.

Sneaky Tips

Space dance: Volumes seal at reserve threshold—add new ones quick!
Chunk life: Stored as hash files in /chunks/xx/yy/hash (first 4 hex chars).
Mem chill: Caps at ~16GB, pauses if hot. Tweak check_memory_limit().
Runs table: Query runs for timestamps; file_versions for history.
Errors?: WAL mode handles locks; retries on busy dbs.

RaidTape-GenMeta 🗂️

Post-backup whisperer: Spits JSON metas for each chunk, spilling which files it's glued into. Turns silent blocks into chatty storytellers—perfect for forensics, audits, or just geeking on dedup drama.
Why Whisper?

Chunk confessions: Each .meta.json lists files using that chunk (path, offset, hashes—full tea).
Volume vibes: Dumps 'em in /meta/ on each disk, no hunting required.
Quick & dirty: One-shot run after backups; scales with your catalog.

Pairs with RaidTape—run post-backup to unlock chunk-level intel. Fun fact: Spot reuse across files like a dedup detective! 🔍
Setup Zip
Same deps as RaidTape (sqlite3 baked in). python raidtape-genmeta.py away.
Usage Zap
raidtape-genmeta.py --catalog /path/to/backup.cat

Scans catalog for chunk-file mappings.
Builds JSONs: {"chunk_id": "abc...", "mappings": [{"file_path": "docs/readme.md", "seq": 0, "file_size": 1234, ...}, ...]}
Lands in volume's /meta/abc123.meta.json.
Outputs: "Generated X JSON metas—chunks now whisper their file secrets! 🗣️"

Sneaky Bits

Skips missing volumes/chunks—no drama.
Versioning? Add run_id if you tweak the query.
Errors? WAL retries keep it chill.


