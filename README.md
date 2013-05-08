Chunker
=======

DropBox + BitTorrent + RSync = Chunker


DropBox?
--------
- Sync files between PCs
- With versioning
  - See who changed what, where, and when
  - TODO: Currently only historic metadata is stored, need to
    build an archiver client which will store old chunks so
	that old versions can be rebuilt


BitTorrent?
-----------
- People downloading the same files will spread the data around
- You can export a read-only snapshot of a shared folder in
  a .torrent-like format


RSync?
------
- Chunks are dynamically sized and not owned by any specific
  file, if you already have a partial match for a download,
  it can be used.
  - Downloading ubuntu.iso (700MB) + kubuntu.iso (900MB) can
    be done in 1200MB, because they have ~400MB in common


Anything Else?
--------------
- Files with corrupt parts can be healed using other similar
  files; eg if you have a successfull ubuntu download and a
  corrupt kubuntu, and the corruption is in the shared area,
  it can be repaired entirely offline.
