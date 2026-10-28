"""recordings — the video medium: receipted chunks, 409 on a changed repeat, completion only when every
expected medium is final, and the byte-concat master of a continuous stream.

Drives the SHIPPED ``upload_chunk`` / ``finalize_master`` over the in-memory fakes, OFFLINE.
"""
from __future__ import annotations

import asyncio
import hashlib

import pytest

from meeting_api.recordings import finalize_master, upload_chunk
from meeting_api.recordings.fakes import InMemoryRecordingRepo, InMemoryStorage
from meeting_api.recordings.service import ChunkConflict

USER = 7
MEETING_ID = 1
SESSION_UID = "conn-video"


def _seeded():
    repo = InMemoryRecordingRepo()
    repo.seed(meeting_id=MEETING_ID, user_id=USER, session_uid=SESSION_UID)
    return repo, InMemoryStorage()


def _up(repo, storage, data, *, seq, final=False, media_type="video", fmt="webm", expected=None):
    return asyncio.run(upload_chunk(
        repo, storage, token_meeting_id=None, session_uid=SESSION_UID, data=data,
        media_type=media_type, media_format=fmt, chunk_seq=seq, is_final=final,
        expected_media_types=expected,
    ))


def _rec(repo):
    return asyncio.run(repo.get_recordings(MEETING_ID))[0]


def _video_mf(repo):
    return next(m for m in _rec(repo)["media_files"] if m["type"] == "video")


def test_repeat_of_an_identical_chunk_is_a_receipt_not_a_second_chunk():
    repo, storage = _seeded()
    first = _up(repo, storage, b"AAAA", seq=0)
    again = _up(repo, storage, b"AAAA", seq=0)
    mf = _video_mf(repo)
    assert again["duplicate"] is True
    assert again["recording_id"] == first["recording_id"]
    assert again["sha256"] == hashlib.sha256(b"AAAA").hexdigest()
    assert mf["chunk_count"] == 1 and mf["file_size_bytes"] == 4  # counters did not move


def test_same_seq_with_different_bytes_is_a_conflict_and_the_first_bytes_survive():
    repo, storage = _seeded()
    first = _up(repo, storage, b"AAAA", seq=0)
    with pytest.raises(ChunkConflict):
        _up(repo, storage, b"BBBB", seq=0)
    assert storage.blobs[first["storage_path"]] == b"AAAA"
    assert _video_mf(repo)["chunk_count"] == 1


def test_receipts_accumulate_across_chunks():
    repo, storage = _seeded()
    for i in range(3):
        _up(repo, storage, bytes([i]) * 5, seq=i)
    mf = _video_mf(repo)
    assert sorted(mf["chunk_receipts"]) == ["0", "1", "2"]
    assert mf["chunk_count"] == 3 and mf["file_size_bytes"] == 15


def test_recording_completes_only_when_every_expected_medium_is_final():
    repo, storage = _seeded()
    both = ["audio", "video"]
    _up(repo, storage, b"a0", seq=0, media_type="audio", expected=both)
    _up(repo, storage, b"v0", seq=0, expected=both)
    _up(repo, storage, b"", seq=1, final=True, media_type="audio", expected=both)
    assert _rec(repo)["status"] == "in_progress", "audio finishing first must not complete the take"
    _up(repo, storage, b"", seq=1, final=True, expected=both)
    rec = _rec(repo)
    assert rec["status"] == "completed" and rec["completed_at"]
    assert rec["expected_media_types"] == both


def test_final_first_chunk_with_expected_media_does_not_complete_early():
    repo, storage = _seeded()
    _up(repo, storage, b"", seq=0, final=True, media_type="audio", expected=["audio", "video"])
    assert _rec(repo)["status"] == "in_progress"


def test_without_expected_media_the_first_final_still_completes_as_before():
    repo, storage = _seeded()
    _up(repo, storage, b"a0", seq=0, media_type="audio", fmt="webm")
    _up(repo, storage, b"", seq=1, final=True, media_type="audio", fmt="webm")
    assert _rec(repo)["status"] == "completed"


def test_audio_chunks_keep_overwrite_semantics_no_receipts():
    repo, storage = _seeded()
    _up(repo, storage, b"a0", seq=0, media_type="audio")
    _up(repo, storage, b"a1", seq=0, media_type="audio")  # historical: no 409, overwrites
    mf = next(m for m in _rec(repo)["media_files"] if m["type"] == "audio")
    assert "chunk_receipts" not in mf


def test_master_of_a_byte_cut_stream_is_the_exact_concatenation():
    repo, storage = _seeded()
    stream = bytes(range(256)) * 20
    cuts = [stream[i:i + 1000] for i in range(0, len(stream), 1000)]
    for i, c in enumerate(cuts):
        _up(repo, storage, c, seq=i)
    _up(repo, storage, b"", seq=len(cuts), final=True)
    rec = _rec(repo)
    key = asyncio.run(finalize_master(
        repo, storage, meeting_id=MEETING_ID, recording_id=rec["id"], media_type="video"))
    assert storage.blobs[key] == stream
    assert storage.content_types[key] == "video/webm"
    assert _rec(repo)["playback_url"]["video"].endswith("type=video")
