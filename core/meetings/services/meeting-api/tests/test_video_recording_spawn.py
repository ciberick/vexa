"""bot_spawn — the opt-in ``video_recording`` request → the sealed ``videoRecording`` invocation field.

Offline: the request resolver (422s), the invocation builder (schema-conformant, None-stripped), and
the stock ``request_bot`` spawn (video only on an explicit opt-in; the historical spawn is unchanged).
"""
from __future__ import annotations

import json

import pytest
from fastapi import HTTPException

from meeting_api.bot_spawn import build_invocation, mint_meeting_token, request_bot
from meeting_api.bot_spawn.fakes import FakeRuntimeClient, InMemoryMeetingRepo
from meeting_api.bot_spawn.invocation import conforms_invocation
from meeting_api.bot_spawn.router import _resolve_video_recording

SECRET = "test-admin-token"
USER = 7


def _resolve(value, platform="google_meet", recording_enabled=True):
    return _resolve_video_recording(value, platform=platform, recording_enabled=recording_enabled)


def test_absent_or_disabled_is_audio_only():
    assert _resolve(None) is None
    assert _resolve({"enabled": False}) is None
    assert _resolve({"enabled": False}, platform="zoom") is None  # disabled never errors


def test_enabled_defaults_to_5_fps_and_accepts_10():
    assert _resolve({"enabled": True}) == {"enabled": True, "fps": 5}
    assert _resolve({"enabled": True, "fps": 10}) == {"enabled": True, "fps": 10}


@pytest.mark.parametrize("value,kwargs", [
    ({"enabled": True}, {"platform": "zoom"}),
    ({"enabled": True}, {"platform": "teams"}),
    ({"enabled": True}, {"recording_enabled": False}),
    ({"enabled": True, "fps": 15}, {}),
    ({"enabled": True, "fps": True}, {}),
    ({"enabled": True, "fps": "5"}, {}),
    ({"fps": 5}, {}),
    ({"enabled": "yes"}, {}),
    ("on", {}),
])
def test_unsupported_requests_are_422_never_silently_downgraded(value, kwargs):
    with pytest.raises(HTTPException) as e:
        _resolve(value, **kwargs)
    assert e.value.status_code == 422


def _base():
    token = mint_meeting_token(1, USER, "google_meet", "abc-defg-hij", secret=SECRET)
    return dict(meeting_id=1, platform="google_meet", meeting_url="https://meet.google.com/abc-defg-hij",
                bot_name="VexaBot", token=token, native_meeting_id="abc-defg-hij",
                connection_id="conn-1", redis_url="redis://redis:6379/0")


def test_build_invocation_carries_video_recording_and_strips_it_otherwise():
    inv = build_invocation(**_base(), recording_enabled=True, video_recording={"enabled": True, "fps": 5})
    conforms_invocation(inv)
    assert inv["videoRecording"] == {"enabled": True, "fps": 5}
    plain = build_invocation(**_base(), recording_enabled=True)
    assert "videoRecording" not in plain


def test_schema_rejects_an_out_of_range_fps():
    inv = build_invocation(**_base(), recording_enabled=True, video_recording={"enabled": True, "fps": 5})
    inv["videoRecording"]["fps"] = 60
    with pytest.raises(Exception):
        conforms_invocation(inv)


async def _spawn(repo, runtime, **kw):
    return await request_bot(
        repo, runtime, user_id=USER, platform="google_meet", native_meeting_id="abc-defg-hij",
        redis_url="redis://redis:6379/0", meeting_api_url="http://meeting-api:8080",
        internal_secret="s", token_secret=SECRET, **kw)


async def test_spawn_with_opt_in_ships_video_recording():
    repo, runtime = InMemoryMeetingRepo(), FakeRuntimeClient()
    await _spawn(repo, runtime, recording_enabled=True, video_recording={"enabled": True, "fps": 5})
    inv = json.loads(runtime.specs[0]["env"]["VEXA_BOT_CONFIG"])
    conforms_invocation(inv)
    assert inv["videoRecording"] == {"enabled": True, "fps": 5}


async def test_historical_spawn_has_no_video_field():
    repo, runtime = InMemoryMeetingRepo(), FakeRuntimeClient()
    await _spawn(repo, runtime, recording_enabled=True)
    inv = json.loads(runtime.specs[0]["env"]["VEXA_BOT_CONFIG"])
    assert "videoRecording" not in inv


async def test_video_without_recording_never_reaches_the_bot():
    repo, runtime = InMemoryMeetingRepo(), FakeRuntimeClient()
    await _spawn(repo, runtime, recording_enabled=False, video_recording={"enabled": True, "fps": 5})
    inv = json.loads(runtime.specs[0]["env"]["VEXA_BOT_CONFIG"])
    assert "videoRecording" not in inv
