"""On-demand OneMix importer of exact-matched, PUBLICLY DOWNLOADABLE Audius tracks.
No per-song IDs and no extraction from YouTube, SoundCloud, or Spotify.
"""
import asyncio
import os
import re
import secrets
import unicodedata
import uuid
from difflib import SequenceMatcher

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(docs_url=None, redoc_url=None)
SHARED_SECRET = os.environ.get("AUDIO_IMPORT_SECRET", "")
WORKER_URL = os.environ.get("ONEMIX_WORKER_URL", "").rstrip("/")
AUDIUS_API_KEY = os.environ.get("AUDIUS_API_KEY", "")
AUDIUS_API_BASE = "https://api.audius.co/v1"
MAX_FILE_BYTES = 25 * 1024 * 1024
RUNNING = set()
JOBS = {}
KEY_RE = re.compile(r"^(youtube|spotify|soundcloud|deezer):[^\s]{1,240}$")
USER_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

class ImportRequest(BaseModel):
    userId: str
    track: str
    title: str = Field(min_length=1, max_length=150)
    artist: str = Field(min_length=1, max_length=120)
    durationMs: int = Field(default=0, ge=0, le=1800000)


def auth(authorization: str | None):
    if not SHARED_SECRET or not authorization or not secrets.compare_digest(authorization, "Bearer " + SHARED_SECRET):
        raise HTTPException(401, "Unauthorized")


def norm(value: str) -> str:
    value = unicodedata.normalize("NFKD", str(value or ""))
    value = "".join(c for c in value if not unicodedata.combining(c))
    value = re.sub(r"\([^)]*(official|lyric|visualizer|video|audio)[^)]*\)", " ", value, flags=re.I)
    return " ".join(re.findall(r"[a-z0-9]+", value.lower()))


def matches(candidate: dict, title: str, artist: str, duration_ms: int = 0) -> bool:
    """No fuzzy copies: titles and uploader identities must match confidently."""
    if candidate.get("downloadable") is not True:
        return False
    if candidate.get("is_stream_gated") or candidate.get("is_unlisted"):
        return False
    target_title = norm(title)
    target_artist = norm(artist)
    ctitle = norm(candidate.get("title", ""))
    user = candidate.get("user") or {}
    c_artist = norm(user.get("name") or candidate.get("artist_name") or "")
    if not target_title or not target_artist or not ctitle or not c_artist:
        return False
    # Never substitute a similarly named song or a tribute/cover.
    if ctitle != target_title or c_artist != target_artist:
        return False
    if duration_ms and candidate.get("duration"):
        if abs(round(duration_ms / 1000) - int(candidate["duration"])) > 12:
            return False
    return bool(candidate.get("id"))


def audius_headers() -> dict:
    return {"x-api-key": AUDIUS_API_KEY} if AUDIUS_API_KEY else {}


async def locate_download(client: httpx.AsyncClient, req: ImportRequest) -> tuple[bytes, str] | None:
    search = await client.get(AUDIUS_API_BASE + "/tracks/search", params={
        "query": f"{req.artist} {req.title}", "limit": 20, "only_downloadable": "true"
    }, headers=audius_headers())
    search.raise_for_status()
    candidates = search.json().get("data", [])
    if not isinstance(candidates, list):
        return None
    for found in candidates:
        if not isinstance(found, dict) or not matches(found, req.title, req.artist, req.durationMs):
            continue
        track_id = str(found["id"])
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", track_id):
            continue
        # This API endpoint provides files ONLY when the artist has permitted downloads.
        async with client.stream("GET", AUDIUS_API_BASE + f"/tracks/{track_id}/download",
                                 params={"format": "mp3"}, headers=audius_headers()) as resp:
            if resp.status_code in (401, 403, 404):
                continue
            resp.raise_for_status()
            if "text/html" in resp.headers.get("content-type", "").lower():
                continue
            chunks, total = [], 0
            async for chunk in resp.aiter_bytes():
                total += len(chunk)
                if total > MAX_FILE_BYTES:
                    raise ValueError("Audio exceeds OneMix's 25 MB limit")
                chunks.append(chunk)
            data = b"".join(chunks)
            if not data:
                continue
            # Accept audio MPEG or MP4 (M4A); don't cache JSON error pages.
            mime = "audio/mpeg" if data.startswith(b"ID3") or (len(data)>1 and data[0]==255 and (data[1]&0xE0)==0xE0) else (
                "audio/mp4" if data[4:8] == b"ftyp" else "")
            if mime:
                return data, mime
    return None


async def run_job(job_id: str, req: ImportRequest):
    try:
        async with httpx.AsyncClient(timeout=75, follow_redirects=True) as client:
            result = await locate_download(client, req)
            if result is None:
                JOBS[job_id]["state"] = "unavailable"
                return
            audio_bytes, mime = result
            response = await client.put(WORKER_URL + "/api/audio/ingest", headers={
                "Authorization": "Bearer " + SHARED_SECRET,
                "X-OneMix-Track": req.track,
                "X-OneMix-User": req.userId,
                "Content-Type": mime,
            }, content=audio_bytes, timeout=90)
            response.raise_for_status()
        JOBS[job_id]["state"] = "ready"
    except Exception as exc:
        JOBS[job_id].update(state="failed", error=str(exc)[:160])
    finally:
        RUNNING.discard((req.userId, req.track))


@app.get("/health")
def health():
    return {"ok": True, "configured": bool(SHARED_SECRET and WORKER_URL.startswith("https://")),
            "source": "Audius publicly downloadable tracks", "activeJobs": len(RUNNING)}


@app.post("/jobs")
async def queue_job(req: ImportRequest, authorization: str | None = Header(default=None)):
    auth(authorization)
    if not USER_RE.fullmatch(req.userId) or not KEY_RE.fullmatch(req.track):
        raise HTTPException(400, "Invalid track or user")
    if not WORKER_URL.startswith("https://"):
        raise HTTPException(503, "ONEMIX_WORKER_URL must be HTTPS")
    key = (req.userId, req.track)
    for job_id, job in JOBS.items():
        if job["key"] == key and job["state"] in ("processing", "ready"):
            return {"jobId": job_id, "state": job["state"]}
    if len(RUNNING) >= 2:
        raise HTTPException(429, "Audio backend busy")
    job_id = uuid.uuid4().hex
    JOBS[job_id] = {"state":"processing", "userId":req.userId, "key":key}
    RUNNING.add(key)
    asyncio.create_task(run_job(job_id, req))
    return {"jobId":job_id, "state":"processing"}


@app.get("/jobs/{job_id}")
async def job_status(job_id: str, userId: str, authorization: str | None = Header(default=None)):
    auth(authorization)
    job = JOBS.get(job_id)
    if not job or job["userId"] != userId:
        raise HTTPException(404,"Job not found")
    return {"state": job["state"], "error": job.get("error")}
