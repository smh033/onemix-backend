"""Authenticated on-demand audio importer for OneMix test deployments.
Only retrieve and store recordings you have permission to use.
"""
import asyncio
import os
import re
import tempfile
import uuid
from pathlib import Path

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from pytubefix import YouTube

app = FastAPI(docs_url=None, redoc_url=None)
SHARED_SECRET = os.environ.get('AUDIO_IMPORT_SECRET', '')
WORKER_URL = os.environ.get('ONEMIX_WORKER_URL', '').rstrip('/')
VIDEO_RE = re.compile(r'^[A-Za-z0-9_-]{11}$')
TRACK_RE = re.compile(r'^(youtube|spotify|soundcloud|deezer):\S{1,240}$')
JOBS = {}
RUNNING = set()

class DownloadRequest(BaseModel):
    userId: str
    track: str
    videoId: str

def check_auth(authorization: str | None):
    import secrets
    if not SHARED_SECRET or not authorization or not secrets.compare_digest(authorization, 'Bearer ' + SHARED_SECRET):
        raise HTTPException(401, 'Unauthorized')

def download_permitted(video_id: str):
    # Run only with permission to download and play the source recording.
    with tempfile.TemporaryDirectory(prefix='onemix-audio-') as folder:
        yt = YouTube('https://www.youtube.com/watch?v=' + video_id)
        stream = yt.streams.filter(only_audio=True, mime_type='audio/mp4').order_by('abr').desc().first()
        if not stream:
            raise RuntimeError('No accessible audio stream')
        filename = stream.download(output_path=folder, filename='track.m4a')
        data = Path(filename).read_bytes()
        if not 0 < len(data) <= 25 * 1024 * 1024:
            raise RuntimeError('Audio exceeds the 25 MB OneMix limit')
        return data

async def run_job(job_id: str, payload: DownloadRequest):
    try:
        audio_bytes = await asyncio.to_thread(download_permitted, payload.videoId)
        async with httpx.AsyncClient(timeout=120) as client:
            result = await client.put(WORKER_URL + '/api/audio/ingest',
                headers={'Authorization':'Bearer ' + SHARED_SECRET,
                         'X-OneMix-Track': payload.track,
                         'X-OneMix-User': payload.userId,
                         'X-OneMix-Video': payload.videoId,
                         'Content-Type': 'audio/mp4'},
                content=audio_bytes)
            result.raise_for_status()
        JOBS[job_id]['state']='ready'
    except Exception as e:
        JOBS[job_id].update(state='failed', error=str(e)[:200])
    finally:
        RUNNING.discard((payload.userId, payload.track))

@app.get('/health')
def health():
    return {'ok': True, 'configured': bool(SHARED_SECRET and WORKER_URL.startswith('https://')), 'source': 'pytubefix', 'activeJobs': len(RUNNING)}

@app.post('/jobs')
async def queue_job(payload: DownloadRequest, authorization: str | None = Header(default=None)):
    check_auth(authorization)
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}',payload.userId) or not TRACK_RE.fullmatch(payload.track) or not VIDEO_RE.fullmatch(payload.videoId):
        raise HTTPException(400,'Invalid request')
    if not WORKER_URL.startswith('https://'):
        raise HTTPException(503,'Set ONEMIX_WORKER_URL to your HTTPS Worker URL')
    if len(RUNNING) >= 2:
        raise HTTPException(429,'Audio backend busy')
    for key,value in JOBS.items():
        if value['userId']==payload.userId and value['track']==payload.track and value['state'] in ('processing','ready'):
            return {'jobId':key,'state':value['state']}
    job_id=uuid.uuid4().hex
    JOBS[job_id]={'state':'processing','userId':payload.userId,'track':payload.track}
    RUNNING.add((payload.userId,payload.track))
    asyncio.create_task(run_job(job_id,payload))
    return {'jobId':job_id,'state':'processing'}

@app.get('/jobs/{job_id}')
async def get_job(job_id: str, userId: str, authorization: str | None = Header(default=None)):
    check_auth(authorization)
    job = JOBS.get(job_id)
    if not job or job['userId'] != userId:
        raise HTTPException(404,'Job not found')
    return {'state':job['state'],'error':job.get('error')}
