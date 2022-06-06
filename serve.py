from pathlib import Path
import asyncio
from hlsvodserve import convert_video_to_hls_vod
from fastapi import FastAPI, BackgroundTasks, UploadFile
from fastapi.responses import PlainTextResponse
from uuid import uuid4, UUID
from dataclasses import dataclass
from typing import List, Literal
import shutil
from datetime import datetime, timezone
from pydantic import BaseModel, parse_obj_as
import os
from dotenv import load_dotenv
load_dotenv()

version = '0.0.0'
app = FastAPI(
  version=version,
)

class Meta(BaseModel):
  id: str
  status: Literal['converting', 'completed']
  timestamp: str
  success: bool = False

work_dir = Path(os.environ['WORK_DIR'])

async def convert_video(task_id: UUID):
  task_dir = get_task_dir(task_id)
  video_file = get_video_file(task_id)

  result = await convert_video_to_hls_vod(
    input_video_file=str(video_file),
    output_playlist_file=str(task_dir / 'playlist.m3u8'),
    output_stream_dir=str(task_dir),
  )

  now = datetime.now(timezone.utc)
  meta_file = get_meta_file(task_id)
  meta_file.parent.mkdir(parents=True, exist_ok=True)
  meta_file.write_text(
    Meta(
      id=str(task_id),
      status='completed',
      timestamp=now.isoformat(),
      success=True,
    ).json(ensure_ascii=False),
    encoding='utf-8',
  )

  # TODO: upload playlist/videos to object storage

def get_task_dir(task_id: UUID) -> Path:
  return work_dir / str(task_id)

def get_video_file(task_id: UUID) -> Path:
  return get_task_dir(task_id) / 'video.mp4'

def get_meta_file(task_id: UUID) -> Path:
  return get_task_dir(task_id) / 'meta.json'

# Add convert task
@app.post('/create')
async def create(file: UploadFile, background_tasks: BackgroundTasks):
  task_id = uuid4()
  now = datetime.now(timezone.utc)

  video_file = get_video_file(task_id)
  video_file.parent.mkdir(parents=True)
  with open(video_file, 'wb') as fp:
    shutil.copyfileobj(file.file, fp)

  meta_file = get_meta_file(task_id)
  meta_file.write_text(
    Meta(
      id=str(task_id),
      status='converting',
      timestamp=now.isoformat(),
      success=False,
    ).json(ensure_ascii=False),
    encoding='utf-8',
  )

  background_tasks.add_task(
    convert_video,
    task_id=task_id,
  )

  return {
    'id': task_id,
  }

@app.get('/status/{task_id}')
async def status(task_id: UUID):
  meta_file = get_meta_file(task_id)
  meta = Meta.parse_file(meta_file)
  return meta

@app.get('/')
async def task_list():
  # TODO: impl
  return [
    {
      'id': '',
    }
  ]

@app.get('/version', response_class=PlainTextResponse)
async def get_version():
  return version
