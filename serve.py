from concurrent.futures import ThreadPoolExecutor, process
from pathlib import Path
import asyncio
from asyncio.subprocess import Process
import threading
import time
import schedule
from hlsvodserve import convert_video_to_hls_vod
from fastapi import FastAPI, BackgroundTasks, UploadFile
from fastapi.responses import PlainTextResponse
from fastapi.encoders import jsonable_encoder
from uuid import uuid4, UUID
from dataclasses import dataclass, field
from typing import BinaryIO, Dict, List, Literal, Optional
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
work_dir = Path(os.environ['WORK_DIR'])

class VideoConvertJobInfo(BaseModel):
  id: str
  status: Literal['converting', 'completed']
  created_at: str
  success: bool = False

# Create job
# Save uploaded video
# Preprocess
# FFmpeg
# Postprocess
# Upload
# Clean job
#   Clean uploaded video
#   Clean converted video
# Remove job log after 15 minutes (completed log?)

@dataclass
class VideoConvertError:
  pass

@dataclass
class InvalidStateError(VideoConvertError):
  message: str = None

@dataclass
class CreateJobResult:
  job_id: UUID

@dataclass
class SaveUploadedVideoResult:
  job_id: UUID
  video_path: Path

@dataclass
class ConvertSavedVideoResult:
  job_id: UUID
  stream_playlist_path: Path
  stream_filenames: List[str]

@dataclass
class JobStatus:
  id: UUID
  created_at: datetime
  job_dir_path: Path
  video_path: Path
  stream_dir_path: Path
  stream_playlist_path: Path
  stream_filenames: List[str] = None
  video_created: bool = False
  video_created_at: Optional[datetime] = None
  stream_created: bool = False
  stream_created_at: Optional[datetime] = None
  upload_created: bool = False
  upload_created_at: Optional[datetime] = None

class JobStatusResponseData(BaseModel):
  id: UUID
  created_at: datetime
  video_created: bool = False
  video_created_at: Optional[datetime] = None
  stream_created: bool = False
  stream_created_at: Optional[datetime] = None
  upload_created: bool = False
  upload_created_at: Optional[datetime] = None

def datetime_utc_aware_now() -> datetime:
  return datetime.now(timezone.utc)

@dataclass
class JobManager:
  job_ids: List[str] = field(default_factory=lambda: [])
  job_status: Dict[str, JobStatus] = field(default_factory=lambda: {})

  async def create_job(self) -> CreateJobResult:
    now = datetime_utc_aware_now()
    job_id = uuid4()
    job_dir_path = work_dir / str(job_id)
    stream_dir_path = job_dir_path

    self.job_ids.append(job_id)
    self.job_status[job_id] = JobStatus(
      id=job_id,
      created_at=now,
      job_dir_path=job_dir_path,
      video_path=job_dir_path / 'video.mp4',
      stream_dir_path=stream_dir_path,
      stream_playlist_path=stream_dir_path / 'playlist.m3u8',
    )

    return CreateJobResult(
      job_id=job_id,
    )

  async def save_uploaded_video(self, job_id: UUID, uploaded_file: BinaryIO):
    job = self.job_status[job_id]
    video_path = job.video_path

    video_path.parent.mkdir(parents=True)
    with open(video_path, 'wb') as fp:
      shutil.copyfileobj(uploaded_file.file, fp)

    job.video_created = True
    job.video_created_at = datetime_utc_aware_now()

    # TODO: check the uploaded file is valid mp4 video or not
    return SaveUploadedVideoResult(
      job_id=job_id,
      video_path=video_path,
    )

  async def convert_saved_video(self, job_id: UUID):
    job = self.job_status[job_id]

    job_dir_path = job.job_dir_path
    video_path = job.video_path
    if not job.video_created:
      raise InvalidStateError(message='Video must be created')

    result = await convert_video_to_hls_vod(
      input_video_path=str(video_path),
      output_playlist_path=str(job_dir_path / 'playlist.m3u8'),
      output_stream_dir_path=str(job_dir_path),
    )
    if not result.success:
      raise InvalidStateError(message=f'FFmpeg returncode={result.returncode}')

    job.stream_created = True
    job.stream_created_at = datetime_utc_aware_now()

    return ConvertSavedVideoResult(
      job_id=job_id,
      stream_playlist_path=result.playlist_path,
      stream_filenames=result.stream_filenames,
    )

  async def upload_converted_video(self, job_id: UUID):
    job = self.job_status[job_id]

    if not job.stream_created:
      raise InvalidStateError(message='Stream must be created')

    # TODO: upload to object storage

    job.upload_created = True
    job.upload_created_at = datetime_utc_aware_now()

  async def clean_video(self, job_id: UUID):
    job = self.job_status[job_id]
    job_dir = job.job_dir_path

    print(f'clean video {job_id}')
    shutil.rmtree(job_dir)

  async def remove_job(self, job_id: UUID):
    # kill ffmpeg process if exist
    await self.clean_video(job_id=job_id)

    self.job_ids.remove(job_id)
    del self.job_status[job_id]

job_manager = JobManager()

async def background_video_task(job_id: UUID):
  job = job_manager.job_status[job_id]

  try:
    await job_manager.convert_saved_video(job_id=job_id)
    await job_manager.upload_converted_video(job_id=job_id)
  finally:
    await job_manager.clean_video(job_id=job_id)
    schedule_remove_job(job_id=job_id, minutes=15)

schedule_event = threading.Event()

def schedule_remove_job(job_id: UUID, minutes: int):
  def remove_job():
    try:
      async def remove_job_async():
        print(f'removing job {job_id}')
        await job_manager.remove_job(job_id=job_id)
        print(f'removing done job {job_id}')

      asyncio.run(remove_job_async())
    finally:
      print(f'removed job {job_id}')
      return schedule.CancelJob

  schedule_job = schedule.every(minutes).minutes.do(remove_job)
  print(f'removing job scheduled at {schedule_job.next_run.isoformat()}')

@app.on_event('startup')
async def startup_clean_work_dir():
  print('cleanup work dir start')
  for job_dir_path in work_dir.iterdir():
    print(f'remove job dir {job_dir_path.name}')
    shutil.rmtree(job_dir_path)
  print('cleanup work dir done')

@app.on_event('startup')
async def startup_schedule():
  loop = asyncio.get_event_loop()
  executor = ThreadPoolExecutor()

  def loop_schedule(event):
    while True:
      if event.is_set():
        break
      schedule.run_pending()
      time.sleep(1)

    print('run all existing scheduled jobs')
    schedule.run_all()

    print('exit schedule')

  loop.run_in_executor(executor, loop_schedule, schedule_event)

@app.on_event('shutdown')
async def shutdown_schedule():
  schedule_event.set()

@app.post('/jobs', response_model=JobStatusResponseData)
async def create_job(file: UploadFile, background_tasks: BackgroundTasks):
  result = await job_manager.create_job()
  job_id = result.job_id

  result = await job_manager.save_uploaded_video(job_id=job_id, uploaded_file=file)

  background_tasks.add_task(
    background_video_task,
    job_id=job_id,
  )

  return jsonable_encoder(job_manager.job_status[job_id])

@app.get('/jobs/{job_id}', response_model=JobStatusResponseData)
async def get_job_status(job_id: UUID):
  return jsonable_encoder(job_manager.job_status[job_id])

@app.get('/jobs', response_model=List[JobStatusResponseData])
async def get_job_list():
  job_list: List[JobStatusResponseData] = []
  for job_id in job_manager.job_ids:
    job_list.append(job_manager.job_status[job_id])

  return jsonable_encoder(job_list)

@app.get('/version', response_class=PlainTextResponse)
async def get_version() -> str:
  return version
