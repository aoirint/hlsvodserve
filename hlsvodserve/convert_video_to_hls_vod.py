from pathlib import Path
import asyncio
from asyncio.subprocess import create_subprocess_exec, PIPE
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import re
import tempfile
import time
from typing import List

@dataclass
class ConvertVideoToHlsVodResult:
  success: bool
  playlist_path: str
  stream_dir_path: str
  stream_filenames: List[str]
  returncode: int
  report_lines: List[str]

async def convert_video_to_hls_vod(
  input_video_path: str,
  output_playlist_path: str,
  output_stream_dir_path: str,
) -> ConvertVideoToHlsVodResult:
  input_video_path = Path(input_video_path)
  output_playlist_path = Path(output_playlist_path)
  output_stream_dir_path = Path(output_stream_dir_path)

  output_stream_dir_path.mkdir(exist_ok=True, parents=True)

  vcodec: str = 'libx264'
  acodec: str = 'aac'
  hls_time: int = 9

  hls_segment_filename: Path = output_stream_dir_path / '%d.ts'

  report_tempfile = tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8')
  report_loglevel = 32 # 32: info, 48: debug
  report = f'file={report_tempfile.name}:level={report_loglevel}'

  command = [
    'ffmpeg',
    '-nostdin',
    '-i',
    str(input_video_path),
    '-vcodec',
    vcodec,
    '-acodec',
    acodec,
    '-f',
    'hls',
    '-hls_time',
    str(hls_time),
    '-hls_playlist_type',
    'vod',
    '-hls_segment_filename',
    str(hls_segment_filename),
    '-start_number',
    '1',
    '-report',
    str(output_playlist_path),
  ]

  proc = await create_subprocess_exec(
    command[0],
    *command[1:],
    env={
      'FFREPORT': report,
    },
  )

  loop = asyncio.get_event_loop()
  executor = ThreadPoolExecutor()

  report_lines = []
  def read_report(report_file):
    report_file.seek(0)
    while True:
        line = report_file.readline()
        if len(line) == 0: # EOF
          if proc.returncode is not None: # process closed and EOF
            break
          time.sleep(0.1)
          continue # for next line written
        if line.endswith('\n'):
          line = line[:-1] # strip linebreak
        report_lines.append(line)
        print(f'REPORT: {line}', flush=True)
    print('report closed') # closed when process exited

  loop.run_in_executor(executor, read_report, report_tempfile)

  returncode = await proc.wait()
  # stdout, stderr may be not closed
  print(f'exited {returncode}')

  playlist_text = output_playlist_path.read_text(encoding='utf-8')
  playlist_lines = playlist_text.split('\n')

  stream_filenames = []
  for playlist_line in playlist_lines:
    line = playlist_line.strip()
    if not line:
      continue
    if line.startswith('#'):
      continue

    stream_filenames.append(line)

  success = returncode == 0

  return ConvertVideoToHlsVodResult(
    success=success,
    playlist_path=str(output_playlist_path),
    stream_dir_path=str(output_stream_dir_path),
    stream_filenames=stream_filenames,
    returncode=returncode,
    report_lines=report_lines,
  )
