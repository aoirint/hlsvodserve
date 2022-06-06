from pathlib import Path
import asyncio
from asyncio.subprocess import create_subprocess_exec, PIPE
from concurrent.futures import ThreadPoolExecutor
import re
import tempfile
import platform
from dataclasses import dataclass
from typing import List

# https://stackoverflow.com/questions/45600579/asyncio-event-loop-is-closed-when-getting-loop
if platform.system() == 'Windows':
  asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

@dataclass
class ConvertVideoToHlsVodResult:
  success: bool
  playlist_file: str
  stream_dir: str
  stream_filenames: List[str]
  returncode: int
  stdout_lines: List[str]
  stderr_lines: List[str]

async def convert_video_to_hls_vod(
  input_video_file: str,
  output_playlist_file: str,
  output_stream_dir: str,
) -> ConvertVideoToHlsVodResult:
  input_video_file = Path(input_video_file)
  output_playlist_file = Path(output_playlist_file)
  output_stream_dir = Path(output_stream_dir)

  output_stream_dir.mkdir(exist_ok=True, parents=True)

  vcodec: str = 'libx264'
  acodec: str = 'aac'
  hls_time: int = 9

  hls_segment_filename: Path = output_stream_dir / '%d.ts'

  command = [
    'ffmpeg',
    '-nostdin',
    '-i',
    str(input_video_file),
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
    str(output_playlist_file),
  ]

  proc = await create_subprocess_exec(
    command[0],
    *command[1:],
    stdout=PIPE,
    stderr=PIPE,
  )

  loop = asyncio.get_event_loop()
  executor = ThreadPoolExecutor()

  stdout_lines = []
  stderr_lines = []
  def read_stdout(stdout):
    while True:
      line = asyncio.run_coroutine_threadsafe(stdout.readline(), loop).result()
      if not line:
        break
      line_text = line.decode('utf-8').strip()
      stdout_lines.append(line_text)
      print(f'STDOUT: {line_text}', flush=True)
    print('stdout closed') # closed when process exited

  def read_stderr(stderr):
    while True:
      line = asyncio.run_coroutine_threadsafe(stderr.readline(), loop).result()
      if not line:
        break
      line_text = line.decode('utf-8').strip()
      stderr_lines.append(line_text)
      print(f'STDERR: {line_text}', flush=True)
    print('stderr closed') # closed when process exited

  loop.run_in_executor(executor, read_stdout, proc.stdout)
  loop.run_in_executor(executor, read_stderr, proc.stderr)

  returncode = await proc.wait()
  # stdout, stderr may be not closed
  print(f'exited {returncode}')

  playlist_text = output_playlist_file.read_text(encoding='utf-8')
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
    playlist_file=str(output_playlist_file),
    stream_dir=str(output_stream_dir),
    stream_filenames=stream_filenames,
    returncode=returncode,
    stdout_lines=stdout_lines,
    stderr_lines=stderr_lines,
  )

async def main(
  video_file: str,
  outdir: str,
):
  video_file = Path(video_file)
  outdir = Path(outdir)

  result = await convert_video_to_hls_vod(
    input_video_file=str(video_file),
    output_playlist_file=str(outdir / 'playlist.m3u8'),
    output_stream_dir=str(outdir),
  )

  print(result)

if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument('video_file', type=str)
  parser.add_argument('--outdir', type=str, default='./')
  args = parser.parse_args()

  video_file = args.video_file
  outdir = args.outdir

  asyncio.run(main(
    video_file=video_file,
    outdir=outdir,
  ))
