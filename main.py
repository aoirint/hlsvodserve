from pathlib import Path
import asyncio
from asyncio.subprocess import create_subprocess_exec, PIPE
from concurrent.futures import ThreadPoolExecutor
import re
import tempfile
import platform

# https://stackoverflow.com/questions/45600579/asyncio-event-loop-is-closed-when-getting-loop
if platform.system() == 'Windows':
  asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def main(
  video_file: str,
  outdir: str,
):
  video_file = Path(video_file)
  outdir = Path(outdir)

  outdir.mkdir(exist_ok=True, parents=True)

  vcodec: str = 'libx264'
  acodec: str = 'aac'
  hls_time: int = 9

  hls_segment_filename: Path = outdir / '%d.ts'
  hls_playlist_filename: Path = outdir / 'playlist.m3u8'

  command = [
    'ffmpeg',
    '-i',
    str(video_file),
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
    str(hls_playlist_filename),
  ]

  proc = await create_subprocess_exec(
    command[0],
    *command[1:],
    stdout=PIPE,
    stderr=PIPE,
  )

  loop = asyncio.get_event_loop()
  executor = ThreadPoolExecutor()

  def read_stdout(stdout):
    while True:
      line = asyncio.run_coroutine_threadsafe(stdout.readline(), loop).result()
      if not line:
        break
      line_text = line.decode('utf-8').strip()
      print(f'STDOUT: {line_text}', flush=True)
    print('stdout closed') # closed when process exited

  def read_stderr(stderr):
    while True:
      line = asyncio.run_coroutine_threadsafe(stderr.readline(), loop).result()
      if not line:
        break
      line_text = line.decode('utf-8').strip()
      print(f'STDERR: {line_text}', flush=True)
    print('stderr closed') # closed when process exited

  loop.run_in_executor(executor, read_stdout, proc.stdout)
  loop.run_in_executor(executor, read_stderr, proc.stderr)

  await proc.wait()

  print('exited')

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
