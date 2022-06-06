from pathlib import Path
import subprocess
from threading import Thread

def main(
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

  proc = subprocess.Popen(
    command,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
  )

  def read_stdout(stdout):
    for line in stdout:
      line_text = line.decode('utf-8').strip()
      print(f'STDOUT: {line_text}', flush=True)
    print('stdout closed') # closed when process exited

  def read_stderr(stderr):
    for line in stderr:
      line_text = line.decode('utf-8').strip()
      print(f'STDERR: {line_text}', flush=True)
    print('stderr closed') # closed when process exited

  Thread(target=read_stdout, args=(proc.stdout,)).start()
  Thread(target=read_stderr, args=(proc.stderr,)).start()

if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument('video_file', type=str)
  parser.add_argument('--outdir', type=str, default='./')
  args = parser.parse_args()

  video_file = args.video_file
  outdir = args.outdir

  main(
    video_file=video_file,
    outdir=outdir,
  )
