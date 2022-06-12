from pathlib import Path
import asyncio
from sirasu import convert_video_to_hls_vod

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
