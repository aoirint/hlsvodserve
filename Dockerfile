FROM python:3.9

ENV WORK_DIR=/work
ENV PATH=/home/user/.local/bin:$PATH

RUN apt-get update && \
    apt-get install -y \
        ffmpeg \
        gosu && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN useradd -m --uid=1000 -o user

ADD ./requirements.txt /tmp/requirements.txt
RUN gosu user pip3 install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /code
ADD ./hlsvodserve /code/hlsvodserve
ADD ./serve.py /code/serve.py
ADD ./convert_video_to_hls_vod.py /code/convert_video_to_hls_vod.py

CMD ["gosu", "user", "uvicorn", "serve:app", "--host", "0.0.0.0", "--port", "8000"]
