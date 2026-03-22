"""
Audio extraction tasks: video -> mp3.

The local path is intentionally split into separate Prefect tasks so failures
in download, ffmpeg extraction, and upload are visible and retry independently.

Future remote extraction should keep the same high-level contract:
video_key -> audio_key.
"""

import subprocess
import tempfile
from pathlib import Path

from prefect import task
from prefect.logging import get_run_logger

from nebius_pipeline.config import settings
from nebius_pipeline.tasks.storage import upload_object


def video_key_to_audio_key(video_key: str) -> str:
    """Convert a video key to the corresponding audio key.

    'video/episode.mp4' -> 'audio/episode.mp3'
    """
    filename = video_key.rsplit("/", 1)[-1]
    stem = filename.rsplit(".", 1)[0]
    return f"{settings.audio_prefix}{stem}.mp3"


def build_local_paths(video_key: str) -> dict[str, str]:
    """Build stable temp paths for local extraction work."""
    filename = video_key.rsplit("/", 1)[-1]
    stem = filename.rsplit(".", 1)[0]
    tmpdir = Path(tempfile.gettempdir()) / "nebius-prefect"
    tmpdir.mkdir(parents=True, exist_ok=True)
    return {
        "local_video": str(tmpdir / filename),
        "local_audio": str(tmpdir / f"{stem}.mp3"),
    }


@task(retries=1, retry_delay_seconds=10)
def run_ffmpeg_extraction(local_video: str, local_audio: str) -> str:
    """Run ffmpeg locally to extract mp3 audio from a video file."""
    logger = get_run_logger()
    input_name = Path(local_video).name
    output_name = Path(local_audio).name

    logger.info(f"Extracting audio with ffmpeg: {input_name} -> {output_name}")
    result = subprocess.run(
        [
            "ffmpeg",
            "-i",
            local_video,
            "-vn",
            "-q:a",
            "2",
            "-y",
            local_audio,
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"ffmpeg failed (exit {result.returncode}): {result.stderr[-500:]}"
        )

    return local_audio


@task(retries=2, retry_delay_seconds=10)
def upload_extracted_audio(local_audio: str, audio_key: str) -> str:
    """Upload extracted audio to Nebius storage."""
    logger = get_run_logger()
    logger.info(f"Uploading extracted audio to {audio_key}")
    return upload_object.fn(local_audio, audio_key)


# TODO: extract_audio_remote - Nebius AI job with ffmpeg container
# Same high-level contract: (video_key: str) -> str
# Would run: ffmpeg -i /data/video/episode.mp4 -vn -q:a 2 /data/audio/episode.mp3
# on a small CPU-only Nebius job.
