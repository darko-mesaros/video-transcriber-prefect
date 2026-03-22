"""
S3-compatible storage tasks for Nebius Object Storage.

Uses boto3 pointed at the Nebius S3 endpoint for object operations
(list, download, upload, move). Bucket management would use the Nebius
gRPC SDK, but we don't need it for the pipeline.

Bucket layout:
    video/episode.mp4          <- raw video uploads (inbox)
    audio/episode.mp3          <- extracted audio (inbox)
    DONE_video/episode.mp4     <- processed videos
    DONE_audio/episode.mp3     <- transcribed audio
    DONE_audio/episode.txt     <- transcripts
"""

import boto3
from prefect import task
from prefect.logging import get_run_logger

from nebius_pipeline.config import settings


def get_s3_client():
    """Create a boto3 S3 client configured for Nebius Object Storage."""
    return boto3.client(
        "s3",
        endpoint_url=settings.nebius_endpoint,
        region_name=settings.nebius_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )


def _list_keys(s3, prefix: str) -> list[str]:
    """List all object keys under a prefix."""
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.nebius_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def _filename(key: str) -> str:
    """Extract the filename from a key: 'video/episode.mp4' -> 'episode.mp4'"""
    return key.rsplit("/", 1)[-1]


@task(retries=2, retry_delay_seconds=5)
def object_exists(key: str) -> bool:
    """Return True if an object exists in the bucket."""
    s3 = get_s3_client()
    try:
        s3.head_object(Bucket=settings.nebius_bucket, Key=key)
        return True
    except Exception:
        return False


@task(retries=3, retry_delay_seconds=10)
def list_new_videos() -> list[str]:
    """List video files in the video/ inbox prefix.

    Anything in video/ is unprocessed by definition. Processed files
    get moved to DONE_video/.
    """
    logger = get_run_logger()
    s3 = get_s3_client()
    keys = _list_keys(s3, settings.video_prefix)
    video_keys = [
        k for k in keys if any(k.endswith(ext) for ext in settings.video_extensions)
    ]
    logger.info(
        f"Found {len(video_keys)} new videos in s3://{settings.nebius_bucket}/{settings.video_prefix}"
    )
    return video_keys


@task(retries=3, retry_delay_seconds=10)
def list_new_audio() -> list[str]:
    """List audio files in the audio/ inbox prefix.

    Anything in audio/ that's an audio file is untranscribed.
    Transcribed files get moved to DONE_audio/.
    """
    logger = get_run_logger()
    s3 = get_s3_client()
    keys = _list_keys(s3, settings.audio_prefix)
    audio_keys = [
        k for k in keys if any(k.endswith(ext) for ext in settings.audio_extensions)
    ]
    logger.info(
        f"Found {len(audio_keys)} new audio files in s3://{settings.nebius_bucket}/{settings.audio_prefix}"
    )
    return audio_keys


@task(retries=2, retry_delay_seconds=5)
def move_object(source_key: str, dest_prefix: str) -> str:
    """Move an object from one prefix to another (copy + delete).

    S3 has no native move, so this is a copy followed by a delete.
    The filename is preserved, only the prefix changes.

    Example: move_object('video/ep.mp4', 'DONE_video/') -> 'DONE_video/ep.mp4'
    """
    logger = get_run_logger()
    s3 = get_s3_client()
    dest_key = f"{dest_prefix}{_filename(source_key)}"
    bucket = settings.nebius_bucket

    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key,
    )
    s3.delete_object(Bucket=bucket, Key=source_key)

    logger.info(f"Moved s3://{bucket}/{source_key} -> s3://{bucket}/{dest_key}")
    return dest_key


@task(retries=2, retry_delay_seconds=5)
def download_object(key: str, local_path: str) -> str:
    """Download an object from Nebius storage to a local path."""
    logger = get_run_logger()
    s3 = get_s3_client()
    s3.download_file(settings.nebius_bucket, key, local_path)
    logger.info(f"Downloaded s3://{settings.nebius_bucket}/{key} -> {local_path}")
    return local_path


@task(retries=2, retry_delay_seconds=5)
def upload_object(local_path: str, key: str) -> str:
    """Upload a local file to Nebius storage."""
    logger = get_run_logger()
    s3 = get_s3_client()
    s3.upload_file(local_path, settings.nebius_bucket, key)
    logger.info(f"Uploaded {local_path} -> s3://{settings.nebius_bucket}/{key}")
    return key
