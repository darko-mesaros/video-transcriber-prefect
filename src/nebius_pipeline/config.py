"""
Configuration for the Nebius video transcription pipeline.

All settings are loaded from environment variables with sensible defaults
matching Darko's existing Nebius setup.

Most env vars use the NEBIUS_PIPELINE_ prefix. The IAM token is an exception:
it uses NEBIUS_IAM_TOKEN (no prefix) to match the Nebius SDK/CLI convention.
"""

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Pipeline configuration loaded from environment variables."""

    # Nebius IAM token - uses validation_alias so it reads NEBIUS_IAM_TOKEN
    # (no prefix) matching the Nebius SDK/CLI convention
    nebius_iam_token: str = Field(
        default="",
        validation_alias="NEBIUS_IAM_TOKEN",
    )

    # Nebius S3-compatible storage
    nebius_bucket: str = "darko-mesaros-videos"
    nebius_bucket_id: str = ""
    video_prefix: str = "video/"
    audio_prefix: str = "audio/"
    done_video_prefix: str = "DONE_video/"
    done_audio_prefix: str = "DONE_audio/"
    nebius_endpoint: str = "https://storage.us-central1.nebius.cloud"
    nebius_region: str = "us-central1"

    # AWS credentials for S3-compatible access (from bucket-uploader service account)
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""

    # Nebius AI Jobs
    nebius_project_id: str = ""
    nebius_subnet_id: str = ""
    whisper_image: str = "ghcr.io/darko-mesaros/nebius-whisper:latest"
    gpu_platform: str = "gpu-h200-sxm"
    gpu_preset: str = "1gpu-16vcpu-200gb"
    ffmpeg_image: str = "lscr.io/linuxserver/ffmpeg:latest"
    ffmpeg_container_command: str = "sh"
    cpu_platform: str = "cpu-e2"
    cpu_preset: str = "2vcpu-8gb"
    job_timeout_minutes: int = 30
    job_disk_gib: int = 250

    # Video file extensions to watch for
    video_extensions: list[str] = [".mp4", ".mkv", ".mov"]
    audio_extensions: list[str] = [".mp3", ".m4a", ".wav", ".flac", ".ogg"]

    model_config = {
        "env_prefix": "NEBIUS_PIPELINE_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }


settings = Settings()
