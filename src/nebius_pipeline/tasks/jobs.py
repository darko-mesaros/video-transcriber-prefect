"""
Nebius AI Job tasks for GPU workloads (Whisper transcription).

Uses the Nebius Python SDK (gRPC) to create and monitor AI jobs.
This is the piece that n8n couldn't do natively - the whole reason
we moved to Prefect.
"""

from datetime import timedelta
import asyncio

from nebius.sdk import SDK
from nebius.api.nebius.ai.v1 import (
    CreateJobRequest,
    GetJobRequest,
    Job,
    JobServiceClient,
    JobSpec,
    JobStatus,
)
from nebius.api.nebius.common.v1 import ResourceMetadata
from nebius.api.nebius.compute.v1 import DiskSpec as ComputeDiskSpec
from prefect import task
from prefect.logging import get_run_logger

from nebius_pipeline.config import settings


def get_sdk() -> SDK:
    """Create an authenticated Nebius SDK instance.

    Uses the IAM token from config (loaded from NEBIUS_IAM_TOKEN env var / .env).
    For production, use a service account credentials file instead.
    """
    return SDK(credentials=settings.nebius_iam_token)


# Terminal states where the job won't change anymore
_TERMINAL_STATES = {
    JobStatus.State.COMPLETED,
    JobStatus.State.FAILED,
    JobStatus.State.CANCELLED,
    JobStatus.State.ERROR,
}


@task(retries=2, retry_delay_seconds=30)
async def create_whisper_job(audio_key: str) -> str:
    """
    Create a Nebius AI job to transcribe an audio file with Whisper.

    The bucket is mounted at /data inside the container, so an audio file
    at 'audio/episode.mp3' becomes '/data/audio/episode.mp3' in the container.

    Args:
        audio_key: The S3 key of the audio file (e.g. 'audio/episode.mp3')

    Returns:
        The job ID for tracking.
    """
    logger = get_run_logger()
    sdk = get_sdk()

    try:
        job_svc = JobServiceClient(sdk)

        # The audio file path inside the container where the bucket is mounted
        container_audio_path = f"/data/{audio_key}"
        stem = audio_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]

        mount_source = settings.nebius_bucket_id or settings.nebius_bucket

        operation = await job_svc.create(
            CreateJobRequest(
                metadata=ResourceMetadata(
                    parent_id=settings.nebius_project_id,
                    name=f"whisper-{stem}",
                ),
                spec=JobSpec(
                    image=settings.whisper_image,
                    args=container_audio_path,
                    platform=settings.gpu_platform,
                    preset=settings.gpu_preset,
                    subnet_id=settings.nebius_subnet_id,
                    timeout=timedelta(minutes=settings.job_timeout_minutes),
                    disk=JobSpec.DiskSpec(
                        type=ComputeDiskSpec.DiskType.NETWORK_SSD,
                        size_bytes=settings.job_disk_gib * 1024 * 1024 * 1024,
                    ),
                    volumes=[
                        JobSpec.VolumeMount(
                            source=mount_source,
                            container_path="/data",
                            mode=JobSpec.VolumeMount.Mode.READ_WRITE,
                        ),
                    ],
                ),
            )
        )

        await operation.wait()
        job_id = operation.resource_id
        logger.info(
            f"Created Whisper job {job_id} for {audio_key} using mount source {mount_source}"
        )
        return job_id

    finally:
        await sdk.close()


@task(retries=2, retry_delay_seconds=30)
async def create_ffmpeg_job(video_key: str) -> str:
    """Create a Nebius AI job to extract mp3 audio from a video file.

    This uses a CPU-oriented container image and the same bucket mount model
    as the Whisper job. The mounted bucket appears at /data inside the
    container, so a video at 'video/episode.mp4' becomes '/data/video/episode.mp4'.
    """
    logger = get_run_logger()
    sdk = get_sdk()

    try:
        job_svc = JobServiceClient(sdk)

        mount_source = settings.nebius_bucket_id or settings.nebius_bucket
        filename = video_key.rsplit("/", 1)[-1]
        stem = filename.rsplit(".", 1)[0]
        container_video_path = f"/data/{video_key}"
        container_audio_path = f"/data/{settings.audio_prefix}{stem}.mp3"
        local_work_dir = "/work"
        local_audio_path = f"{local_work_dir}/{stem}.mp3"

        # The robust path is to write the mp3 to the job's local disk first,
        # then copy it into the mounted bucket. This avoids relying on the
        # bucket mount behaving like a fully seekable POSIX filesystem during
        # ffmpeg trailer/finalization writes.
        ffmpeg_args = (
            f'-lc "mkdir -p {local_work_dir} /data/{settings.audio_prefix.rstrip("/")} '
            f"&& ffmpeg -i '{container_video_path}' -vn -q:a 2 -y '{local_audio_path}' "
            f"&& cp '{local_audio_path}' '{container_audio_path}'\""
        )

        operation = await job_svc.create(
            CreateJobRequest(
                metadata=ResourceMetadata(
                    parent_id=settings.nebius_project_id,
                    name=f"ffmpeg-{stem}",
                ),
                spec=JobSpec(
                    image=settings.ffmpeg_image,
                    container_command=settings.ffmpeg_container_command,
                    args=ffmpeg_args,
                    platform=settings.cpu_platform,
                    preset=settings.cpu_preset,
                    subnet_id=settings.nebius_subnet_id,
                    timeout=timedelta(minutes=settings.job_timeout_minutes),
                    disk=JobSpec.DiskSpec(
                        type=ComputeDiskSpec.DiskType.NETWORK_SSD,
                        size_bytes=settings.job_disk_gib * 1024 * 1024 * 1024,
                    ),
                    volumes=[
                        JobSpec.VolumeMount(
                            source=mount_source,
                            container_path="/data",
                            mode=JobSpec.VolumeMount.Mode.READ_WRITE,
                        ),
                    ],
                ),
            )
        )

        await operation.wait()
        job_id = operation.resource_id
        logger.info(
            f"Created ffmpeg job {job_id} for {video_key} using image {settings.ffmpeg_image}"
        )
        return job_id

    finally:
        await sdk.close()


@task(retries=5, retry_delay_seconds=60)
async def get_job_status(job_id: str) -> dict:
    """
    Get the current status of a Nebius AI job.

    Returns:
        Job status information as a dict.
    """
    logger = get_run_logger()
    sdk = get_sdk()

    try:
        job_svc = JobServiceClient(sdk)
        job: Job = await job_svc.get(GetJobRequest(id=job_id))

        state = job.status.state if job.status else JobStatus.State.STATE_UNSPECIFIED
        done = state in _TERMINAL_STATES

        logger.info(f"Job {job_id} state: {state.name} (done={done})")

        return {
            "job_id": job_id,
            "state": state.name,
            "done": done,
        }

    finally:
        await sdk.close()


@task
async def wait_for_job_completion(
    job_id: str,
    poll_seconds: int = 15,
    max_polls: int = 120,
) -> dict:
    """Poll a Nebius AI job until it reaches a terminal state.

    Returns the final job status dict. Raises if the job does not complete
    successfully.
    """
    logger = get_run_logger()

    for _ in range(max_polls):
        status = await get_job_status.fn(job_id)
        state = status["state"]
        if status["done"]:
            if state != JobStatus.State.COMPLETED.name:
                raise RuntimeError(f"Job {job_id} finished unsuccessfully: {state}")
            logger.info(f"Job {job_id} completed successfully")
            return status
        await asyncio.sleep(poll_seconds)

    raise TimeoutError(
        f"Job {job_id} did not reach a terminal state after {max_polls * poll_seconds} seconds"
    )
