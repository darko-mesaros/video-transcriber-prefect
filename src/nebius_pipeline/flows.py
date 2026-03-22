"""
Video transcription pipeline flow.

Watches Nebius Object Storage for new video/audio files and triggers
Whisper transcription jobs on H200 GPUs.

Bucket layout (inbox model):
    video/          <- drop raw videos here
    audio/          <- drop extracted audio here
    DONE_video/     <- processed videos (moved after audio extraction)
    DONE_audio/     <- transcribed audio + transcripts (moved after Whisper)

Pipeline:
  1. Check video/ for new videos -> extract audio -> move to DONE_video/
  2. Check audio/ for new audio -> run Whisper -> move to DONE_audio/
"""

from prefect import flow
from prefect.logging import get_run_logger

from nebius_pipeline.config import settings
from nebius_pipeline.tasks.storage import (
    list_new_videos,
    list_new_audio,
    move_object,
    object_exists,
    download_object,
)
from nebius_pipeline.tasks.extract import (
    build_local_paths,
    run_ffmpeg_extraction,
    upload_extracted_audio,
    video_key_to_audio_key,
)
from nebius_pipeline.tasks.jobs import (
    create_ffmpeg_job,
    create_whisper_job,
    wait_for_job_completion,
)


@flow(name="Transcription Pipeline", log_prints=True)
async def transcription_pipeline() -> dict:
    """
    Main pipeline: detect new files, extract audio, launch transcription jobs,
    and move completed files to DONE_ prefixes.

    Returns a summary of what was found and what was processed.
    """
    logger = get_run_logger()

    # Step 1: Check for new videos and extract audio
    new_videos = list_new_videos()
    extracted = []
    for video_key in new_videos:
        paths = build_local_paths(video_key)
        audio_key = video_key_to_audio_key(video_key)
        download_object(video_key, paths["local_video"])
        run_ffmpeg_extraction(paths["local_video"], paths["local_audio"])
        upload_extracted_audio(paths["local_audio"], audio_key)
        move_object(video_key, settings.done_video_prefix)
        extracted.append(audio_key)

    if extracted:
        logger.info(f"Extracted audio from {len(extracted)} videos: {extracted}")

    # Step 2: Check for new audio (includes freshly extracted + previously uploaded)
    new_audio = list_new_audio()

    # Step 3: Launch Whisper jobs for each new audio file
    job_ids = []
    transcripts_moved = []
    for audio_key in new_audio:
        job_id = await create_whisper_job(audio_key)
        job_ids.append(job_id)

        # Wait for the actual container workload to finish before moving files.
        await wait_for_job_completion(job_id)

        transcript_key = f"{audio_key.rsplit('.', 1)[0]}.txt"
        if object_exists(transcript_key):
            move_object(transcript_key, settings.done_audio_prefix)
            transcripts_moved.append(transcript_key)

        move_object(audio_key, settings.done_audio_prefix)

    summary = {
        "new_videos": len(new_videos),
        "audio_extracted": len(extracted),
        "new_audio": len(new_audio),
        "jobs_created": len(job_ids),
        "job_ids": job_ids,
        "transcripts_moved": len(transcripts_moved),
    }

    logger.info(f"Pipeline complete: {summary}")
    return summary


@flow(name="Extract Audio Only", log_prints=True)
def extract_audio_flow() -> dict:
    """
    Extract audio from new videos without launching Whisper jobs.
    Useful for testing the ffmpeg step in isolation.
    """
    logger = get_run_logger()

    new_videos = list_new_videos()
    extracted = []
    for video_key in new_videos:
        paths = build_local_paths(video_key)
        audio_key = video_key_to_audio_key(video_key)
        download_object(video_key, paths["local_video"])
        run_ffmpeg_extraction(paths["local_video"], paths["local_audio"])
        upload_extracted_audio(paths["local_audio"], audio_key)
        move_object(video_key, settings.done_video_prefix)
        extracted.append(audio_key)

    summary = {
        "new_videos": len(new_videos),
        "audio_extracted": extracted,
    }

    logger.info(f"Extraction complete: {summary}")
    return summary


@flow(name="Extract Audio In Cloud", log_prints=True)
async def extract_audio_cloud_flow() -> dict:
    """Extract audio from new videos using a Nebius CPU job."""
    logger = get_run_logger()

    new_videos = list_new_videos()
    extracted = []
    job_ids = []
    for video_key in new_videos:
        job_id = await create_ffmpeg_job(video_key)
        job_ids.append(job_id)
        await wait_for_job_completion(job_id)
        move_object(video_key, settings.done_video_prefix)
        extracted.append(video_key_to_audio_key(video_key))

    summary = {
        "new_videos": len(new_videos),
        "audio_extracted": extracted,
        "jobs_created": len(job_ids),
        "job_ids": job_ids,
    }

    logger.info(f"Cloud extraction complete: {summary}")
    return summary


@flow(name="Fully Cloud Transcription Pipeline", log_prints=True)
async def fully_cloud_pipeline() -> dict:
    """Run ffmpeg extraction on CPU in Nebius, then Whisper on GPU."""
    logger = get_run_logger()

    new_videos = list_new_videos()
    extracted = []
    ffmpeg_job_ids = []
    for video_key in new_videos:
        job_id = await create_ffmpeg_job(video_key)
        ffmpeg_job_ids.append(job_id)
        await wait_for_job_completion(job_id)
        move_object(video_key, settings.done_video_prefix)
        extracted.append(video_key_to_audio_key(video_key))

    if extracted:
        logger.info(f"Cloud-extracted audio from {len(extracted)} videos: {extracted}")

    new_audio = list_new_audio()
    whisper_job_ids = []
    transcripts_moved = []
    for audio_key in new_audio:
        job_id = await create_whisper_job(audio_key)
        whisper_job_ids.append(job_id)
        await wait_for_job_completion(job_id)

        transcript_key = f"{audio_key.rsplit('.', 1)[0]}.txt"
        if object_exists(transcript_key):
            move_object(transcript_key, settings.done_audio_prefix)
            transcripts_moved.append(transcript_key)

        move_object(audio_key, settings.done_audio_prefix)

    summary = {
        "new_videos": len(new_videos),
        "audio_extracted": len(extracted),
        "ffmpeg_jobs_created": len(ffmpeg_job_ids),
        "ffmpeg_job_ids": ffmpeg_job_ids,
        "new_audio": len(new_audio),
        "whisper_jobs_created": len(whisper_job_ids),
        "whisper_job_ids": whisper_job_ids,
        "transcripts_moved": len(transcripts_moved),
    }

    logger.info(f"Fully-cloud pipeline complete: {summary}")
    return summary


@flow(name="Check Bucket", log_prints=True)
def check_bucket() -> dict:
    """
    Lightweight flow: just list what's in the inbox prefixes and report status.
    Useful for testing credentials and seeing what needs processing.
    """
    logger = get_run_logger()

    new_videos = list_new_videos()
    new_audio = list_new_audio()

    summary = {
        "new_videos": new_videos,
        "new_audio": new_audio,
    }

    logger.info(f"Bucket status: {summary}")
    return summary
