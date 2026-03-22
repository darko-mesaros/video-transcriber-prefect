"""
Entry point for the transcription pipeline.

Usage:
    uv run python -m nebius_pipeline check       # Check what needs processing
    uv run python -m nebius_pipeline extract      # Extract audio from new videos only
    uv run python -m nebius_pipeline cloud-extract # Extract audio in Nebius CPU jobs
    uv run python -m nebius_pipeline run          # Full pipeline (extract + transcribe)
    uv run python -m nebius_pipeline cloud-run    # Full cloud pipeline (CPU ffmpeg + GPU Whisper)
    uv run python -m nebius_pipeline serve        # Serve on a 15-minute schedule
"""

import sys

from nebius_pipeline.flows import (
    transcription_pipeline,
    extract_audio_flow,
    extract_audio_cloud_flow,
    fully_cloud_pipeline,
    check_bucket,
)


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "check"

    if command == "check":
        result = check_bucket()
        print(f"\nResult: {result}")

    elif command == "extract":
        result = extract_audio_flow()
        print(f"\nResult: {result}")

    elif command == "cloud-extract":
        import asyncio

        result = asyncio.run(extract_audio_cloud_flow())
        print(f"\nResult: {result}")

    elif command == "run":
        import asyncio

        result = asyncio.run(transcription_pipeline())
        print(f"\nResult: {result}")

    elif command == "cloud-run":
        import asyncio

        result = asyncio.run(fully_cloud_pipeline())
        print(f"\nResult: {result}")

    elif command == "serve":
        transcription_pipeline.serve(
            name="nebius-transcription",
            cron="*/15 * * * *",
            tags=["nebius", "transcription", "gpu"],
        )

    else:
        print(f"Unknown command: {command}")
        print(
            "Usage: python -m nebius_pipeline [check|extract|cloud-extract|run|cloud-run|serve]"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
