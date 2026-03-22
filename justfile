# Nebius Transcription Pipeline - Prefect

root_video := "caching_strategies_rough_cut_v1_small.mp4"

# Check bucket status (what needs processing)
check:
    uv run python -m nebius_pipeline check

# Extract audio from new videos only (local ffmpeg)
extract:
    uv run python -m nebius_pipeline extract

# Extract audio from new videos using Nebius CPU jobs
cloud-extract:
    uv run python -m nebius_pipeline cloud-extract

# Run the full transcription pipeline once (extract + transcribe)
run:
    uv run python -m nebius_pipeline run

# Run the full cloud pipeline once (CPU ffmpeg + GPU Whisper)
cloud-run:
    uv run python -m nebius_pipeline cloud-run

# Serve the pipeline on a 15-minute schedule
serve:
    uv run python -m nebius_pipeline serve

# Start the Prefect UI dashboard
dashboard:
    uv run prefect server start

# Sync dependencies
sync:
    uv sync

# Lint with ruff
lint:
    uv run ruff check src/

# Format with ruff
fmt:
    uv run ruff format src/

# Reset Nebius storage prefixes for testing
storage-reset:
    #!/usr/bin/env bash
    set -euo pipefail
    set -a
    source .env
    set +a

    bucket="${NEBIUS_PIPELINE_NEBIUS_BUCKET:-darko-mesaros-videos}"
    endpoint="${NEBIUS_PIPELINE_NEBIUS_ENDPOINT:-https://storage.us-central1.nebius.cloud}"
    region="${NEBIUS_PIPELINE_NEBIUS_REGION:-us-central1}"
    root_key={{root_video}}

    export AWS_ACCESS_KEY_ID="$NEBIUS_PIPELINE_AWS_ACCESS_KEY_ID"
    export AWS_SECRET_ACCESS_KEY="$NEBIUS_PIPELINE_AWS_SECRET_ACCESS_KEY"
    export AWS_REGION="$region"

    printf 'About to reset storage prefixes in:\n'
    printf '  bucket:   %s\n' "$bucket"
    printf '  endpoint: %s\n\n' "$endpoint"
    read -r -p 'Is this the correct Nebius endpoint? [y/N]: ' confirm
    if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
        printf 'Aborted.\n'
        exit 1
    fi

    tmpdir=$(mktemp -d)
    trap 'rm -rf "$tmpdir"' EXIT
    local_file="$tmpdir/$root_key"

    aws --endpoint-url "$endpoint" --region "$region" s3 rm "s3://$bucket/video/" --recursive
    aws --endpoint-url "$endpoint" --region "$region" s3 rm "s3://$bucket/audio/" --recursive
    aws --endpoint-url "$endpoint" --region "$region" s3 rm "s3://$bucket/DONE_video/" --recursive
    aws --endpoint-url "$endpoint" --region "$region" s3 rm "s3://$bucket/DONE_audio/" --recursive

    if aws --endpoint-url "$endpoint" --region "$region" s3api copy-object \
        --bucket "$bucket" \
        --key "video/$root_key" \
        --copy-source "$bucket/$root_key" \
        --tagging-directive REPLACE \
        --tagging "" > /dev/null 2>&1; then
        printf 'Used server-side copy for %s -> video/%s\n' "$root_key" "$root_key"
    else
        printf 'Server-side copy failed, falling back to download/upload for %s\n' "$root_key"
        aws --endpoint-url "$endpoint" --region "$region" s3 cp "s3://$bucket/$root_key" "$local_file"
        aws --endpoint-url "$endpoint" --region "$region" s3 cp "$local_file" "s3://$bucket/video/$root_key"
    fi

    printf 'Reset complete. Root preserved, copied %s to video/, cleared video/audio/DONE_ prefixes.\n' "$root_key"
