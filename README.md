# Nebius Prefect Pipeline

This project is a small Prefect-based pipeline for processing media stored in Nebius Object Storage. It started as an experiment in orchestrating a transcription workflow without building a pile of custom n8n nodes, and it turned into a useful reference for how Nebius storage, AI Jobs, and Prefect fit together.

At a high level, the pipeline watches a bucket layout that looks like this:

```text
video/          raw video uploads waiting to be processed
audio/          extracted audio waiting to be transcribed
DONE_video/     processed videos
DONE_audio/     processed audio and transcript outputs
```

The current implementation supports two extraction modes. In local mode, the machine running Prefect downloads the video, runs `ffmpeg`, uploads the mp3, and then triggers a Nebius GPU job for Whisper. In cloud mode, a Nebius CPU job runs `ffmpeg` directly against the mounted bucket, writes the result to local job disk first, copies it into `audio/`, and then a Nebius GPU job handles transcription.

## Why This Exists

Nebius has two different integration surfaces. Object operations use the S3-compatible API, while AI Jobs use the Nebius Python SDK and its gRPC-based API. Prefect turned out to be a good fit because the orchestration stays close to ordinary Python: `boto3` for storage, `subprocess` for local `ffmpeg`, and the Nebius SDK for jobs. The project is intentionally simple enough to read like code, not like framework glue.

## What The Flows Do

The main orchestration lives in `src/nebius_pipeline/flows.py`.

There are four flows worth knowing about:

- `check_bucket()` - just tells you what is waiting in `video/` and `audio/`
- `extract_audio_flow()` - local extraction only
- `transcription_pipeline()` - local extraction + cloud Whisper
- `fully_cloud_pipeline()` - cloud ffmpeg extraction + cloud Whisper

The fully-cloud path is the one that best represents where this project is heading. The local path is still useful for development, debugging, and comparing behavior.

## Environment

The project reads its config from `.env` via `pydantic-settings`. The Nebius IAM token is read from `NEBIUS_IAM_TOKEN`, while the rest of the config uses the `NEBIUS_PIPELINE_` prefix.

The easiest way to get started is to copy `.env.example` to `.env` and fill in the real values.

At minimum, you need:

```bash
NEBIUS_IAM_TOKEN="<from: nebius iam get-access-token>"
NEBIUS_PIPELINE_AWS_ACCESS_KEY_ID="<s3 access key>"
NEBIUS_PIPELINE_AWS_SECRET_ACCESS_KEY="<s3 secret key>"
NEBIUS_PIPELINE_NEBIUS_PROJECT_ID="<project-id>"
NEBIUS_PIPELINE_NEBIUS_SUBNET_ID="<subnet-id>"
NEBIUS_PIPELINE_NEBIUS_BUCKET_ID="<bucket-id>"
```

There are sensible defaults in `src/nebius_pipeline/config.py` for bucket name, prefixes, image names, timeout, and disk size. CPU platform and preset may need to be overridden depending on region availability. For example, in `us-central1` you will likely want something like:

```bash
NEBIUS_PIPELINE_CPU_PLATFORM="cpu-d3"
NEBIUS_PIPELINE_CPU_PRESET="4vcpu-16gb"
```

One important detail: `.env` should contain plain `KEY="value"` lines. Do not use `export KEY=...` there.

## Running It

The project uses `uv` and a `justfile` for the common entry points.

To see what is waiting in the bucket:

```bash
just check
```

To extract audio locally:

```bash
just extract
```

To run the hybrid pipeline, where extraction is local but Whisper runs in Nebius:

```bash
just run
```

To run extraction in Nebius CPU jobs:

```bash
just cloud-extract
```

To run the fully-cloud version:

```bash
just cloud-run
```

If you want the Prefect UI locally:

```bash
just dashboard
```

And if you want a scheduled process instead of one-off runs:

```bash
just serve
```

## Resetting State For Testing

There is a helper command for repeatable testing:

```bash
just storage-reset
```

This clears the working prefixes (`video/`, `audio/`, `DONE_video/`, `DONE_audio/`) and copies the root test video back into `video/`. It intentionally asks for confirmation and prints the resolved bucket and endpoint before doing anything, because it uses S3 API commands directly and should never accidentally point at the wrong place.

## A Note On Cloud ffmpeg

The cloud ffmpeg path taught an important lesson. Writing mp3 output directly to the mounted object-storage path was unreliable because ffmpeg finalization behaved like it wanted a more normal filesystem. The robust fix was not a codec flag. It was architectural: write the mp3 to the job's local disk first, then copy the completed file into the mounted bucket path.

That behavior lives in `src/nebius_pipeline/tasks/jobs.py`, in the `create_ffmpeg_job()` path. The mounted bucket is still used for inputs and final outputs, but the actual ffmpeg write happens on local job disk. That is the safer model for media processing on object-storage-backed mounts.

## Project Shape

The code is intentionally split by responsibility:

```text
src/nebius_pipeline/
  config.py         configuration and env parsing
  flows.py          Prefect flow orchestration
  tasks/storage.py  S3-compatible storage tasks
  tasks/extract.py  local ffmpeg tasks
  tasks/jobs.py     Nebius AI Job creation and polling
  __main__.py       CLI entrypoint
```

If you are returning to this later, `flows.py` is the best place to start reading. If you are debugging Nebius AI Job creation, jump straight to `tasks/jobs.py`.

## Current Status

The project is no longer just a proof of concept. The following paths have been exercised:

- local extraction works
- cloud extraction works
- Nebius Whisper job creation works
- transcript write-back works
- fully-cloud pipeline works

The next likely evolution is not more orchestration plumbing. It is improving the container contracts, especially teaching the Whisper container to accept directory or batch-style input so a single GPU job can process multiple files more efficiently.
