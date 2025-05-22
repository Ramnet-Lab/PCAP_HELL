<div align="center">

<img src="./pcap_hell.png" alt="PCAP Hell Banner" style="max-width:100%;"/>

</div>

> **Note:**
> This pipeline is specifically designed for environments where installing standard Elastic ingestion tools such as Filebeat or Logstash is impractical or impossible. It provides an alternative solution for ingesting `.pcap` data into Elasticsearch when those tools cannot be used.

# PCAP-to-Elasticsearch Pipeline

## Purpose
This project implements a robust, event-driven, and concurrent pipeline for processing `.pcap` files and ingesting their contents into Elasticsearch. The system is designed for reliability, modularity, and efficient disk usage, with all configuration loaded from a `.env` file.

## Architecture Overview

The pipeline is composed of modular, independently-operating stages, each responsible for a specific transformation or transfer of data. The system uses an event-driven, monitor/worker model with concurrency at key stages to maximize throughput and responsiveness. Each stage is responsible for its own cleanup and robust logging, ensuring traceability and reliability.

### Pipeline Stages

1. **Monitor (pcap_monitor.py / main.py)**
   - Watches the input directory for new `.pcap` files.
   - Uses a stability check: a file is considered stable if its size does not change for a configurable interval.
   - Enqueues stable files for downstream processing.
   - Logs all file events and errors.
   - Cleans up by only processing files that are stable and not already processed.

2. **Convert (pcap_to_ndjson.py)**
   - Converts each stable `.pcap` file to `.ndjson` using `tshark`.
   - Maintains a persistent log (`processed_files.txt`) to ensure each file is only processed once (idempotency).
   - Logs conversion results and errors.
   - Cleans up by skipping already-processed files and removing temporary artifacts on failure.

3. **Split (ndjson_splitter.py)**
   - Splits large `.ndjson` files into 10,000-line chunk files using the Unix `split` command.
   - Tracks split files in a persistent log (`split.log`) to avoid redundant work.
   - Logs all actions and errors.
   - Cleans up by removing incomplete or failed chunk files and only retaining successfully split chunks.

4. **Distribute (chunk_distributor.py)**
   - Groups chunk files into batches for upload, distributing them across batch directories.
   - Ensures even distribution and prepares files for parallel upload.
   - Logs distribution actions and errors.
   - Cleans up by removing or archiving batch directories after successful upload.

5. **Upload (pcap_es_uploader.py)**
   - Uploads chunk batches to Elasticsearch using concurrent workers.
   - Tracks uploaded files in a persistent log to prevent duplicate ingestion.
   - Logs upload results, errors, and retries.
   - Cleans up by deleting successfully uploaded chunk files and batch directories, minimizing disk usage.

## Configuration and Operational Flow

- All configuration (input/output directories, stability interval, batch size, Elasticsearch nodes, etc.) is loaded from `.env`.
- The monitor detects new `.pcap` files and enqueues them for processing.
- Each file flows through the pipeline: conversion → splitting → distribution → upload.
- At each stage, persistent logs ensure idempotency and safe restarts.
- Cleanup logic at every stage ensures that only necessary files are retained, failed or partial artifacts are removed, and disk usage remains bounded.
- Robust logging at every stage provides traceability and aids in debugging and monitoring.

## Key Design Decisions

- **Event-Driven Concurrency:** The pipeline uses concurrent workers (via `ThreadPoolExecutor` and threads) to process multiple files and batches in parallel, maximizing throughput and responsiveness.
- **Modular Responsibility:** Each stage is implemented as a separate module with a clear interface and responsibility, enabling independent development, testing, and maintenance.
- **Persistent State and Idempotency:** Each stage maintains its own persistent log to ensure that files are only processed once, supporting safe restarts and crash recovery.
- **Explicit Cleanup:** Each stage is responsible for cleaning up its own temporary or processed files, reducing the risk of disk bloat and improving reliability.
- **Robust Logging:** All actions, errors, and state transitions are logged using Python’s `logging` module, supporting observability and troubleshooting.
- **Error Handling:** Errors at each stage are caught, logged, and do not halt the pipeline. Failed files are retried or skipped according to the stage’s logic, ensuring the pipeline remains robust in the face of transient or file-specific issues.

## Cleanup Approach Rationale

The explicit, stage-specific cleanup approach ensures that:
- Disk usage remains bounded, as temporary and processed files are removed as soon as they are no longer needed.
- The system is resilient to crashes and restarts, as persistent logs and cleanup logic prevent duplicate processing and accumulation of stale files.
- Each stage can be debugged and monitored independently, as logs and cleanup are modular and localized.

## Frameworks/Libraries/Tools Used

- **python-dotenv**: Loads environment variables from `.env`.
- **os, time, threading, concurrent.futures, logging, subprocess**: Standard Python libraries for file system access, concurrency, logging, and process management.
- **tshark**: Converts `.pcap` to `.ndjson`.
- **split (Unix)**: Splits large `.ndjson` files into chunks.
- **curl**: Uploads chunk files to Elasticsearch.

## Setup Commands Issued

- `.env` created with all required configuration variables.
- `.gitignore` created to exclude Python artifacts and `.env`.
- `requirements.txt` includes all Python dependencies.

## Deployment/Build Notes

- To install dependencies: `uv pip install -r requirements.txt`
- To run the full pipeline: `python main.py`

## Usage

1. Ensure all configuration variables are set in your `.env` file.
2. Install dependencies:
   ```
   uv pip install -r requirements.txt
   ```
3. Run the pipeline:
   ```
   python main.py
   ```

## Running as a systemd Service

You can run the pipeline as a persistent background service using `systemd`. This allows the orchestrator to start automatically on boot and be managed with `systemctl`.

### Example: `pcap-pipeline.service` unit file

Create a file named `pcap-pipeline.service` with the following content:

```ini
[Unit]
Description=PCAP-to-Elasticsearch Pipeline Orchestrator
After=network.target

[Service]
Type=simple
User=YOUR_USERNAME
WorkingDirectory=/path/to/your/pipeline
EnvironmentFile=/path/to/your/pipeline/.env
ExecStart=/usr/bin/python3 main.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

- **User**: Replace `YOUR_USERNAME` with the user that should run the service.
- **WorkingDirectory**: Set to the directory containing `main.py` and your `.env` file.
- **EnvironmentFile**: Path to your `.env` file with pipeline configuration.
- **ExecStart**: Adjust the Python path if needed (e.g., use a virtual environment).

### Setup Instructions

1. **Copy the service file**
   Save your `pcap-pipeline.service` to `/etc/systemd/system/pcap-pipeline.service` (requires root privileges):

   ```sh
   sudo cp pcap-pipeline.service /etc/systemd/system/pcap-pipeline.service
   ```

2. **Reload systemd to recognize the new service**
   ```sh
   sudo systemctl daemon-reload
   ```

3. **Enable the service to start on boot**
   ```sh
   sudo systemctl enable pcap-pipeline
   ```

4. **Start the service**
   ```sh
   sudo systemctl start pcap-pipeline
   ```

5. **Stop the service**
   ```sh
   sudo systemctl stop pcap-pipeline
   ```

6. **Check service status**
   ```sh
   sudo systemctl status pcap-pipeline
   ```

7. **View service logs**
   ```sh
   journalctl -u pcap-pipeline -f
   ```

### Notes

- The service runs in the specified `WorkingDirectory`. All relative paths (including `.env`) are resolved from here.
- Ensure all dependencies are installed and accessible to the specified user (consider using a virtual environment and updating `ExecStart` accordingly).
- If you update the service file, always run `sudo systemctl daemon-reload` before restarting the service.
