import os
import sys
import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Configuration
BATCH_DIRS = [
    os.getenv("BATCH_DIR_B1"),
    os.getenv("BATCH_DIR_B2"),
    os.getenv("BATCH_DIR_B3"),
    os.getenv("BATCH_DIR_B4"),
]
ES_NODES = [os.getenv("ELASTICSEARCH_NODE")]  # Single node for now, extendable to list

CHUNK_EXT = ".ndjson"
LOG_FILENAME = "upload.log"
MAX_PARALLEL_UPLOADS = 8  # Tune as needed

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)

def read_uploaded_files(log_path):
    """Read the set of already uploaded files from the log."""
    if not log_path.exists():
        return set()
    with log_path.open("r") as f:
        return set(line.strip() for line in f if line.strip())

def append_uploaded_file(log_path, filename):
    """Append a filename to the upload log."""
    with log_path.open("a") as f:
        f.write(f"{filename}\n")

def upload_chunk(chunk_path, es_node, log_path):
    """Upload a chunk file to Elasticsearch using curl."""
    url = f"{es_node}/pcap/_bulk"
    cmd = [
        "curl",
        "-H", "Content-Type: application/x-ndjson",
        "-XPOST", url,
        "--data-binary", f"@{chunk_path}"
    ]
    logging.info(f"Uploading {chunk_path} to {url}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode == 0:
            append_uploaded_file(log_path, chunk_path.name)
            logging.info(f"Uploaded {chunk_path} successfully.")
            return (chunk_path, True, "")
        else:
            logging.error(f"Failed to upload {chunk_path}: {result.stderr.strip()}")
            return (chunk_path, False, result.stderr.strip())
    except Exception as e:
        logging.error(f"Exception uploading {chunk_path}: {e}")
        return (chunk_path, False, str(e))

def process_batch_dir(batch_dir, es_nodes):
    """Process all chunk files in a batch directory."""
    batch_path = Path(batch_dir)
    log_path = batch_path / LOG_FILENAME
    uploaded = read_uploaded_files(log_path)
    chunk_files = sorted([p for p in batch_path.glob(f"*{CHUNK_EXT}") if p.is_file()])
    to_upload = [p for p in chunk_files if p.name not in uploaded]

    if not to_upload:
        logging.info(f"No new chunks to upload in {batch_dir}")
        return

    logging.info(f"Found {len(to_upload)} new chunks in {batch_dir}")

    # Round-robin node assignment
    node_count = len(es_nodes)
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_UPLOADS) as executor:
        futures = []
        for idx, chunk_path in enumerate(to_upload):
            es_node = es_nodes[idx % node_count]
            futures.append(executor.submit(upload_chunk, chunk_path, es_node, log_path))
        uploaded_chunks = []
        for future in as_completed(futures):
            chunk_path, success, error = future.result()
            if success:
                uploaded_chunks.append(chunk_path)
            else:
                logging.error(f"Upload failed for {chunk_path}: {error}")

        # Cleanup: delete successfully uploaded chunk files
        for chunk_path in uploaded_chunks:
            try:
                os.remove(chunk_path)
                logging.info(f"Deleted uploaded chunk file: {chunk_path}")
            except Exception as del_err:
                logging.error(f"Failed to delete chunk file {chunk_path}: {del_err}")

def main():
    for batch_dir in BATCH_DIRS:
        if not batch_dir or not Path(batch_dir).is_dir():
            logging.warning(f"Batch directory {batch_dir} does not exist or is not set.")
            continue
        process_batch_dir(batch_dir, ES_NODES)
    logging.info("All batch directories processed.")

if __name__ == "__main__":
    main()