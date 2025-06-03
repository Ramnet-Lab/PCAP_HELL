import os
import shutil
import logging
from dotenv import load_dotenv
from collections import defaultdict
from typing import cast

# Load environment variables from .env
load_dotenv()

_split_dir_raw = os.getenv("SPLIT_DIR")
if not _split_dir_raw:
    raise EnvironmentError("SPLIT_DIR environment variable must be set and non-empty.")
SPLIT_DIR: str = _split_dir_raw  # type: ignore

batch_env_vars = [
    "BATCH_DIR_B1",
    "BATCH_DIR_B2",
    "BATCH_DIR_B3",
    "BATCH_DIR_B4",
]
batch_dirs_raw = []
for var in batch_env_vars:
    val = os.getenv(var)
    if not val:
        raise EnvironmentError(f"{var} environment variable must be set and non-empty.")
    batch_dirs_raw.append(cast(str, val))

# Collect batch directories, filtering out any that are None or empty (should not happen now)
BATCH_DIRS = [d for d in batch_dirs_raw if d]
BATCH_DIRS: list[str]  # type hint for static checkers
BATCH_LOG = os.path.join(SPLIT_DIR, "batch.log")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(SPLIT_DIR, "chunk_distributor.log")),
        logging.StreamHandler()
    ]
)

def ensure_directories():
    if not os.path.isdir(SPLIT_DIR):
        logging.error(f"SPLIT_DIR does not exist: {SPLIT_DIR}")
        raise FileNotFoundError(f"SPLIT_DIR does not exist: {SPLIT_DIR}")
    if len(BATCH_DIRS) != 4:
        logging.error(f"Exactly 4 batch directories must be configured. Found: {BATCH_DIRS}")
        raise ValueError(f"Exactly 4 batch directories must be configured. Found: {BATCH_DIRS}")
    for d in BATCH_DIRS:
        if not os.path.isdir(d):
            try:
                os.makedirs(d, exist_ok=True)
                logging.info(f"Created batch directory: {d}")
            except Exception as e:
                logging.error(f"Failed to create batch directory {d}: {e}")
                raise

def load_distributed_chunks():
    distributed = set()
    if os.path.exists(BATCH_LOG):
        with open(BATCH_LOG, "r") as f:
            for line in f:
                chunk = line.strip()
                if chunk:
                    distributed.add(chunk)
    return distributed

def log_distributed_chunk(chunk_path):
    with open(BATCH_LOG, "a") as f:
        f.write(chunk_path + "\n")

def group_chunks_by_base(split_dir):
    """
    Returns a dict: {base: [chunk_file1, chunk_file2, ...]}
    where base is the NDJSON base (without .chunk_XXXX)
    """
    chunks = defaultdict(list)
    for fname in os.listdir(split_dir):
        if ".chunk_" in fname:
            base = fname.split(".chunk_")[0]
            chunks[base].append(fname)
    return chunks

def distribute_chunks(chunk_files):
    """
    Distributes a list of chunk files as evenly as possible into 4 batches.
    Returns a list of lists: [b1_files, b2_files, b3_files, b4_files]
    """
    chunk_files_sorted = sorted(chunk_files)
    n = len(chunk_files_sorted)
    base_count = n // 4
    extras = n % 4
    batches = []
    idx = 0
    for i in range(4):
        count = base_count + (1 if i < extras else 0)
        batches.append(chunk_files_sorted[idx:idx+count])
        idx += count
    return batches

def main():
    ensure_directories()
    distributed = load_distributed_chunks()
    chunks_by_base = group_chunks_by_base(SPLIT_DIR)

    for base, chunk_files in chunks_by_base.items():
        # Filter out already distributed chunks
        undistributed = [f for f in chunk_files if os.path.join(SPLIT_DIR, f) not in distributed]
        if not undistributed:
            logging.info(f"All chunks for base '{base}' already distributed.")
            continue

        # Round-robin assignment of undistributed chunks to batch directories
        undistributed_sorted = sorted(undistributed)
        num_batches = len(BATCH_DIRS)
        for idx, fname in enumerate(undistributed_sorted):
            batch_dir = BATCH_DIRS[idx % num_batches]
            src = os.path.join(SPLIT_DIR, fname)
            dst = os.path.join(batch_dir, fname)
            try:
                shutil.move(src, dst)
                log_distributed_chunk(src)
                logging.info(f"Assigned chunk '{src}' to batch directory '{batch_dir}' (idx={idx}) and moved to '{dst}'")
            except Exception as e:
                logging.error(f"Failed to move {src} to {dst}: {e}")

if __name__ == "__main__":
    try:
        main()
        logging.info("Chunk distribution completed successfully.")
    except Exception as e:
        logging.error(f"Chunk distribution failed: {e}")