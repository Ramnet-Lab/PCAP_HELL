import os
import sys
import logging
from dotenv import load_dotenv
from pathlib import Path
import subprocess

def setup_logging(split_dir: Path):
    log_file = split_dir / "ndjson_splitter.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

def load_config():
    load_dotenv()
    ndjson_dir = os.getenv("OUTPUT_DIR")
    split_dir = os.getenv("SPLIT_DIR")
    batch_size = int(os.getenv("NDJSON_BATCH_SIZE", "1000"))
    if not ndjson_dir or not split_dir:
        logging.error("Both OUTPUT_DIR and SPLIT_DIR must be set in the .env file.")
        sys.exit(1)
    return Path(ndjson_dir), Path(split_dir), batch_size

def read_split_log(split_dir: Path):
    log_path = split_dir / "split.log"
    if not log_path.exists():
        return set()
    with open(log_path, "r") as f:
        return set(line.strip() for line in f if line.strip())

def append_to_split_log(split_dir: Path, base_name: str):
    log_path = split_dir / "split.log"
    with open(log_path, "a") as f:
        f.write(base_name + "\n")

def split_ndjson_file(ndjson_path: Path, split_dir: Path, batch_size: int = 1000):
    base = ndjson_path.stem
    chunk_prefix = split_dir / f"{base}.chunk_"
    cmd = [
        "split",
        "-l", str(batch_size),
        "-a", "4",
        str(ndjson_path),
        str(chunk_prefix)
    ]
    logging.info(f"Splitting {ndjson_path} into chunks of {batch_size} lines with prefix {chunk_prefix}")
    try:
        subprocess.run(cmd, check=True)
        logging.info(f"Successfully split {ndjson_path}")
        try:
            os.remove(ndjson_path)
            logging.info(f"Deleted original NDJSON file: {ndjson_path}")
        except Exception as del_err:
            logging.error(f"Failed to delete {ndjson_path}: {del_err}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to split {ndjson_path}: {e}")
        return False

def ensure_dir(path: Path):
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)

def main():
    ndjson_dir, split_dir, batch_size = load_config()
    ensure_dir(split_dir)
    setup_logging(split_dir)
    split_log = read_split_log(split_dir)

    ndjson_files = sorted(ndjson_dir.glob("*.ndjson"))
    if not ndjson_files:
        logging.info(f"No .ndjson files found in {ndjson_dir}")
        return

    for ndjson_path in ndjson_files:
        base = ndjson_path.stem
        if base in split_log:
            logging.info(f"Skipping {ndjson_path} (already split)")
            continue
        success = split_ndjson_file(ndjson_path, split_dir, batch_size)
        if success:
            append_to_split_log(split_dir, base)

if __name__ == "__main__":
    main()