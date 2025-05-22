import logging
import sys
import os
import time
import threading
from pathlib import Path
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load .env configuration
try:
    from dotenv import load_dotenv
except ImportError:
    print("python-dotenv is required. Install with 'uv pip install python-dotenv'")
    sys.exit(1)

def load_env():
    env_path = Path('.') / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        logging.info("Loaded configuration from .env")
    else:
        logging.warning(".env file not found. Proceeding with environment variables.")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Import pipeline modules
try:
    import pcap_to_ndjson
    import ndjson_splitter
    import chunk_distributor
    import pcap_es_uploader
except ImportError as e:
    logging.error(f"Failed to import pipeline module: {e}")
    sys.exit(1)

# Configuration (loaded after .env)
INPUT_DIR = None
OUTPUT_DIR = None
SPLIT_DIR = None
BATCH_DIRS = []
STABILITY_INTERVAL = 10
MAX_WORKERS = 10

def init_config():
    global INPUT_DIR, OUTPUT_DIR, SPLIT_DIR, BATCH_DIRS, STABILITY_INTERVAL, MAX_WORKERS
    INPUT_DIR = os.getenv("INPUT_DIR")
    OUTPUT_DIR = os.getenv("OUTPUT_DIR")
    SPLIT_DIR = os.getenv("SPLIT_DIR")
    BATCH_DIRS = [
        os.getenv("BATCH_DIR_B1"),
        os.getenv("BATCH_DIR_B2"),
        os.getenv("BATCH_DIR_B3"),
        os.getenv("BATCH_DIR_B4"),
    ]
    STABILITY_INTERVAL = int(os.getenv("STABILITY_INTERVAL", "10"))
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
    if not INPUT_DIR or not OUTPUT_DIR or not SPLIT_DIR or not all(BATCH_DIRS):
        logging.error("One or more required environment variables are missing.")
        sys.exit(1)

def is_file_stable(filepath, interval):
    try:
        prev_size = os.path.getsize(filepath)
    except OSError:
        return False
    time.sleep(interval)
    try:
        curr_size = os.path.getsize(filepath)
    except OSError:
        return False
    return prev_size == curr_size

def monitor_directory(input_dir, stability_interval, file_queue, stop_event):
    processed_files = set()
    logging.info(f"Monitoring directory: {input_dir} (stability interval: {stability_interval}s)")
    while not stop_event.is_set():
        try:
            pcap_files = {
                os.path.join(input_dir, f)
                for f in os.listdir(input_dir)
                if f.endswith(".pcap") and os.path.isfile(os.path.join(input_dir, f))
            }
        except FileNotFoundError:
            logging.warning(f"Input directory '{input_dir}' does not exist.")
            time.sleep(2)
            continue
        new_files = pcap_files - processed_files
        for filepath in new_files:
            if is_file_stable(filepath, stability_interval):
                logging.info(f"Stable .pcap file detected: {filepath}")
                file_queue.put(filepath)
                processed_files.add(filepath)
        time.sleep(1)

def get_base_name(path):
    return os.path.splitext(os.path.basename(path))[0]

def find_chunk_files_for_base(split_dir, base):
    # Returns list of chunk files for a given NDJSON base
    return sorted([
        os.path.join(split_dir, f)
        for f in os.listdir(split_dir)
        if f.startswith(base + ".chunk_")
    ])

def distribute_chunks_for_base(split_dir, batch_dirs, base):
    chunk_files = find_chunk_files_for_base(split_dir, base)
    if not chunk_files:
        logging.error(f"No chunk files found for base {base} in {split_dir}")
        return []
    # Use the same distribution logic as chunk_distributor
    n = len(chunk_files)
    base_count = n // 4
    extras = n % 4
    batches = []
    idx = 0
    for i in range(4):
        count = base_count + (1 if i < extras else 0)
        batches.append(chunk_files[idx:idx+count])
        idx += count
    moved = []
    for i, batch in enumerate(batches):
        batch_dir = batch_dirs[i]
        for src in batch:
            dst = os.path.join(batch_dir, os.path.basename(src))
            try:
                os.makedirs(batch_dir, exist_ok=True)
                if os.path.exists(dst):
                    logging.warning(f"Destination {dst} already exists. Overwriting.")
                    os.remove(dst)
                os.rename(src, dst)
                moved.append(dst)
                logging.info(f"Moved {src} -> {dst}")
            except Exception as e:
                logging.error(f"Failed to move {src} to {dst}: {e}")
    return moved

def upload_chunks_for_base(batch_dirs, base):
    # For each batch dir, upload all chunk files for this base
    for batch_dir in batch_dirs:
        batch_path = Path(batch_dir)
        chunk_files = sorted([p for p in batch_path.glob(f"{base}.chunk_*") if p.is_file()])
        if not chunk_files:
            continue
        log_path = batch_path / pcap_es_uploader.LOG_FILENAME
        es_nodes = pcap_es_uploader.ES_NODES
        with ThreadPoolExecutor(max_workers=pcap_es_uploader.MAX_PARALLEL_UPLOADS) as executor:
            futures = []
            for idx, chunk_path in enumerate(chunk_files):
                es_node = es_nodes[idx % len(es_nodes)]
                futures.append(executor.submit(
                    pcap_es_uploader.upload_chunk, chunk_path, es_node, log_path
                ))
            for future in as_completed(futures):
                chunk_path, success, error = future.result()
                if not success:
                    logging.error(f"Upload failed for {chunk_path}: {error}")

def process_pcap_file(pcap_path):
    thread_name = threading.current_thread().name
    base = get_base_name(pcap_path)
    ndjson_path = os.path.join(OUTPUT_DIR, base + ".ndjson")
    logging.info(f"[WORKER START] Processing file: {pcap_path}")

    try:
        # 1. Convert to NDJSON
        logging.info(f"[{thread_name}] [STAGE] Converting to NDJSON: {pcap_path} -> {ndjson_path}")
        success = pcap_to_ndjson.convert_pcap_to_ndjson(pcap_path, ndjson_path)
        if not success:
            logging.error(f"[{thread_name}] [ERROR] Failed to convert {pcap_path} to NDJSON.")
            return
        logging.info(f"[{thread_name}] [STAGE COMPLETE] NDJSON conversion done: {ndjson_path}")

        # 2. Split NDJSON into chunks
        from pathlib import Path as _Path
        logging.info(f"[{thread_name}] [STAGE] Splitting NDJSON into chunks: {ndjson_path}")
        split_success = ndjson_splitter.split_ndjson_file(_Path(ndjson_path), _Path(SPLIT_DIR))
        if not split_success:
            logging.error(f"[{thread_name}] [ERROR] Failed to split NDJSON {ndjson_path}.")
            return
        logging.info(f"[{thread_name}] [STAGE COMPLETE] NDJSON split done: {SPLIT_DIR}")
    
        # 3. Distribute chunks into batch folders
        logging.info(f"[{thread_name}] [STAGE] Distributing chunks for base: {base}")
        moved_chunks = distribute_chunks_for_base(SPLIT_DIR, BATCH_DIRS, base)
        if not moved_chunks:
            logging.error(f"[{thread_name}] [ERROR] No chunks distributed for base {base}.")
            return
        logging.info(f"[{thread_name}] [STAGE COMPLETE] Chunks distributed for base: {base}")
    
        # 4. Upload chunks to Elasticsearch
        logging.info(f"[{thread_name}] [STAGE] Uploading chunks to Elasticsearch for base: {base}")
        upload_chunks_for_base(BATCH_DIRS, base)
        logging.info(f"[{thread_name}] [STAGE COMPLETE] Upload to Elasticsearch done for base: {base}")
    
        logging.info(f"[WORKER FINISH] Completed pipeline for {pcap_path}")
        # Delete the original .pcap file after successful pipeline completion
        try:
            os.remove(pcap_path)
            logging.info(f"[CLEANUP] Deleted original .pcap file: {pcap_path}")
        except Exception as del_err:
            logging.error(f"[CLEANUP ERROR] Failed to delete {pcap_path}: {del_err}")
    except Exception as e:
        logging.error(f"[{thread_name}] [EXCEPTION] Error processing {pcap_path}: {e}", exc_info=True)

def main():
    load_env()
    init_config()
    file_queue = Queue()
    stop_event = threading.Event()

    # Start monitor thread
    monitor_thread = threading.Thread(
        target=monitor_directory,
        args=(INPUT_DIR, STABILITY_INTERVAL, file_queue, stop_event),
        name="MonitorThread",
        daemon=True
    )
    monitor_thread.start()

    # Start worker pool
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = set()
        try:
            while True:
                try:
                    pcap_path = file_queue.get(timeout=1)
                except Empty:
                    if not monitor_thread.is_alive():
                        break
                    continue
                future = executor.submit(process_pcap_file, pcap_path)
                futures.add(future)
                # Clean up completed futures
                done = {f for f in futures if f.done()}
                futures -= done
        except KeyboardInterrupt:
            logging.info("Received KeyboardInterrupt. Shutting down.")
        finally:
            stop_event.set()
            monitor_thread.join(timeout=5)
            logging.info("Monitor thread stopped.")
            # Wait for all workers to finish
            for f in as_completed(futures):
                pass
            logging.info("All workers completed.")

if __name__ == "__main__":
    main()