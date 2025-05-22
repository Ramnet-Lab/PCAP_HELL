import os
import time
import threading
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

INPUT_DIR = os.getenv("INPUT_DIR")
STABILITY_INTERVAL = int(os.getenv("STABILITY_INTERVAL", "10"))

if not INPUT_DIR:
    raise ValueError("INPUT_DIR environment variable is not set.")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def get_pcap_files(directory):
    """Return a set of .pcap file paths in the directory."""
    try:
        return {
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if f.endswith(".pcap") and os.path.isfile(os.path.join(directory, f))
        }
    except FileNotFoundError:
        logging.warning(f"Input directory '{directory}' does not exist.")
        return set()

def is_file_stable(filepath, interval):
    """Check if file size remains unchanged for the given interval (in seconds)."""
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

def monitor_directory(input_dir, stability_interval):
    """Continuously monitor the directory for new stable .pcap files."""
    processed_files = set()
    logging.info(f"Monitoring directory: {input_dir} (stability interval: {stability_interval}s)")
    while True:
        pcap_files = get_pcap_files(input_dir)
        new_files = pcap_files - processed_files
        for filepath in new_files:
            if is_file_stable(filepath, stability_interval):
                logging.info(f"Stable .pcap file detected: {filepath}")
                processed_files.add(filepath)
        time.sleep(1)

def main():
    # Ensure input directory exists
    if not os.path.isdir(INPUT_DIR):
        logging.error(f"Input directory '{INPUT_DIR}' does not exist.")
        return
    monitor_thread = threading.Thread(
        target=monitor_directory,
        args=(INPUT_DIR, STABILITY_INTERVAL),
        daemon=True
    )
    monitor_thread.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Shutting down monitor.")

if __name__ == "__main__":
    main()