import os
import subprocess
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

INPUT_DIR = os.getenv("INPUT_DIR")
OUTPUT_DIR = os.getenv("OUTPUT_DIR")
PROCESSED_LOG = os.path.join(OUTPUT_DIR, "processed_files.txt")

if not INPUT_DIR:
    raise ValueError("INPUT_DIR environment variable is not set.")
if not OUTPUT_DIR:
    raise ValueError("OUTPUT_DIR environment variable is not set.")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def ensure_output_dir():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_processed_files():
    if not os.path.exists(PROCESSED_LOG):
        return set()
    with open(PROCESSED_LOG, "r") as f:
        return set(line.strip() for line in f if line.strip())

def save_processed_file(filepath):
    with open(PROCESSED_LOG, "a") as f:
        f.write(filepath + "\n")

def get_pcap_files(directory):
    try:
        return [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if f.endswith(".pcap") and os.path.isfile(os.path.join(directory, f))
        ]
    except FileNotFoundError:
        logging.warning(f"Input directory '{directory}' does not exist.")
        return []

def convert_pcap_to_ndjson(pcap_path, ndjson_path):
    cmd = ["tshark", "-r", pcap_path, "-T", "ek"]
    try:
        with open(ndjson_path, "w") as out_f:
            subprocess.run(cmd, stdout=out_f, stderr=subprocess.PIPE, check=True)
        logging.info(f"Converted: {pcap_path} -> {ndjson_path}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to convert {pcap_path}: {e.stderr.decode().strip()}")
        return False

def main():
    ensure_output_dir()
    processed = load_processed_files()
    pcap_files = get_pcap_files(INPUT_DIR)
    for pcap_path in pcap_files:
        abs_pcap_path = os.path.abspath(pcap_path)
        if abs_pcap_path in processed:
            continue
        base_name = os.path.splitext(os.path.basename(pcap_path))[0]
        ndjson_path = os.path.join(OUTPUT_DIR, base_name + ".ndjson")
        success = convert_pcap_to_ndjson(pcap_path, ndjson_path)
        if success:
            save_processed_file(abs_pcap_path)

if __name__ == "__main__":
    main()