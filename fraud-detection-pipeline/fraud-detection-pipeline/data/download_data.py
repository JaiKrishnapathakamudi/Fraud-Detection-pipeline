"""
download_data.py
-----------------
Downloads the PaySim Synthetic Mobile Money Transaction dataset from Kaggle.

Requirements:
    pip install kaggle
    A Kaggle API token placed at ~/.kaggle/kaggle.json
    (Account -> Settings -> API -> Create New Token on kaggle.com)

Usage:
    python data/download_data.py
"""

import os
import subprocess
import sys
import zipfile

KAGGLE_DATASET = "ealaxi/paysim1"
TARGET_DIR = os.path.dirname(os.path.abspath(__file__))
ZIP_NAME = "paysim1.zip"


def main():
    kaggle_json = os.path.expanduser("~/.kaggle/kaggle.json")
    if not os.path.exists(kaggle_json):
        print(
            "ERROR: Kaggle API credentials not found at ~/.kaggle/kaggle.json\n"
            "1. Go to https://www.kaggle.com/settings/account\n"
            "2. Click 'Create New Token' to download kaggle.json\n"
            "3. Move it to ~/.kaggle/kaggle.json and run: chmod 600 ~/.kaggle/kaggle.json"
        )
        sys.exit(1)

    print(f"Downloading dataset '{KAGGLE_DATASET}' into {TARGET_DIR} ...")
    subprocess.run(
        ["kaggle", "datasets", "download", "-d", KAGGLE_DATASET, "-p", TARGET_DIR],
        check=True,
    )

    zip_path = os.path.join(TARGET_DIR, ZIP_NAME)
    if not os.path.exists(zip_path):
        # kaggle sometimes names the zip after the dataset instead
        candidates = [f for f in os.listdir(TARGET_DIR) if f.endswith(".zip")]
        if candidates:
            zip_path = os.path.join(TARGET_DIR, candidates[0])

    print(f"Extracting {zip_path} ...")
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(TARGET_DIR)

    print("Done. Look for 'PS_20174392719_1491204439457_log.csv' in the data/ folder.")
    print("Tip: rename it to data/paysim.csv for the rest of the pipeline scripts.")


if __name__ == "__main__":
    main()
