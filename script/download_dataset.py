"""Download the Flight Price Dataset of Bangladesh from Kaggle into data/raw/."""

import shutil
from pathlib import Path

import kagglehub


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
DEST = RAW_DIR / "Flight_Price_Dataset_of_Bangladesh.csv"


def main() -> None:
    print("Downloading dataset from Kaggle...")

    path = kagglehub.dataset_download(
        "mahatiratusher/flight-price-dataset-of-bangladesh"
    )

    print(f"Dataset downloaded to: {path}")

    src_dir = Path(path)
    csv_files = list(src_dir.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV file found in downloaded dataset: {src_dir}"
        )

    src_csv = csv_files[0]

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src_csv, DEST)

    print(f"Dataset copied to: {DEST}")


if __name__ == "__main__":
    main()