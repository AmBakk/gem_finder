import os
import zipfile

# os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
# os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')
#
# from kaggle.api.kaggle_api_extended import KaggleApi

KAGGLE_DATASET = 'davidcariboo/player-scores'  # The dataset to download from Kaggle
RAW_DATA_DIR = '../data/raw'

print(f"Downloading dataset '{KAGGLE_DATASET}' into {RAW_DATA_DIR}")
os.system(f'kaggle datasets download -d davidcariboo/player-scores -p {RAW_DATA_DIR}')

zip_file_path = os.path.join(RAW_DATA_DIR, 'player-scores.zip')

# Unzip the dataset into the raw data directory
print(f"Unzipping {zip_file_path} into {RAW_DATA_DIR}")
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(RAW_DATA_DIR)

# Optionally, remove the zip file after extraction
os.remove(zip_file_path)

