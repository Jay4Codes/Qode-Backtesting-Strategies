import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)


def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger


def load_index_data():
    index_data = pd.read_parquet("database/index/nifty_2024.parquet", engine="pyarrow")

    return index_data


def main():
    logger = setup_logger()
    index_data = load_index_data()
    logger.info(index_data.head())


if __name__ == "__main__":
    main()
