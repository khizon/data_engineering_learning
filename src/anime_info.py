import logging
import os
from utils import *

def main():
    try:
        # ingest_top_airing_anime()
        # if os.path.exists(os.path.join(os.getcwd(), 'data', 'top_airing.parquet')):
        #     update_top_airing_anime()
        ingest_anime_info()
        if os.path.exists(os.path.join(os.getcwd(), 'data', 'anime_info.parquet')):
            update_anime_info()

    except Exception as e:
        # Log any exceptions
        logging.error('Error occurred: %s', str(e))

if __name__ == "__main__":
    main()
