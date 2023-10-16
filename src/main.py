import json
import logging
import os
from utils import *

def main():
    try:
        get_keys()
        ingest_top_airing_anime()
        update_top_airing_anime()
        ingest_top_airing_anime()
        update_anime_info()

    except Exception as e:
        # Log any exceptions
        logging.error('Error occurred: %s', str(e))

if __name__ == "__main__":
    main()
