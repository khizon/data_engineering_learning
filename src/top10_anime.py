import logging
import os
from utils import *

def main():
    try:
        update_top10_anime()

    except Exception as e:
        # Log any exceptions
        logging.error('Error occurred: %s', str(e))

if __name__ == "__main__":
    main()
