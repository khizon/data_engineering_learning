import json
import logging
import os
from utils import *

# Configure the logging settings
os.makedirs('logs', exist_ok=True)
logging.basicConfig(filename='logs/test.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    try:
        get_keys()

    except Exception as e:
        # Log any exceptions
        logging.error('Error occurred: %s', str(e))

if __name__ == "__main__":
    main()
