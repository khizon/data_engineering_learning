import json
import logging
import os

# Configure the logging settings
os.makedirs('../logs', exist_ok=True)
logging.basicConfig(filename='test.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    try:
        # Read the contents of secrets/keys.json
        with open('keys.json', 'r') as file:
            keys_data = json.load(file)

        # Log the contents to data/test.log
        logging.info('Contents of keys.json: %s', json.dumps(keys_data, indent=2))

        # Your additional code logic here, if needed

    except Exception as e:
        # Log any exceptions
        logging.error('Error occurred: %s', str(e))

if __name__ == "__main__":
    main()
