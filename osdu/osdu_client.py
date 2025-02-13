import json
import os
import requests

from config.logger_config import logger
from config.project_config import PROJECT_CONFIG

class OSDUClient:
    def __init__(self):
        config_path = "config/osdu_config.json"
        """
        Initializes the OSDUClient by loading configuration from a JSON file.

        Parameters:
        - config_path (str): Path to the configuration file. Default is 'config/config.json'.
        """
        self.base_url = None
        self.headers = None
        self.load_config(config_path)

    def load_config(self, config_path):
        """
        Loads configuration from the specified JSON file.

        Parameters:
        - config_path (str): Path to the configuration file.

        Raises:
        - FileNotFoundError: If the configuration file is not found.
        - KeyError: If required keys are missing in the configuration.
        """
        try:
            # Check if the file exists
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Configuration file not found: {config_path}")

            # Load the configuration file
            with open(config_path, 'r') as file:
                config = json.load(file)

            # Extract required fields
            self.base_url = config.get('base_url')
            self.headers = config.get('headers')

            # Validate that required keys are present
            if not self.base_url or not self.headers:
                raise KeyError("Missing required configuration keys: 'base_url' or 'headers'")

        except FileNotFoundError as e:
            print(f"Error: {e}")
            raise
        except KeyError as e:
            print(f"Error: {e}")
            raise
        except json.JSONDecodeError as e:
            print(f"Error parsing configuration file: {e}")
            raise



    def search(self, payload):
        """
        Sends a POST request to the OSDU search API.

        Parameters:
        - payload (dict): The complete search query payload.

        Returns:
        - dict: Parsed JSON response from the API.

        Raises:
        - requests.exceptions.RequestException: If the API request fails.
        - json.JSONDecodeError: If the response cannot be parsed as JSON.
        - Exception: For any unexpected errors.
        """
        try:
            # Construct the query URL (make it configurable if needed)
            query_url = f"{self.base_url}/api/search/v2/query"

            # Log only a partial payload to avoid exposing sensitive data
            logger.info(
                f"Sending search request to {query_url} with limited payload preview: {json.dumps(payload)[:200]}...")

            # Send the POST request
            response = requests.post(query_url, headers=self.headers, json=payload)

            # Handle HTTP errors
            if response.status_code != 200:
                logger.error(f"Error: Received status code {response.status_code}, Response: {response.text}")
                response.raise_for_status()

            # Parse the response as JSON
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Request to OSDU API failed: {e}")
            raise

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            logger.error(f"Raw response: {response.text if response else 'No response received'}")
            raise

        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise

    def crs_converter(self, from_crs, to_crs, points):
        """
        Converts CRS coordinates using the API.

        Parameters:
        - from_crs (str): The source CRS in the required format.
        - points (list): List of points to be converted.

        Returns:
        - dict: Parsed JSON response from the API.

        Raises:
        - Exception: If the API request fails or returns a non-200 status code.
        """
        try:
            # Define the API endpoint
            query_url = f"{self.base_url}/api/crs/converter/v2/convert"

            # Prepare the payload
            payload = {
                "fromCRS": from_crs,
                "toCRS": to_crs,
                "points": points
            }

            # Send the POST request
            response = requests.post(query_url, headers=self.headers, json=payload)

            # Check for HTTP errors
            if response.status_code != 200:
                print(f"Error: Received status code {response.status_code}")
                print(f"Response: {response.text}")
                response.raise_for_status()

            # Parse the response as JSON
            return response.json()

        except requests.exceptions.RequestException as e:
            # Handle network-related errors (e.g., connection issues, timeouts)
            print(f"Request failed: {e}")
            raise

        except ValueError as e:
            # Handle JSON decoding errors
            print(f"Failed to parse JSON response: {e}")
            print(f"Raw response: {response.text if response else 'No response'}")
            raise

        except Exception as e:
            # Handle any other unexpected errors
            print(f"An unexpected error occurred: {e}")
            raise
