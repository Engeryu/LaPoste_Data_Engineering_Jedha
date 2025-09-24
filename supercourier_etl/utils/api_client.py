# supercourier_etl/utils/api_client.py
"""
    
"""
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

class WeatherAPIClient:
    """A client for fetching historical weather data from WeatherAPI."""
    BASE_URL = "http://api.weatherapi.com/v1/history.json"

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("API key cannot be empty.")
        self.api_key = api_key

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def get_historical_weather(self, location: str, date: str) -> dict:
        """
        Fetches hourly historical weather data for a specific location and date.
        Retries up to 3 times in case of transient network errors.

        Args:
            location: The location query (e.g., "Paris").
            date: The date in "YYYY-MM-DD" format.

        Returns:
            A dictionary containing the API response.
        """
        params = {
            "key": self.api_key,
            "q": location,
            "dt": date
        }
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data for {date} at {location}: {e}")

            return {}
