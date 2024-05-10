import requests

from requests.auth import HTTPBasicAuth
from load_traces.utils.date_utils import date_to_timestamp_microseconds


def fetch_jaeger_traces(
        server: str,
        service_name: str,
        start_date: str,
        end_date: str,
        limit: int = 20,
        operation: str | None = None,
        username: str | None = None,
        password: str | None = None
):
    """
    Fetch Jaeger traces for a specific service within a given date range.

    This function sends a GET request to the Jaeger API and retrieves the traces for a specific service within a given date range.
    The start and end dates are converted to timestamps in microseconds.
    If a username and password are provided, they are used for basic HTTP authentication.

    Args:
        server (str): The URL of the Jaeger server.
        service_name (str): The name of the service for which to fetch the traces.
        start_date (str): The start date of the date range.
        end_date (str): The end date of the date range.
        limit (int, optional): The maximum number of traces to fetch. Defaults to 20.
        operation (str, optional): The name of the operation for which to fetch the traces. Defaults to None.
        username (str, optional): The username for basic HTTP authentication. Defaults to None.
        password (str, optional): The password for basic HTTP authentication. Defaults to None.

    Returns:
        dict: The JSON response from the Jaeger API.

    Raises:
        requests.exceptions.RequestException: If the GET request to the Jaeger API fails.

    """

    # Convert the start and end dates to timestamps in microseconds
    start_timestamp_micro = date_to_timestamp_microseconds(start_date)
    end_timestamp_micro = date_to_timestamp_microseconds(end_date)

    # Prepare the parameters for the GET request
    params = {
        "service": service_name,
        "start": start_timestamp_micro,
        "end": end_timestamp_micro,
        "limit": limit,
        "operation": operation
    }

    # Prepare the basic HTTP authentication, if a username and password are provided
    auth = HTTPBasicAuth(username, password) if username and password else None

    # Send the GET request to the Jaeger API and return the JSON response
    response = requests.get(url=server, params=params, auth=auth)

    assert response.status_code == 200, f"Failed to fetch Jaeger traces: {response.text}"
    assert response.json().get('data'), f"No traces found for service: {service_name}"

    return response.json()
