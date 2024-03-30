from datetime import timedelta, datetime


def get_date_strings() -> tuple[str, str]:
    """
    Get start and end date strings in "YYYY-MM-DD HH:MM:SS" format.

    This function calculates the current date and time (end date) and the date and time one day ago (start date).
    It then formats these dates and times as strings in the "YYYY-MM-DD HH:MM:SS" format.

    Returns:
        tuple[str, str]: A tuple containing the start and end date strings.

    Examples:
        >>> get_date_strings()
        ('2022-03-14 15:26:37', '2022-03-15 15:26:37')
    """

    # Calculate the current date and time
    end_date = datetime.now()

    # Calculate the date and time one day ago
    start_date = end_date - timedelta(days=1)

    # Format the start and end dates as strings and return them
    return start_date.strftime("%Y-%m-%d %H:%M:%S"), end_date.strftime("%Y-%m-%d %H:%M:%S")


def date_to_timestamp_microseconds(date_str: str) -> int:
    """
    Convert a date string to a Unix timestamp in microseconds.

    Args:
    - date_str (str): The date string in "YYYY-MM-DD HH:MM:SS" format.

    Returns:
    - int: The Unix timestamp in microseconds.
    """
    date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    timestamp_microseconds = int(datetime.timestamp(date_obj) * 1e6)
    return timestamp_microseconds
