from typing import Any


def convert_tag_value(value, type_hint) -> Any:
    """
    Convert tag value based on the type hint or inferred data type.

    This function takes a value and a type hint as input. It then converts the value to the type specified by the type hint.
    If the type hint is not recognized, the function attempts to infer the type from the value itself.
    If the type cannot be inferred, the original value is returned.

    Args:
        value (Any): The value to be converted.
        type_hint (str): A string hinting at the type to which the value should be converted.

    Returns:
        Any: The converted value, or the original value if the type could not be inferred or converted.

    Examples:
        >>> convert_tag_value('true', 'bool')
        True
        >>> convert_tag_value('123', 'int')
        123
        >>> convert_tag_value('123.456', 'float')
        123.456
        >>> convert_tag_value('yes', 'str')
        'yes'
    """

    # Convert the value to a boolean if the type hint is 'bool' or 'boolean'
    if type_hint in ['bool', 'boolean']:
        return bool(value)

    # Convert the value to a string if the type hint is 'string' or 'str'
    if type_hint in ['string', 'str']:
        return str(value)

    # Convert the value to an integer if the type hint is 'int' or 'integer'
    if type_hint in ['int', 'integer']:
        return int(value)

    # Convert the value to a float if the type hint is 'float' or 'double'
    if type_hint in ['float', 'double']:
        return float(value)

    # If the value is a string, check if it represents a boolean value
    if isinstance(value, str):
        return value.lower() in ['true', '1', 't', 'y', 'yes']

    # Return the original value if type is unknown or not specified
    return value
