def parse_string_array_config(array_string):
    # Removes the brackets and splits the string by commas
    return [item.strip() for item in array_string.strip('[]').split(',')]

def parse_int_array_config(array_string):
    # Removes the brackets and splits the string by commas
    return [int(item.strip()) for item in array_string.strip('[]').split(',')]
