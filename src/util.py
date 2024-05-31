from typing import List


# Example1: "deviceName+browserName" -> List("deviceName", "browserName")
# Example2: "deviceName" -> List("deviceName")
def parse_group_by_combination_config(string: str) -> List[str]:
    return [item.strip() for item in string.split('+')]
