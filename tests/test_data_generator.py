import json
import random
import time


def generate_metric_value(base, variation, spike=False, spike_range=(1, 2)):
    value = base + random.uniform(-variation, variation)
    if spike:
        value += random.uniform(*spike_range)
    return value


def generate_test_data(start_timestamp, minutes=60):
    data = {}
    for minute in range(minutes):
        timestamp = start_timestamp + minute * 60  # Increments by 60 seconds

        # Determines if a spike should be added
        iphone_spike = 15 <= minute < 25 or 35 <= minute < 45
        pc_spike = 25 <= minute < 40

        # Generates metric values with or without spikes
        iphone_page_load_time = generate_metric_value(1.5, 0.1, spike=iphone_spike, spike_range=(1, 2))
        pc_checkout_conversion_rate = generate_metric_value(0.5, 0.05, spike=pc_spike, spike_range=(0.3, 0.5))

        # Stores data as a list of dictionaries
        data[timestamp] = [
            {
                'deviceName': 'iPhone',
                'page_load_time': iphone_page_load_time,
                'checkout_conversion_rate': generate_metric_value(0.5, 0.05),  # Normal value for iPhone
                'group_size': random.randint(200, 300)
            },
            {
                'deviceName': 'PC',
                'page_load_time': generate_metric_value(1.5, 0.1),  # Normal value for PC
                'checkout_conversion_rate': pc_checkout_conversion_rate,
                'group_size': random.randint(100, 200)
            }
        ]

    return data


# Starting epoch timestamp at a minute boundary (example: 1609459200)
base_timestamp = int(time.time()) // 60 * 60
test_data = generate_test_data(base_timestamp)

# Saves the generated data
with open('data/anomaly_test_data.json', 'w', encoding='utf-8') as file:
    json.dump(test_data, file, indent=4)