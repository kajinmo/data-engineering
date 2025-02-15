import os
import sys
import random
import time


def build_weather_station_name_list():
    """
    Grabs the weather station names from example data provided in repo and dedups
    """
    station_names = []
    with open('./data/weather_stations.csv', 'r', encoding='utf-8') as file:
        file_contents = file.read()
    for station in file_contents.splitlines():
        if '#' in station:
            next
        else:
            station_names.append(station.split(';')[0])
    return list(set(station_names))


def convert_bytes(num):
    """
    Convert bytes to a human-readable format (e.g., KiB, MiB, GiB)
    """
    for x in ['bytes', 'KiB', 'MiB', 'GiB']:
        if num < 1024.0:
            return '%3.1f %s' % (num, x)
        num /= 1024.0


def format_elapsed_time(seconds):
    """
    Format elapsed time in a human-readable format
    """
    if seconds < 60:
        return f'{seconds:.3f} seconds'
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f'{int(minutes)} minutes {int(seconds)} seconds'
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if minutes == 0:
            return f'{int(hours)} hours {int(seconds)} seconds'
        else:
            return f'{int(hours)} hours {int(minutes)} minutes {int(seconds)} seconds'


def estimate_file_size(weather_station_names, num_rows_to_create):
    """
    Tries to estimate how large a file the test data will be
    """
    max_string = float('-inf')
    min_string = float('inf')
    per_record_size = 0

    for station in weather_station_names:
        if len(station) > max_string:
            max_string = len(station)
        if len(station) < min_string:
            min_string = len(station)
        per_record_size = ((max_string + min_string * 2) + len(',-123.4')) / 2

    total_file_size = num_rows_to_create * per_record_size * 0.57
    human_file_size = convert_bytes(total_file_size)

    return f'The estimated file size is:  {human_file_size}.'


def build_test_data(weather_station_names, num_rows_to_create):
    """
    Generates and writes to file the requested length of test data
    """
    start_time = time.time()
    coldest_temp = -99.9
    hottest_temp = 99.9
    station_names_10k_max = random.choices(weather_station_names, k=10_000)
    batch_size = 10000 # instead of writing line by line to file, process a batch of stations and put it to disk
    print('Creating the file... this will take about 10 minutes...')

    try:
        with open('./data/measurements.txt', 'w', encoding='utf-8') as file:
            for s in range(0,num_rows_to_create // batch_size):
                
                batch = random.choices(station_names_10k_max, k=batch_size)
                prepped_deviated_batch = '\n'.join([f'{station};{random.uniform(coldest_temp, hottest_temp):.1f}' for station in batch]) # :.1f should quicker than round on a large scale, because round utilizes mathematical operation
                file.write(prepped_deviated_batch + '\n')
                
        sys.stdout.write('\n')
    except Exception as e:
        print('Something went wrong. Printing error info and exiting...')
        print(e)
        exit()
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    file_size = os.path.getsize('./data/measurements.txt')
    human_file_size = convert_bytes(file_size)
 
    print('File successfully written to data/measurements.txt')
    print(f'Final size:  {human_file_size}')
    print(f'Elapsed time: {format_elapsed_time(elapsed_time)}')