from csv import reader
from collections import defaultdict, Counter
from tqdm import tqdm  # progress bars
import time
from create_measurements import num_rows_to_create

row_number = num_rows_to_create

def process_temperatures(path_do_txt):
    # using positive and negative infinity to compare
    # defaultdict: provides a default lambdavalue for the key that does not exist(substituting KeyError) 
    mins = defaultdict(lambda: float('inf'))
    maxs = defaultdict(lambda: float('-inf'))
    sums = defaultdict(float)
    measurements = Counter()

    with open(path_do_txt, 'r', encoding='utf-8') as file:
        _reader = reader(file, delimiter=';')
        # configuring tqdm on the iterator, this will show the percentage of completion
        for row in tqdm(_reader, total=row_number, desc="Processing"):
            station_name, temperature = str(row[0]), float(row[1])
            measurements.update([station_name])
            mins[station_name] = min(mins[station_name], temperature)
            maxs[station_name] = max(maxs[station_name], temperature)
            sums[station_name] += temperature

    print("Loaded data. Calculating statistics...")

    # calculating minimum, average and maximum for each station
    results = {}
    for station, measure_count in measurements.items():
        mean_temp = sums[station] / measure_count
        results[station] = (mins[station], mean_temp, maxs[station])

    print("Statistics calculated. Sorting...")
    # sorting results by station name
    sorted_results = dict(sorted(results.items()))

    # formatting results for display
    formatted_results = {station: f"{min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}"
                         for station, (min_temp, mean_temp, max_temp) in sorted_results.items()}

    return formatted_results


if __name__ == "__main__":
    path_do_csv = "data/measurements.txt"

    print("Starting file processing.")
    start_time = time.time()  # Start time

    results = process_temperatures(path_do_csv)

    end_time = time.time()  # End Time

    for station, metrics in results.items():
        print(station, metrics, sep=': ')

    print(f"\nProcessing completed on {end_time - start_time:.2f} seconds.")