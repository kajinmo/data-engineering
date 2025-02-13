from utils import build_weather_station_name_list, estimate_file_size, build_test_data

num_rows_to_create = 1_000_000_000

def main():
    """
    main program function
    """
    weather_station_names = []
    weather_station_names = build_weather_station_name_list()
    print(estimate_file_size(weather_station_names, num_rows_to_create))
    build_test_data(weather_station_names, num_rows_to_create)
    print('Test file completed.')


if __name__ == '__main__':
    main()