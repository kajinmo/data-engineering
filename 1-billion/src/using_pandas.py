import pandas as pd
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from create_measurements import num_rows_to_create

CONCURRENCY = cpu_count()
total_rows = num_rows_to_create
chunksize = 100_000_000
filename = "data/measurements.txt"

def process_chunk(chunk):
    # Aggregate data within chunk using Pandas
    aggregated = chunk.groupby('station')['measure'].agg(['min', 'max', 'mean']).reset_index()
    return aggregated

def create_df_with_pandas(filename, total_rows, chunksize=chunksize):
    total_chunks = total_rows // chunksize + (1 if total_rows % chunksize else 0)
    results = []

    with pd.read_csv(filename, sep=';', header=None, names=['station', 'measure'], chunksize=chunksize) as reader:
        # Wrapping iterator with tqdm to visualize progress
        with Pool(CONCURRENCY) as pool:
            for chunk in tqdm(reader, total=total_chunks, desc="Processing..."):
                # Process each chunk in parallel
                result = pool.apply_async(process_chunk, (chunk,))
                results.append(result)

            results = [result.get() for result in results]

    final_df = pd.concat(results, ignore_index=True)

    final_aggregated_df = final_df.groupby('station').agg({
        'min': 'min',
        'max': 'max',
        'mean': 'mean'
    }).reset_index().sort_values('station')

    return final_aggregated_df

if __name__ == "__main__":
    import time
    print("Starting file processing.")
    start_time = time.time()
    df = create_df_with_pandas(filename, total_rows, chunksize)
    took = time.time() - start_time

    print(df.head())
    print(f"Pandas took: {took:.2f} sec")