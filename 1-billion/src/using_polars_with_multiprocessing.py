import polars as pl
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import time
import os
from create_measurements import num_rows_to_create

CONCURRENCY = cpu_count()
total_rows = num_rows_to_create
chunksize = 100_000_000
filename = "data/measurements.txt"

def process_chunk(chunk_info):
    start_row, end_row = chunk_info
    
    # Lê apenas as linhas do chunk específico
    df = pl.read_csv(
        filename,
        separator=';',
        has_header=False,
        new_columns=['station', 'measure'],
        skip_rows=start_row,
        n_rows=end_row - start_row
    )
    
    # Realiza as agregações
    aggregated = df.group_by('station').agg([
        pl.col('measure').min().alias('min'),
        pl.col('measure').max().alias('max'),
        pl.col('measure').mean().alias('mean')
    ])
    
    return aggregated

def create_df_with_polars(filename, total_rows, chunksize=chunksize):
    # Verifica se o arquivo existe
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File {filename} not found")
    
    # Calcula o número de chunks
    total_chunks = total_rows // chunksize + (1 if total_rows % chunksize else 0)
    
    # Cria lista de tuplas (start_row, end_row) para cada chunk
    chunk_ranges = [
        (i * chunksize, min((i + 1) * chunksize, total_rows))
        for i in range(total_chunks)
    ]
    
    results = []
    
    # Processa os chunks em paralelo
    with Pool(CONCURRENCY) as pool:
        # Cria os futures para processamento assíncrono
        futures = [pool.apply_async(process_chunk, (chunk_range,)) 
                  for chunk_range in chunk_ranges]
        
        # Coleta os resultados com barra de progresso
        for future in tqdm(futures, desc="Processing chunks"):
            try:
                result = future.get()
                results.append(result)
            except Exception as e:
                print(f"Error processing chunk: {e}")
                continue
    
    if not results:
        raise ValueError("No results were processed successfully")
    
    # Concatena todos os resultados
    final_df = pl.concat(results)
    
    # Faz a agregação final
    final_aggregated_df = final_df.group_by('station').agg([
        pl.col('min').min().alias('min'),
        pl.col('max').max().alias('max'),
        pl.col('mean').mean().alias('mean')
    ]).sort('station')
    
    return final_aggregated_df

if __name__ == "__main__":
    print("Starting file processing with Polars...")
    start_time = time.time()
    
    try:
        df = create_df_with_polars(filename, total_rows, chunksize)
        took = time.time() - start_time
        
        print("\nResults:")
        print(df.head())
        print(f"\nPolars took: {took:.2f} sec")
    except Exception as e:
        print(f"An error occurred: {e}")