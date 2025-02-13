import polars as pl
from tqdm import tqdm
import time
import os
from create_measurements import num_rows_to_create

# Configurações
total_rows = num_rows_to_create
filename = "data/measurements.txt"

def process_chunk(df):
    """Processa um chunk de dados e retorna as agregações."""
    return df.group_by('station').agg([
        pl.col('measure').min().alias('min'),
        pl.col('measure').max().alias('max'),
        pl.col('measure').mean().alias('mean')
    ])

def create_df_with_polars(filename, total_rows, chunksize=10_000_000):
    """Processa o arquivo em chunks e retorna o DataFrame final agregado."""
    # Verifica se o arquivo existe
    if not os.path.exists(filename):
        raise FileNotFoundError(f"File {filename} not found")
    
    # Inicializa uma lista para armazenar os resultados parciais
    results = []

    # Usa scan_csv para leitura lazy e processamento em chunks
    lazy_df = pl.scan_csv(
        filename,
        separator=';',
        has_header=False,
        new_columns=['station', 'measure']
    )

    # Processa os chunks com barra de progresso
    for chunk in tqdm(lazy_df.collect(streaming=True).iter_slices(n_rows=chunksize), desc="Processing chunks"):
        try:
            # Processa o chunk e adiciona o resultado à lista
            result = process_chunk(chunk)
            results.append(result)
        except Exception as e:
            print(f"Error processing chunk: {e}")
            continue
    
    if not results:
        raise ValueError("No results were processed successfully")
    
    # Concatena todos os resultados e faz a agregação final
    final_df = pl.concat(results).group_by('station').agg([
        pl.col('min').min().alias('min'),
        pl.col('max').max().alias('max'),
        pl.col('mean').mean().alias('mean')
    ]).sort('station')
    
    return final_df

if __name__ == "__main__":
    print("Starting file processing with Polars...")
    start_time = time.time()
    
    try:
        df = create_df_with_polars(filename, total_rows)
        took = time.time() - start_time
        
        print("\nResults:")
        print(df.head())
        print(f"\nPolars took: {took:.2f} sec")
    except Exception as e:
        print(f"An error occurred: {e}")