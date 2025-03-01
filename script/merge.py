import os
import pandas as pd

def merge_csv_files(directory, output_file):
    # Cria uma lista com os arquivos CSV do diretório
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    
    # Se não houver arquivos CSV, retorna
    if not csv_files:
        print("Nenhum arquivo CSV encontrado no diretório.")
        return
    
    # Lista para armazenar os dataframes
    dataframes = []
    
    # Leitura e adição de cada arquivo CSV na lista de dataframes
    for file in csv_files:
        file_path = os.path.join(directory, file)
        df = pd.read_csv(file_path)
        dataframes.append(df)
    
    # Concatenar todos os dataframes
    merged_df = pd.concat(dataframes, ignore_index=True)
    
    # Salvar o dataframe final em um único arquivo CSV
    merged_df.to_csv(output_file, index=False)
    
    print(f"Arquivos CSV combinados e salvos em: {output_file}")

# Exemplo de uso
directory = '../data/stocks' # Substitua com o caminho do diretório
output_file = 'merged_data_all.csv'  # O arquivo de saída

merge_csv_files(directory, output_file)
