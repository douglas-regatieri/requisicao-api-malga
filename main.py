import requests
from config import creds
import pandas as pd

start_date = '2023-05-23'
start_hour = '00:00:00'
status = 'authorized'
sort_type = 'DESC'
url_base = f"https://api.malga.io/v1/charges?sort={sort_type}&created.gt={start_date}T{start_hour}&status={status}"

def requisao_api():

    headers = {
        'X-Client-ID': creds.x_client_id,
        'X-Api-Key': creds.x_api_key
    }

    response = requests.get(url_base, headers=headers)

    if response.status_code == 200:
        data = response.json()
        print(data)

        return data
    
    else:
        print("Erro na requisição:", response.status_code)

def save_to_csv():
    data = requisao_api()

    if data and 'items' in data:
        total_pages = data['meta']['totalPages']
        df_list = []  # Lista para armazenar os dataframes de cada página

        for page in range(1, total_pages + 1):
            url = f"{url_base}&page={page}"
            headers = {
                'X-Client-ID': creds.x_client_id,
                'X-Api-Key': creds.x_api_key
            }
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                page_data = response.json()
                df = pd.json_normalize(page_data['items'])

                for index, row in df.iterrows():
                    if 'transactionRequests' in row and isinstance(row['transactionRequests'], list):
                        df_transactionRequests = pd.json_normalize(row['transactionRequests'])
                        for column in df_transactionRequests.columns:
                            df[f'transactionRequests_{column}'] = df_transactionRequests[column]

                df_list.append(df)
                print(f"Página {page} processada com sucesso.")
            else:
                print(f"Erro na requisição da página {page}:", response.status_code)

        final_df = pd.concat(df_list, ignore_index=True)  # Concatena os dataframes de todas as páginas

        final_df.to_csv('dados.csv', index=False)
        print("Arquivo CSV gerado com sucesso!")
    else:
        print("Não foi possível obter os dados da API.")

save_to_csv()
