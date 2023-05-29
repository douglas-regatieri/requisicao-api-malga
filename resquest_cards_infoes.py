import csv
import requests
from config import creds

# Defina o nome do arquivo CSV de entrada
input_file = 'assets/dados.csv'

# Definindo o tamanho do lote de consulta para evitar ser derrubado pelo servidor
batch_size = 1000

# Defina a URL base da API
base_url = 'https://api.malga.io/v1/cards/'

# Função para fazer a requisição na API e retornar os dados do cartão em formato JSON
def get_card_info(card_id):
    url = base_url + str(card_id)
    headers = {
        'X-Client-ID': creds.x_client_id,
        'X-Api-Key': creds.x_api_key
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Abra o arquivo CSV de entrada
with open(input_file, 'r', encoding='utf-8') as file:
    csv_reader = csv.DictReader(file)
    card_ids = [row['paymentSource.cardId'] for row in csv_reader]

# Divida os IDs em lotes menores
card_id_batches = [card_ids[i:i+batch_size] for i in range(0, len(card_ids), batch_size)]

# Variáveis para acompanhar o progresso
total_cards = len(card_ids)
processed_cards = 0

# Loop para processar os lotes de cartões e salvar as informações
for batch_num, card_id_batch in enumerate(card_id_batches, start=1):
    # Lista para armazenar as informações dos cartões do lote atual
    card_info_list = []
    
    # Processa os cartões do lote atual
    for card_id in card_id_batch:
        card_info = get_card_info(card_id)
        if card_info is not None:
            card_info_list.append(card_info)
        
        # Atualiza o progresso por cartões
        processed_cards += 1
        print(f"Processado {processed_cards}/{total_cards} cartões")
    
    # Verifique se foram encontradas informações dos cartões
    if len(card_info_list) > 0:
        # Cria o nome do arquivo de saída para o lote atual
        output_file = f'informacoes_cartoes_lote{batch_num}.csv'
        
        # Abra o arquivo CSV de saída para o lote atual
        with open(output_file, 'w', newline='') as file:
            fieldnames = card_info_list[0].keys()
            csv_writer = csv.DictWriter(file, fieldnames=fieldnames)
            
            # Escreva o cabeçalho no arquivo CSV
            csv_writer.writeheader()
            
            # Escreva as informações dos cartões no arquivo CSV
            for card_info in card_info_list:
                csv_writer.writerow(card_info)
                
        print(f'As informações do lote {batch_num} foram salvas no arquivo "{output_file}".')
    else:
        print(f'Nenhuma informação de cartão foi encontrada para o lote {batch_num}.')
