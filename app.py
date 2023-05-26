from flask import Flask, render_template, request, send_file
import requests
import pandas as pd
from config import creds

app = Flask(__name__, template_folder='templates')

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        start_date = request.form['start_date']
        start_hour = request.form['start_hour']
        status = request.form['status']
        sort_type = request.form['sort_type']

        save_to_csv(start_date, start_hour, status, sort_type)
        return "Arquivos CSV gerados com sucesso!"

    return render_template('index.html')

def requisao_api(start_date, start_hour, status, sort_type):
    url_base = f"https://api.malga.io/v1/charges?sort={sort_type}&created.gt={start_date}T{start_hour}&status={status}"

    headers = {
        'X-Client-ID': creds.x_client_id,
        'X-Api-Key': creds.x_api_key
    }

    response = requests.get(url_base, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print("Erro na requisição:", response.status_code)
        return None

def save_to_csv(start_date, start_hour, status, sort_type):
    data = requisao_api(start_date, start_hour, status, sort_type)

    if data and 'items' in data:
        total_pages = data['meta']['totalPages']
        df_list = []  # Lista para armazenar os dataframes de cada página

        for page in range(1, total_pages + 1):
            url = f"https://api.malga.io/v1/charges?sort={sort_type}&created.gt={start_date}T{start_hour}&status={status}&page={page}"
            headers = {
                'X-Client-ID': creds.x_client_id,
                'X-Api-Key': creds.x_api_key
            }
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                page_data = response.json()
                df = pd.json_normalize(page_data['items'])

                # Verificar a presença dos campos antes de acessá-los
                if 'fraudAnalysisMetadata.cart.items' in df.columns:
                    df['cart.items.name'] = df['fraudAnalysisMetadata.cart.items'].apply(lambda x: x[0]['name'])
                    df['cart.items.quantity'] = df['fraudAnalysisMetadata.cart.items'].apply(lambda x: x[0]['quantity'])
                if 'transactionRequests' in df.columns:
                    df['transactionRequests.id'] = df['transactionRequests'].apply(lambda x: x[0]['id'])
                    df['transactionRequests.idempotencyKey'] = df['transactionRequests'].apply(lambda x: x[0]['idempotencyKey'])
                    df['transactionRequests.providerId'] = df['transactionRequests'].apply(lambda x: x[0]['providerId'])
                    df['transactionRequests.providerType'] = df['transactionRequests'].apply(lambda x: x[0]['providerType'])
                    df['transactionRequests.amount'] = df['transactionRequests'].apply(lambda x: x[0]['amount'])

                # Selecionar apenas as colunas desejadas
                selected_columns = [
                    'id',
                    'description',
                    'orderId',
                    'createdAt',
                    'amount',
                    'statementDescriptor',
                    'status',
                    'paymentMethod.installments',
                    'paymentMethod.paymentType',
                    'paymentSource.cardId',
                    'fraudAnalysisMetadata.customer.name',
                    'fraudAnalysisMetadata.customer.email',
                    'fraudAnalysisMetadata.customer.identity',
                    'fraudAnalysisMetadata.customer.identityType',
                    'fraudAnalysisMetadata.customer.phone',
                    'fraudAnalysisMetadata.customer.billingAddress.city',
                    'fraudAnalysisMetadata.customer.billingAddress.state',
                    'cart.items.name',
                    'cart.items.quantity',
                    'transactionRequests.id',
                    'transactionRequests.idempotencyKey',
                    'transactionRequests.providerId',
                    'transactionRequests.providerType',
                    'transactionRequests.amount'
                ]
                df = df[selected_columns]

                df_list.append(df)
                print(f"Página {page} de {total_pages} processada com sucesso.")
            else:
                print(f"Erro na requisição da página {page}:", response.status_code)

        final_df = pd.concat(df_list, ignore_index=True)  # Concatena os dataframes de todas as páginas

        final_df.to_csv('dados.csv', index=False)
        print("Arquivo CSV gerado com sucesso!")

        # Obtendo os valores únicos do campo paymentSource.cardId
        unique_card_ids = final_df['paymentSource.cardId'].unique()

        card_data_list = []  # Lista para armazenar os dataframes das consultas individuais de cartões
        qtd_card = 0
        for card_id in unique_card_ids:
            card_url = f"https://api.malga.io/v1/cards/{card_id}"
            headers = {
                'X-Client-ID': creds.x_client_id,
                'X-Api-Key': creds.x_api_key
            }
            card_response = requests.get(card_url, headers=headers)

            if card_response.status_code == 200:
                card_data = card_response.json()
                card_df = pd.json_normalize(card_data)
                card_data_list.append(card_df)
            else:
                print(f"Erro na requisição do cartão {card_id}:", card_response.status_code)
            
            qtd_card = qtd_card + 1
            print(f"Cartão {qtd_card} de {len(unique_card_ids)} processado com sucesso.")

        if card_data_list:
            card_data_df = pd.concat(card_data_list, ignore_index=True)  # Concatena os dataframes dos cartões

            card_data_df.to_csv('dados_cartoes.csv', index=False)
            print("Arquivo CSV de dados dos cartões gerado com sucesso!")
        else:
            print("Não foi possível obter os dados dos cartões.")

    else:
        print("Não foi possível obter os dados da API.")

@app.route('/download', methods=['GET'])
def download():
    return send_file('dados.csv', as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
