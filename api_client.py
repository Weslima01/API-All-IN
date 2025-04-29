import requests
import time
import datetime
import json
import pandas as pd
import os


class ApiClient:
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.token = None

    def get_token(self):
        auth_url = f"{self.base_url}?method=get_token"
        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded"
        }
        payload = {
            "username": self.username,
            "password": self.password
        }
        response = requests.post(auth_url, data=payload, headers=headers)
        if response.status_code == 200:
            self.token = response.json()["token"]
            return self.token
        else:
            raise Exception("Failed to obtain token")

    def get_optout_info(self, dt_inicio, dt_fim, page=1):
        url = (
            f"{self.base_url}?method=getOptout"
            f"&output=json"
            f"&token={self.token}"
            f"&dt_inicio={dt_inicio}"
            f"&dt_fim={dt_fim}"
            f"&page={page}"
        )
        headers = {"accept": "application/json"}
        print(f"\n📡 URL chamada: {url}")
        response = requests.post(url, headers=headers)

        try:
            return response.json()
        except Exception:
            print("⚠️ A resposta não é JSON. Conteúdo bruto:")
            print(response.text)
            return {"erro": response.text}

    def get_encerradas_info(self, method, dt_inicio, dt_fim):
        endpoint = f"?method={method}&output=json&token={self.token}&dt_inicio={dt_inicio}&dt_fim={dt_fim}"
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url)
        return response.json()

    def get_relatorio_envio(self, campanha_id):
        endpoint = f"?token={self.token}&method=relatorio_envio&output=json&campanha_id={campanha_id}"
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url)
        return response.json()

    def get_relatorio_abertura(self, campanha_id):
        endpoint = f"?token={self.token}&method=get_abertura_envio&output=json&campanha_id={campanha_id}"
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url)
        return response.json()

    def get_relatorio_clique(self, campanha_id):
        endpoint = f"?token={self.token}&method=get_clique_envio&output=json&campanha_id={campanha_id}"
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url)
        return response.json()


def optout_info():
    client = ApiClient(
        "https://painel02.allinmail.com.br/allinapi/",
        "neotass__allinapi",
        "7y6aSu5E"
    )
    client.get_token()
    dados_api = []

    data_inicial = datetime.date(2025, 2, 7)
    data_final = datetime.date.today()

    print("🔄 Iniciando o processamento dos relatórios...")
    print("📥 Obtendo informações optout...")

    while data_inicial <= data_final:
        data_fim_intervalo = data_inicial + datetime.timedelta(days=29)
        if data_fim_intervalo > data_final:
            data_fim_intervalo = data_final

        print(f"\n📅 Buscando de {data_inicial} até {data_fim_intervalo}...")
        page = 1
        todos_os_resultados = []

        while True:
            try:
                result = client.get_optout_info(str(data_inicial), str(data_fim_intervalo), page=page)

                if not isinstance(result, dict):
                    print(f"⚠️ Resposta inesperada (tipo {type(result)}): {result}")
                    break

                print(f"📬 Conteúdo recebido:\n{json.dumps(result, indent=2)}")

                itens = result.get("itensConteudo", [])
                print(f"📄 Página {page}: {len(itens)} itens")

                if not itens:
                    break

                todos_os_resultados.extend(itens)
                page += 1
                time.sleep(0.2)

            except Exception as e:
                print(f"❌ Erro na página {page} ({data_inicial} a {data_fim_intervalo}): {e}")
                break

        dados_api.append({
            "inicio": str(data_inicial),
            "fim": str(data_fim_intervalo),
            "dados": todos_os_resultados
        })

        data_inicial = data_fim_intervalo + datetime.timedelta(days=1)
        time.sleep(0.2)

    os.makedirs('dados/raw', exist_ok=True)
    with open('dados/raw/dados_api_optout.json', 'w', encoding='utf-8') as f:
        json.dump(dados_api, f, ensure_ascii=False, indent=4)

    print("\n✅ Arquivo 'dados_api_optout.json' salvo com sucesso.")


def get_encerradas_info():
    client = ApiClient(
        "https://painel02.allinmail.com.br/allinapi/",
        "neotass__allinapi",
        "7y6aSu5E"
    )
    client.get_token()
    dados_api = []

    data_inicial = datetime.date(2025, 2, 7)
    data_final = datetime.date.today()
    dias = []
    while data_inicial <= data_final:
        dias.append(data_inicial.strftime('%Y-%m-%d'))
        data_inicial += datetime.timedelta(days=1)

    for dia in dias:
        results = client.get_encerradas_info("get_encerradas_info", dia, dia)
        dados_api.append(results)

    os.makedirs('dados/raw', exist_ok=True)
    with open('dados/raw/dados_api.json', 'w') as f:
        json.dump(dados_api, f, indent=4)

def relatorio_envio():
    client = ApiClient(
        "https://painel02.allinmail.com.br/allinapi/",
        "neotass__allinapi",
        "7y6aSu5E"
    )
    client.get_token()
    id_campanhas = pd.read_excel("dados/silver/get_encerradas_info.xlsx", usecols=['ID Campanha'])
    ids_campanhas = id_campanhas['ID Campanha'].to_list()

    dados_api = []
    for id_c in ids_campanhas:
        print(id_c)
        results = client.get_relatorio_envio(id_c)
        dados_api.append(results)

    os.makedirs('dados/raw', exist_ok=True)
    with open('dados/raw/dados_relatorio_envio.json', 'w') as f:
        json.dump(dados_api, f, indent=4)

def relatorio_abertura():
    client = ApiClient(
        "https://painel02.allinmail.com.br/allinapi/",
        "neotass__allinapi",
        "7y6aSu5E"
    )
    client.get_token()
    dados = []
    id_campanhas = pd.read_excel("dados/silver/get_envio_info.xlsx", usecols=['id_campanha'])
    ids_campanhas = id_campanhas['id_campanha'].to_list()

    for id in ids_campanhas:
        results = client.get_relatorio_abertura(id)
        dados.append(results)

    os.makedirs('dados/raw', exist_ok=True)
    with open('dados/raw/dados_relatorio_abertura.json', 'w') as f:
        json.dump(dados, f, indent=4)

def relatorio_clique():
    client = ApiClient(
        "https://painel02.allinmail.com.br/allinapi/",
        "neotass__allinapi",
        "7y6aSu5E"
    )
    client.get_token()
    dados = []
    id_campanhas = pd.read_excel("dados/silver/get_envio_info.xlsx", usecols=['id_campanha'])
    ids_campanhas = id_campanhas['id_campanha'].to_list()

    for id in ids_campanhas:
        results = client.get_relatorio_clique(id)
        dados.append(results)

    os.makedirs('dados/raw', exist_ok=True)
    with open('dados/raw/dados_relatorio_clique.json', 'w') as f:
        json.dump(dados, f, indent=4)
