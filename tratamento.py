import pandas as pd
import json
import os

def tratamento_get_encerradas_info():
    with open('dados/raw/dados_api.json', 'r') as f:
        dados = json.load(f)

    dados = [d for d in dados if 'itensConteudo' in d]
    df = pd.DataFrame([item for d in dados for item in d['itensConteudo']])

    df = df.rename(columns={
        'itensConteudo_id_campanha': 'ID Campanha',
        'itensConteudo_nm_campanha': 'Nome Campanha',
        'itensConteudo_dt_inicio': 'Data Início',
        'itensConteudo_hr_inicio': 'Hora Início',
        'itensConteudo_dt_encerramento': 'Data Encerramento',
        'itensConteudo_total_envio': 'Total Envio',
        'itensConteudo_total_entregues': 'Total Entregues',
        'itensConteudo_total_abertura': 'Total Abertura',
        'itensConteudo_total_clique': 'Total Clique'
    })

    df.to_excel("dados/silver/get_encerradas_info.xlsx", index=False)


def tratamento_relatorio_envio():
    with open('dados/raw/dados_relatorio_envio.json', 'r') as f:
        dados = json.load(f)
  
    lista_dados = []

    for item in dados:
        row = {
            'id_campanha': item.get('itensConteudo_id_campanha'),
            'nome_campanha': item.get('itensConteudo_nm_campanha'),
            'tipo': item.get('itensConteudo_tipo'),
            'lista': item.get('itensConteudo_nm_lista'),
            'nm_filter': item.get('itensConteudo_nm_filter'),
            'total_envio': item.get('itensConteudo_tot_envio'),
            'total_optout': item.get('itensConteudo_total_optout'),
            'total_spam_hotmail': item.get('itensConteudo_tot_spam_hotmail'),
            'totIndike': item.get('itensConteudo_totIndike'),
            'totViewTwitter': item.get('itensConteudo_totViewTwitter'),
            'totClikTwitter': item.get('itensConteudo_totClikTwitter'),
            'total_spam_yahoo': item.get('itensConteudo_tot_spam_yahoo'),
            'total_entregues': item.get('itensConteudo_tot_entregues'),
            'total_nao_entregues': item.get('itensConteudo_nao_entregues'),
            'erro_permanente': item.get('itensConteudo_erro_permanente'),
            'erro_temporario': item.get('itensConteudo_erro_temporario'),
            'percentual_entregues': item.get('itensConteudo_per_entregues'),
            'percentual_permanente': item.get('itensConteudo_per_permanente'),
            'percentual_temporario': item.get('itensConteudo_per_temporario'),
            'nm_usuario_sub': item.get('itensConteudo_nm_usuario_sub'),
            'total_aberto': item.get('itensConteudo_tot_aberto'),
            'percentual_aberto': item.get('itensConteudo_per_total_aberto'),
            'total_nao_aberto': item.get('itensConteudo_nao_aberto'),
            'nm_subject': item.get('itensConteudo_nm_subject'),
            'data_inicio': item.get('itensConteudo_dt_inicio'),
            'hora_inicio': item.get('itensConteudo_hr_inicio'),
            'data_encerramento': item.get('itensConteudo_dt_encerramento'),
            'tamanho_email': item.get('itensConteudo_tamanho_email'),
            'tamanho_total_campanha': item.get('itensConteudo_tamanho_total_campanha'),
            'total_click_unicos': item.get('itensConteudo_nr_total_click_unicos'),
            'percentual_click_unicos': item.get('itensConteudo_per_total_click_unicos'),
            'total_click': item.get('itensConteudo_nr_total_click')
        }
        lista_dados.append(row)

    df = pd.DataFrame(lista_dados)
    df.to_excel("dados/silver/get_envio_info.xlsx", index=False)


def tratamento_relatorio_abertura():
    with open('dados/raw/dados_relatorio_abertura.json', 'r') as f:
        dados = json.load(f)

    lista_dados = []

    for bloco in dados:
        for item in bloco.get('itensConteudo', []):
            row = {
                'id_campanha': item.get('itensConteudo_id_campanha'),
                'nm_email': item.get('itensConteudo_nm_email'),
                'data_click': item.get('itensConteudo_dt_view'),
                'pais': item.get('itensConteudo_pais')
            }
            lista_dados.append(row)

    if lista_dados:
        df = pd.DataFrame(lista_dados)
        df.to_excel("dados/silver/relatorio_abertura.xlsx", index=False)
    else:
        print("Nenhum dado válido encontrado.")


def tratamento_relatorio_clique():
    with open('dados/raw/dados_relatorio_clique.json', 'r') as f:
        dados = json.load(f)
  
    lista_dados = []

    for bloco in dados:
        for item in bloco.get('itensConteudo', []):
            row = {
                'id_campanha': item.get('itensConteudo_id_campanha'),
                'nm_email': item.get('itensConteudo_nm_email'),
                'data_clique': item.get('itensConteudo_dt_click'),
            }
            lista_dados.append(row)

    df = pd.DataFrame(lista_dados)
    df.to_excel("dados/silver/relatorio_clique.xlsx", index=False)


def tratamento_optout_info():
    with open('dados/raw/dados_api_optout.json', 'r') as f:
        dados = json.load(f)

    lista_dados = []

    for bloco in dados:
        for item in bloco.get('dados', []):
            row = {
                'nm_email': item.get('itensConteudo_nm_email'),
                'motivo': item.get('itensConteudo_motivo'),
                'dt_optout': item.get('itensConteudo_dt_optout'),
                'nm_lista': item.get('itensConteudo_nm_lista'),
                'nm_campanha': item.get('itensConteudo_nm_campanha'),
                'id_campanha': item.get('itensConteudo_id_campanha')
            }
            lista_dados.append(row)

    df = pd.DataFrame(lista_dados)

    df = df.rename(columns={
        'nm_email': 'Email',
        'motivo': 'Motivo',
        'dt_optout': 'Data Optout',
        'nm_lista': 'Lista',
        'nm_campanha': 'Nome Campanha',
        'id_campanha': 'ID Campanha'
    })

    os.makedirs('dados/silver', exist_ok=True)
    df.to_excel("dados/silver/optout_info.xlsx", index=False)
if __name__ == "__main__":
    tratamento_get_encerradas_info()
    tratamento_relatorio_envio()
    tratamento_relatorio_abertura()
    tratamento_relatorio_clique()
    tratamento_optout_info()
