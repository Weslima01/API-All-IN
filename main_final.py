# ========================== IMPORTAÇÕES ==========================
import time
import os
import traceback
from datetime import datetime

from api_client import optout_info, get_encerradas_info, relatorio_envio, relatorio_abertura, relatorio_clique
from tratamento import tratamento_get_encerradas_info, tratamento_relatorio_envio, tratamento_relatorio_abertura, tratamento_relatorio_clique, tratamento_optout_info
from envio_email import enviar_email_com_anexos
from requerimento_all import executar_fluxo_spam
from download_all import executar_download_denuncias
# from db_utils import upload_excel_to_mysql_incremental

# ========================== CONFIGURAÇÕES ==========================

PASTA_DOWNLOADS = os.path.join(os.path.expanduser("~"), "Downloads")

DESTINATARIO = "alline.tozzi@neotass.com.br"
CC = ["levy.ramalho@neotass.com.br"]

ARQUIVOS_FIXOS = [
    r"C:\Users\Wesley\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\Python\allin_pipeline\dados\silver\get_encerradas_info.xlsx",
    r"C:\Users\Wesley\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\Python\allin_pipeline\dados\silver\get_envio_info.xlsx",
    r"C:\Users\Wesley\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\Python\allin_pipeline\dados\silver\relatorio_abertura.xlsx",
    r"C:\Users\Wesley\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\Python\allin_pipeline\dados\silver\relatorio_clique.xlsx",
    r"C:\\Users\\WesleyLimaNeotassMar\\Python\\api_all_in-main\\dados\\raw\\dados_api_optout.xlsx",
]

# ========================== FUNÇÃO MAIN ==========================

def main():
    print("🔄 Iniciando o processamento dos relatórios...")

    # ========================== ETAPA 1 - API e Tratamento ==========================
    try:
        print("📥 Obtendo informações optout...")
        optout_info()
        print("✅ Informações de optput obtidas!")

        print("📥 Obtendo informações encerradas...")
        get_encerradas_info()
        print("✅ Informações encerradas obtidas!")

        print("🛚 Tratando informações encerradas...")
        tratamento_get_encerradas_info()
        print("✅ Tratamento de encerradas concluído!")

        print("📊 Gerando relatório de envio...")
        relatorio_envio()
        print("✅ Relatório de envio gerado!")

        print("🛚 Tratando relatório de envio...")
        tratamento_relatorio_envio()
        print("✅ Tratamento de envio concluído!")

        print("📊 Gerando relatório de abertura...")
        relatorio_abertura()
        print("✅ Relatório de abertura gerado!")

        print("🛚 Tratando relatório de abertura...")
        tratamento_relatorio_abertura()
        print("✅ Tratamento de abertura concluído!")

        print("📊 Gerando relatório de cliques...")
        relatorio_clique()
        print("✅ Relatório de cliques gerado!")

        print("🛚 Tratando relatório de cliques...")
        tratamento_relatorio_clique()
        print("✅ Tratamento de cliques concluído!")

    except Exception:
        print("❌ Erro durante execução da etapa de API e tratamento:")
        traceback.print_exc()

    # ========================== ETAPA 2 - Verificação de SPAM ==========================
    print("\n🧭 Executando verificação de SPAM...")
    try:
        campanhas_ids, log_spam = executar_fluxo_spam()
        print("✅ Verificação de SPAM concluída.")
    except Exception:
        print("❌ Erro na execução da verificação de SPAM.")
        traceback.print_exc()
        campanhas_ids, log_spam = [], []

    # ========================== ESPERA 30 MINUTOS ==========================
    print("\n⏳ Aguardando 30 minutos antes do download de denúncias...")
    time.sleep(1800)

    # ========================== ETAPA 3 - Download de Denúncias ==========================
    print("\n📥 Executando download e unificação de denúncias...")
    try:
        arquivo_denuncias = executar_download_denuncias(ids_campanha=campanhas_ids, log_previo=log_spam)
        print("✅ Download e unificação concluídos.")
    except Exception:
        print("❌ Erro na execução do download de denúncias.")
        traceback.print_exc()
        arquivo_denuncias = None

    # ========================== ETAPA 4 - Envio de E-mail ==========================
    print("\n📧 Preparando envio de e-mail...")

    # Montar lista de anexos
    log_final = os.path.join(PASTA_DOWNLOADS, "status_execucao_allin.txt")

    arquivos_para_anexar = ARQUIVOS_FIXOS + [arquivo_denuncias if arquivo_denuncias else "", log_final]
    arquivos_existentes = [arquivo for arquivo in arquivos_para_anexar if os.path.exists(arquivo)]

    if arquivos_existentes:
        print(f"📂 Anexando {len(arquivos_existentes)} arquivos encontrados...")
        enviar_email_com_anexos(DESTINATARIO, arquivos_existentes, cc=CC)
        print("✅ E-mail enviado com sucesso! 🚀")
    else:
        print("❌ Nenhum arquivo encontrado. E-mail não enviado.")

# # ========================== EXECUÇÃO STANDALONE ==========================

# if __name__ == "__main__":
#     main()
