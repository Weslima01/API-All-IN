# ========================== IMPORTAÇÕES ==========================
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import traceback
import pandas as pd
import os
import glob
from datetime import datetime

# ========================== CONFIGURAÇÕES PADRÃO ==========================

PASTA_DOWNLOADS = os.path.join(os.path.expanduser("~"), "Downloads")
LOGIN_URL = "https://painel03.allinmail.com.br/index.php"
USUARIO = "neotass_"
SENHA = "Neo@mkt20252404#"

# ========================== FUNÇÕES AUXILIARES ==========================

def configurar_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--headless=new")  # Executa o navegador de forma invisível
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 20)
    return driver, wait

def realizar_login(driver, wait, usuario, senha):
    print("🔐 Fazendo login...")
    driver.get(LOGIN_URL)
    wait.until(EC.presence_of_element_located((By.ID, "usuario"))).send_keys(usuario)
    driver.find_element(By.ID, "senha").send_keys(senha)
    driver.find_element(By.XPATH, "//button[div[text()='Entrar']]").click()
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "sidebarNovo")))
    print("✅ Login realizado.")

def acessar_repositorio(driver, wait):
    print("📁 Acessando repositório...")
    repo_xpath = "//span[text()='Repositório']"
    repo_btn = wait.until(EC.element_to_be_clickable((By.XPATH, repo_xpath)))
    driver.execute_script("arguments[0].click();", repo_btn)
    time.sleep(3)

def baixar_arquivo_por_id(driver, wait, id_campanha, download_status):
    nome_arquivo = f"denuncia_campanha_{id_campanha}"
    print(f"\n🔍 Procurando arquivo: {nome_arquivo}")

    try:
        wait.until(EC.presence_of_element_located((By.XPATH, f"//td[contains(text(), '{nome_arquivo}')]")))
    except:
        print(f"❌ Arquivo '{nome_arquivo}' não encontrado visualmente.")
        download_status.append(f"{id_campanha} | ❌ Arquivo não encontrado visualmente.")
        return

    try:
        tabela = driver.find_element(By.CSS_SELECTOR, "#wrap > div.area-sidebarNovo > div > div.content.classUtilTour > table")
    except:
        print("❌ Tabela de arquivos não encontrada.")
        download_status.append(f"{id_campanha} | ❌ Tabela não encontrada.")
        return

    linhas = tabela.find_elements(By.XPATH, ".//tbody/tr")
    encontrado = False

    for i, linha in enumerate(linhas):
        colunas = linha.find_elements(By.TAG_NAME, "td")
        if len(colunas) < 2:
            continue
        if colunas[1].text.strip() != nome_arquivo:
            continue

        print(f"✅ Arquivo encontrado na linha {i}.")
        encontrado = True
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", linha)
        time.sleep(1)

        try:
            botao_acoes = colunas[0].find_element(By.CSS_SELECTOR, "div > ul > li > span")
            driver.execute_script("arguments[0].click();", botao_acoes)
            print("⚙️ Menu 'AÇÕES ARQUIVOS' aberto.")
        except:
            print(f"❌ Botão 'AÇÕES' não encontrado na linha {i}")
            download_status.append(f"{id_campanha} | ❌ Botão AÇÕES não encontrado.")
            return

        time.sleep(1)

        try:
            baixar_css = "#wrap > div.area-sidebarNovo > div > div.content.classUtilTour > table > tbody > tr.selecionado > td:nth-child(1) > div > ul > li > ul > a > li > span"
            botao_baixar = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, baixar_css)))
            botao_baixar.click()
            print(f"✅ Download do '{nome_arquivo}' iniciado.")
            download_status.append(f"{id_campanha} | ✅ Download iniciado.")
        except:
            print(f"❌ Botão 'BAIXAR' não encontrado para '{nome_arquivo}'.")
            download_status.append(f"{id_campanha} | ❌ Botão BAIXAR não encontrado.")
        break

    if not encontrado:
        print(f"❌ Arquivo '{nome_arquivo}' não encontrado na tabela.")
        download_status.append(f"{id_campanha} | ❌ Arquivo não encontrado na tabela.")

def unir_csvs_downloads(download_status):
    arquivos_todos = glob.glob(os.path.join(PASTA_DOWNLOADS, "denuncia_campanha_*.csv"))
    arquivos_por_id = {}

    for arquivo in arquivos_todos:
        nome = os.path.basename(arquivo)
        id_base = nome.split(".csv")[0].split("(")[0].strip()
        arquivos_por_id.setdefault(id_base, []).append(arquivo)

    arquivos_final = [max(lista, key=lambda x: os.path.getmtime(x)) for lista in arquivos_por_id.values()]
    dfs = []

    for arquivo in arquivos_final:
        try:
            if os.path.getsize(arquivo) < 10:
                print(f"⚠️ Ignorado (vazio): {arquivo}")
                download_status.append(f"{os.path.basename(arquivo)} | ⚠️ Ignorado: vazio")
                continue

            df = pd.read_csv(arquivo, sep=";", encoding="utf-8", engine="python", header=None)
            if df.empty:
                print(f"⚠️ Ignorado (sem dados): {arquivo}")
                download_status.append(f"{os.path.basename(arquivo)} | ⚠️ Ignorado: sem dados.")
                continue

            id_campanha = os.path.basename(arquivo).replace("denuncia_campanha_", "").split(".csv")[0].split("(")[0].strip()
            df.insert(0, "ID_CAMPANHA", id_campanha)
            dfs.append(df)

        except Exception as e:
            print(f"❌ Erro ao ler {arquivo}: {e}")
            download_status.append(f"{os.path.basename(arquivo)} | ❌ Erro ao ler: {e}")

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        df_final.columns = ["ID_CAMPANHA", "EMAIL"]

        data_hoje = datetime.now().strftime("%d-%m-%Y")
        output_path = os.path.join(PASTA_DOWNLOADS, f"denuncias_{data_hoje}.xlsx")
        df_final.to_excel(output_path, index=False)
        print(f"\n📁 Arquivo final salvo em: {output_path}")
        download_status.append(f"✅ Arquivo final salvo: {output_path}")
        return output_path
    else:
        print("⚠️ Nenhum CSV válido encontrado para unificação.")
        download_status.append("⚠️ Nenhum CSV unificado.")
        return None

def salvar_log(download_status):
    log_path = os.path.join(PASTA_DOWNLOADS, "log_downloads.txt")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("LOG DE DOWNLOAD E UNIFICAÇÃO\n")
        f.write("="*40 + "\n\n")
        for linha in download_status:
            f.write(f"{linha}\n")
    print(f"\n📝 Log salvo em: {log_path}")

# ========================== FUNÇÃO PRINCIPAL ==========================

def executar_download_denuncias(ids_campanha, log_previo=None):
    download_status = [] if log_previo is None else log_previo.copy()
    driver, wait = configurar_driver()

    try:
        realizar_login(driver, wait, USUARIO, SENHA)
        acessar_repositorio(driver, wait)

        for id_campanha in ids_campanha:
            baixar_arquivo_por_id(driver, wait, id_campanha, download_status)
            time.sleep(3)

    except Exception as e:
        print("❌ Erro durante o processo geral:")
        traceback.print_exc()
        download_status.append(f"❌ ERRO GERAL: {str(e)}")

    finally:
        print("\n📦 Iniciando unificação dos arquivos baixados...")
        arquivo_final = unir_csvs_downloads(download_status)
        salvar_log(download_status)
        time.sleep(5)
        driver.quit()

        return arquivo_final

# ========================== EXECUÇÃO STANDALONE ==========================

if __name__ == "__main__":
    IDS_CAMPANHA = [
        11634369, 11634370, 11636141, 11636618, 11636619,
        11641132, 11640566, 11640563, 11642844, 11644389
    ]

    arquivo_gerado = executar_download_denuncias(IDS_CAMPANHA)
    print("\n✅ Execução standalone concluída.")
    print(f"📎 Arquivo final: {arquivo_gerado if arquivo_gerado else 'Nenhum arquivo gerado.'}")
