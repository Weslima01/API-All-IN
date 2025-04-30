# ========================== IMPORTAÇÕES ==========================
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import traceback
import os
from datetime import datetime

# ========================== PADRÕES ==========================

DEFAULT_CAMPANHAS_IDS = [
    11634369, 11634370, 11636141, 11636618, 11636619,
    11641132, 11640566, 11640563, 11642844, 11644389
]

BASE_URL = "https://painel03.allinmail.com.br/relatorios_envio_v2.php?id_campanha="
LOGIN_URL = "https://painel03.allinmail.com.br/index.php"
DEFAULT_USUARIO = "neotass_" #TODO :ALTERAR PARA VARIAVEL DE AMBIENTE 
DEFAULT_SENHA = "Neo@mkt20252404#"
DEFAULT_TIMEOUT = 20

# ========================== FUNÇÕES DE SUPORTE ==========================

def configurar_navegador(timeout=DEFAULT_TIMEOUT):
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--headless=new")  # Executa o navegador de forma invisível
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    wait = WebDriverWait(driver, timeout)
    return driver, wait

def realizar_login(driver, wait, usuario, senha):
    print("🔐 Acessando painel...")
    driver.get(LOGIN_URL)
    wait.until(EC.presence_of_element_located((By.ID, "usuario"))).send_keys(usuario)
    driver.find_element(By.ID, "senha").send_keys(senha)
    driver.find_element(By.XPATH, "//button[div[text()='Entrar']]").click()
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "sidebarNovo")))
    print("✅ Login realizado com sucesso.\n")

def acessar_campanha(driver, wait, id_campanha, resultado):
    print(f"📄 Acessando campanha: {id_campanha}")
    driver.get(BASE_URL + str(id_campanha))

    try:
        botao_xpath = "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'spam por usuários')]"
        botao = wait.until(EC.element_to_be_clickable((By.XPATH, botao_xpath)))
        botao.click()
        print("🟣 Botão 'SPAM POR USUÁRIOS' clicado.")
        time.sleep(2)

        try:
            fechar_xpath = "//div[@id='txt-alerta']//div[text()='Fechar']"
            fechar = wait.until(EC.element_to_be_clickable((By.XPATH, fechar_xpath)))
            fechar.click()
            print("✅ Modal fechado.")
        except:
            print("ℹ️ Nenhum modal exibido.")

        resultado.append(f"{id_campanha} | ✅ SPAM verificado com sucesso")

    except Exception as e:
        mensagem = str(e).lower()
        if "spam" in mensagem or "timeout" in mensagem:
            print("⚠️ Botão de SPAM não encontrado.")
            resultado.append(f"{id_campanha} | ⚠️ Sem botão de SPAM")
        else:
            print("❌ Erro inesperado:", e)
            resultado.append(f"{id_campanha} | ❌ Erro inesperado: {e}")
    print()

def gerar_relatorio_excel(resultado, nome_arquivo="status_campanhas.xlsx"):
    df = pd.DataFrame([r.split("|") for r in resultado], columns=["ID Campanha", "Status"])
    df["ID Campanha"] = df["ID Campanha"].str.strip()
    df["Status"] = df["Status"].str.strip()
    df.to_excel(nome_arquivo, index=False)
    print(f"📊 Arquivo '{nome_arquivo}' gerado com sucesso!")

def salvar_log_spam(resultado, pasta_downloads=os.path.join(os.path.expanduser("~"), "Downloads")):
    data = datetime.now().strftime("%Y-%m-%d")
    caminho = os.path.join(pasta_downloads, f"status_execucao_allin_SPAM_{data}.txt")
    with open(caminho, "w", encoding="utf-8") as f:
        f.write("LOG DE VERIFICAÇÃO DE SPAM - ALL iN\n")
        f.write("=" * 40 + "\n\n")
        for linha in resultado:
            f.write(f"{linha}\n")
    print(f"📝 Log de SPAM salvo em: {caminho}")

# ========================== EXECUÇÃO PRINCIPAL ==========================

def executar_fluxo_spam(
    campanhas_ids=DEFAULT_CAMPANHAS_IDS,
    usuario=DEFAULT_USUARIO,
    senha=DEFAULT_SENHA,
    nome_excel="status_campanhas.xlsx"
):
    resultado = []
    driver, wait = configurar_navegador()
    try:
        realizar_login(driver, wait, usuario, senha)
        for id_campanha in campanhas_ids:
            acessar_campanha(driver, wait, id_campanha, resultado)
        gerar_relatorio_excel(resultado, nome_excel)
        salvar_log_spam(resultado)
        return campanhas_ids, resultado
    except Exception as e:
        print("❌ Erro inesperado durante fluxo de SPAM:")
        traceback.print_exc()
        return campanhas_ids, resultado
    finally:
        time.sleep(5)
        driver.quit()

# # ========================== EXECUÇÃO STANDALONE ==========================

# if __name__ == "__main__":
#     executar_fluxo_spam()
#     print("✅ Execução standalone concluída.")
