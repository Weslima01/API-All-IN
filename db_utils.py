import mysql.connector
import pandas as pd
from datetime import datetime
import os

def salvar_log_em_downloads(nome_arquivo, df):
    """Salva um DataFrame como Excel na pasta Downloads do usuário"""
    pasta_downloads = os.path.join(os.path.expanduser("~"), "Downloads")
    caminho = os.path.join(pasta_downloads, nome_arquivo)
    df.to_excel(caminho, index=False)
    print(f"📁 Log salvo: {caminho}", flush=True)

def create_connection():
    try:
        conn = mysql.connector.connect(
            user="adminneotass",
            password="1.NeoTass@2021*SartTi", #TODO: ALTERAR PARA VARIAVEL AMBIENTE
            host="neotasssp1.mysql.database.azure.com",
            database="bd_samsung",
            ssl_ca=r"C:\\Users\\Wesley\\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\\Python\\DigiCertGlobalRootCA.pem",
            ssl_verify_cert=False
        )
        if conn.is_connected():
            print("✅ Conectado com sucesso ao MySQL!", flush=True)
            return conn
    except mysql.connector.Error as e:
        print(f"❌ Erro na conexão com MySQL: {e}", flush=True)
    return None

def carregar_planilhas(path_envio, path_depara):
    df_envio = pd.read_excel(path_envio)
    df_depara = pd.read_excel(path_depara)
    return df_envio, df_depara

def transformar_linha(row, colunas):
    linha = []
    for j, v in enumerate(row):
        col = colunas[j]
        try:
            if pd.isna(v):
                linha.append(None)
            elif isinstance(v, (pd.Timestamp, pd.Timedelta)):
                linha.append(v.to_pydatetime())
            elif isinstance(v, (int, float, str, bool)):
                linha.append(v)
            elif hasattr(v, 'item'):
                linha.append(v.item())
            else:
                linha.append(str(v))
        except Exception as e:
            print(f"❌ Erro na linha, coluna '{col}': {v} ({type(v)})", flush=True)
            raise e
    return tuple(linha)

def alimentar_envios(path_envio, path_depara, conn):
    try:
        df_envio, df_depara = carregar_planilhas(path_envio, path_depara)

        campanhas_validas = df_depara["id_campanha"].dropna().unique()
        df_envio = df_envio[df_envio["id_campanha"].isin(campanhas_validas)]

        if "nm_usuario_sub" in df_depara.columns:
            df_envio = df_envio.merge(df_depara[["id_campanha", "nm_usuario_sub"]],
                                      on="id_campanha", how="left")
        else:
            df_envio["nm_usuario_sub"] = None
            print("⚠️ Coluna 'nm_usuario_sub' não encontrada no DE/PARA. Valor None será atribuído.", flush=True)

        colunas_envio = [
            "id_campanha", "nome_campanha", "tipo", "lista", "total_envio", "total_optout",
            "total_entregues", "total_nao_entregues", "erro_permanente", "erro_temporario",
            "total_aberto", "total_nao_aberto", "nm_subject",
            "data_inicio", "hora_inicio", "data_encerramento", "total_click_unicos", "total_click"
        ]

        # Adiciona nm_usuario_sub dinamicamente se estiver presente no df_envio
        if "nm_usuario_sub" in df_envio.columns:
            colunas_envio.insert(10, "nm_usuario_sub")
        else:
            df_envio["nm_usuario_sub"] = None
            colunas_envio.insert(10, "nm_usuario_sub")

        df_envio["data_inicio"] = pd.to_datetime(df_envio["data_inicio"], errors="coerce").dt.date
        df_envio["data_encerramento"] = pd.to_datetime(df_envio["data_encerramento"], format="%d/%m/%Y %H:%M", errors="coerce").dt.date
        df_envio["hora_inicio"] = pd.to_datetime(df_envio["hora_inicio"], format="%H:%M", errors="coerce").dt.time

        df_envio = df_envio[colunas_envio].copy()
        df_envio = df_envio.astype(object).where(pd.notnull(df_envio), None)
        df_envio.drop_duplicates(subset=["id_campanha"], inplace=True)

        if df_envio.empty:
            print("⚠️ Nenhuma campanha para inserir.", flush=True)
            print("🏁 Execução de alimentar_envios finalizada.", flush=True)
            return

        data = [transformar_linha(row, colunas_envio) for _, row in df_envio.iterrows()]

        placeholders = ', '.join(['%s'] * len(colunas_envio))
        update_sql = ',\n                '.join([f"{col}=VALUES({col})" for col in colunas_envio if col != "id_campanha"])

        insert_sql = f'''
            INSERT INTO fato_comunicacao_envio (
                {', '.join(colunas_envio)}
            ) VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE
                {update_sql};
        '''

        cursor = conn.cursor()
        cursor.executemany(insert_sql, data)
        conn.commit()
        cursor.close()
        print(f"✅ {len(data)} campanhas inseridas/atualizadas.", flush=True)
        salvar_log_em_downloads("log_envios_atualizados.xlsx", pd.DataFrame(data, columns=colunas_envio))
        print("🏁 Execução de alimentar_envios finalizada.", flush=True)

    except Exception as e:
        print(f"❌ Erro ao inserir envios: {e}", flush=True)

def alimentar_aberturas(path_abertura, path_depara, conn):
    try:
        df_abertura = pd.read_excel(path_abertura)
        df_depara = pd.read_excel(path_depara)

        campanhas_validas = df_depara["id_campanha"].dropna().unique()
        df_filtrado = df_abertura[df_abertura["id_campanha"].isin(campanhas_validas)]

        df_filtrado = df_filtrado.rename(columns={"data_click": "data_abertura"})
        df_filtrado["data_abertura"] = pd.to_datetime(df_filtrado["data_abertura"], errors="coerce")
        df_filtrado = df_filtrado.dropna(subset=["data_abertura"])

        df_final = df_filtrado[["id_campanha", "nm_email", "data_abertura"]].copy()
        df_final = df_final.astype(object).where(pd.notnull(df_final), None)
        df_final.drop_duplicates(inplace=True)

        if df_final.empty:
            print("⚠️ Nenhuma nova abertura para inserir.", flush=True)
            print("🏁 Execução de alimentar_aberturas finalizada.", flush=True)
            return

        data = [transformar_linha(row, df_final.columns) for _, row in df_final.iterrows()]

        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO fato_comunicacao_abertura
            (id_campanha, nm_email, data_abertura)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                nm_email = VALUES(nm_email),
                data_abertura = VALUES(data_abertura);
        """, data)
        conn.commit()
        cursor.close()
        print(f"✅ {len(data)} aberturas inseridas/atualizadas.", flush=True)
        salvar_log_em_downloads("log_aberturas_atualizadas.xlsx", pd.DataFrame(data, columns=df_final.columns))
        print("🏁 Execução de alimentar_aberturas finalizada.", flush=True)

    except Exception as e:
        print(f"❌ Erro ao inserir aberturas: {e}", flush=True)

def get_max_id_clique(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(id_clique) FROM fato_comunicacao_clique;")
        max_id = cursor.fetchone()[0]
        cursor.close()
        return max_id if max_id is not None else 0
    except:
        return 0

def alimentar_cliques(path_clique, path_depara, conn):
    try:
        df_clique = pd.read_excel(path_clique)
        df_depara = pd.read_excel(path_depara)

        campanhas_validas = df_depara["id_campanha"].dropna().unique()
        df_filtrado = df_clique[df_clique["id_campanha"].isin(campanhas_validas)]

        df_filtrado = df_filtrado.rename(columns={"data_click": "data_clique"})
        df_filtrado["data_clique"] = pd.to_datetime(df_filtrado["data_clique"], errors="coerce")
        df_filtrado = df_filtrado.dropna(subset=["data_clique"])

        df_filtrado = df_filtrado[["id_campanha", "nm_email", "data_clique"]].copy()
        df_filtrado.drop_duplicates(inplace=True)
        df_filtrado = df_filtrado.astype(object).where(pd.notnull(df_filtrado), None)

        if df_filtrado.empty:
            print("⚠️ Nenhum clique novo para inserir.", flush=True)
            print("🏁 Execução de alimentar_cliques finalizada.", flush=True)
            return

        start_id = get_max_id_clique(conn) + 1
        df_filtrado.insert(0, "id_clique", range(start_id, start_id + len(df_filtrado)))

        data = [transformar_linha(row, df_filtrado.columns) for _, row in df_filtrado.iterrows()]

        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO fato_comunicacao_clique
            (id_clique, id_campanha, nm_email, data_clique)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                id_campanha = VALUES(id_campanha),
                nm_email = VALUES(nm_email),
                data_clique = VALUES(data_clique);
        """, data)
        conn.commit()
        cursor.close()
        print(f"✅ {len(data)} cliques inseridos/atualizados.", flush=True)
        salvar_log_em_downloads("log_cliques_atualizados.xlsx", pd.DataFrame(data, columns=df_filtrado.columns))
        print("🏁 Execução de alimentar_cliques finalizada.", flush=True)

    except Exception as e:
        print(f"❌ Erro ao inserir cliques: {e}", flush=True)

def get_max_id_optout(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(id_optout) FROM fato_comunicacao_optout;")
        max_id = cursor.fetchone()[0]
        cursor.close()
        return max_id if max_id is not None else 0
    except:
        return 0

def alimentar_optouts(path_optout, path_depara, conn):
    try:
        df_optout = pd.read_excel(path_optout)
        df_optout.rename(columns={
            'ID Campanha': 'id_campanha',
            'Email': 'nm_email',
            'Nome Campanha': 'nm_usuario_sub'
        }, inplace=True)
        df_optout["tipo_evento"] = "Opt-out"
        if "data_evento" not in df_optout.columns:
            df_optout["data_evento"] = datetime.now()

        df_depara = pd.read_excel(path_depara)
        campanhas_validas = df_depara["id_campanha"].dropna().unique()
        df_filtrado = df_optout[df_optout["id_campanha"].isin(campanhas_validas)]

        df_filtrado = df_filtrado[[
            "id_campanha", "nm_email", "nm_usuario_sub", "tipo_evento", "data_evento"
        ]].copy()

        df_filtrado.drop_duplicates(inplace=True)
        df_filtrado = df_filtrado.astype(object).where(pd.notnull(df_filtrado), None)

        if df_filtrado.empty:
            print("⚠️ Nenhum opt-out novo para inserir.", flush=True)
            print("🏁 Execução de alimentar_optouts finalizada.", flush=True)
            return

        start_id = get_max_id_optout(conn) + 1
        df_filtrado.insert(0, "id_optout", range(start_id, start_id + len(df_filtrado)))

        data = [transformar_linha(row, df_filtrado.columns) for _, row in df_filtrado.iterrows()]

        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO fato_comunicacao_optout
            (id_optout, id_campanha, nm_email, nm_usuario_sub, tipo_evento, data_evento)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                nm_email = VALUES(nm_email),
                nm_usuario_sub = VALUES(nm_usuario_sub),
                tipo_evento = VALUES(tipo_evento)
        """, data)
        conn.commit()
        cursor.close()
        print(f"✅ {len(data)} opt-outs inseridos/atualizados.", flush=True)
        salvar_log_em_downloads("log_optouts_atualizados.xlsx", pd.DataFrame(data, columns=df_filtrado.columns))
        print("🏁 Execução de alimentar_optouts finalizada.", flush=True)

    except Exception as e:
        print(f"❌ Erro ao inserir opt-outs: {e}", flush=True)

    # Execução principal
    path_envio = r"C:\\Users\\Wesley\\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\\Python\\allin_pipeline\\dados\\silver\\get_envio_info.xlsx"
    path_abertura = r"C:\\Users\\Wesley\\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\\Python\\allin_pipeline\\dados\\silver\\relatorio_abertura.xlsx"
    path_depara = r"C:\\Users\\Wesley\\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\\Python\\allin_pipeline\\DE_PARA_CAMPANHAS.xlsx"
    path_clique = r"C:\\Users\\Wesley\\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\\Python\\allin_pipeline\\dados\\silver\\relatorio_clique.xlsx"
    path_optout = r"C:\\Users\\Wesley\\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\\Python\\allin_pipeline\\dados\\silver\\optout_info.xlsx"



def subida_mysql(path_envio, path_abertura, path_depara, path_clique, path_optout):
    conn = create_connection()
    if conn:
        try:
            alimentar_envios(path_envio, path_depara, conn)
            alimentar_aberturas(path_abertura, path_depara, conn)
            alimentar_cliques(path_clique, path_depara, conn)
            alimentar_optouts(path_optout, path_depara, conn)
            print("✅ Execução finalizada com sucesso!", flush=True)
        finally:
            conn.close()

subida_mysql(
    path_envio = r"C:\\Users\\Wesley\\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\\Python\\allin_pipeline\\dados\\silver\\get_envio_info.xlsx",
path_abertura = r"C:\\Users\\Wesley\\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\\Python\\allin_pipeline\\dados\\silver\\relatorio_abertura.xlsx",
path_depara = r"C:\\Users\\Wesley\\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\\Python\\allin_pipeline\\DE_PARA_CAMPANHAS.xlsx",
path_clique = r"C:\\Users\\Wesley\\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\\Python\\allin_pipeline\\dados\\silver\\relatorio_clique.xlsx",
path_optout = r"C:\\Users\\Wesley\\OneDrive - NEOTASS PUBLICIDADE E PRODUCOES LT\\Python\\allin_pipeline\\dados\\silver\\optout_info.xlsx"

)
            
