import mysql.connector
import pandas as pd
import re

def create_connection():
    try:
        connection = mysql.connector.connect(
            user="adminneotass",
            password="1.NeoTass@2021*SartTi",
            host="neotasssp1.mysql.database.azure.com",
            database="bd_conexao_azul_new",
            ssl_ca=r"C:\Users\WesleyLimaNeotassMar\Python\DigiCertGlobalRootCA.pem",
            ssl_verify_cert=True
        )
        if connection.is_connected():
            print("✅ Conectado com sucesso ao MySQL!")
            return connection
    except mysql.connector.Error as e:
        print(f"❌ Erro na conexão com MySQL: {e}")
        return None

def upload_excel_to_mysql_incremental(file_path, table_name, unique_cols):
    conn = create_connection()
    if conn:
        try:
            df = pd.read_excel(file_path)
            df.columns = [re.sub(r'\W+', '_', col.strip().lower()) for col in df.columns]

            cursor = conn.cursor()

            col_defs = []
            for col in df.columns:
                tipo = "TEXT"
                if pd.api.types.is_integer_dtype(df[col]):
                    tipo = "INT"
                elif pd.api.types.is_float_dtype(df[col]):
                    tipo = "FLOAT"
                elif pd.api.types.is_bool_dtype(df[col]):
                    tipo = "BOOLEAN"
                col_defs.append(f"`{col}` {tipo}")

            unique_clause = f", UNIQUE({', '.join([f'`{c}`' for c in unique_cols])})" if unique_cols else ""

            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    {', '.join(col_defs)}
                    {unique_clause}
                );
            """
            cursor.execute(create_sql)

            cols = ', '.join(df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            updates = ', '.join([f"`{col}`=VALUES(`{col}`)" for col in df.columns])

            insert_sql = f"""
                INSERT INTO {table_name} ({cols})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {updates};
            """

            data = [tuple(None if pd.isna(v) else v for v in row) for _, row in df.iterrows()]
            cursor.executemany(insert_sql, data)

            conn.commit()
            print(f"✅ {len(data)} registros processados (incremental) na tabela `{table_name}`.")
        except Exception as e:
            print(f"❌ Erro ao inserir dados na tabela `{table_name}`: {e}")
        finally:
            cursor.close()
            conn.close()
