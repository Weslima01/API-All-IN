# Projeto All in Pipeline

Este repositório contém scripts de automação, coleta de dados via API e geração de relatórios da plataforma All iN.

## Estrutura

- `api_client.py` – Cliente de API para capturar dados
- `download_all.py` – Automatiza downloads e salvamento
- `envio_email.py` – Envio automático de relatórios
- `tratamento.py` – Limpeza e formatação dos dados
- `dados/` – Armazenamento de arquivos .json, .xlsx, etc.
- `status_campanhas.xlsx` – Monitoramento das campanhas

## Como executar

1. Configure seu ambiente virtual:
   ```
   python -m venv venv
   source venv/bin/activate  # Linux/macOS
   .\venv\Scripts\activate   # Windows
   ```

2. Instale as dependências:
   ```
   pip install -r requirements.txt
   ```

3. Execute o script principal:
   ```
   python main_final.py
   ```

## Autor
Wesley Santos – Neotass
