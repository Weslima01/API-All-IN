import os
import win32com.client

def enviar_email_com_anexos(destinatario, arquivos, cc=None, cco=None):
    """
    Envia um e-mail com os arquivos anexados usando o Outlook.
    
    Parâmetros:
        destinatario (str): Endereço de e-mail principal.
        arquivos (list): Lista de caminhos dos arquivos a anexar.
        cc (list, opcional): Lista de e-mails para cópia.
        cco (list, opcional): Lista de e-mails para cópia oculta.
    """
    try:
        outlook = win32com.client.Dispatch("Outlook.Application")
        email = outlook.CreateItem(0)

        email.To = destinatario
        if cc:
            email.CC = "; ".join(cc)
        if cco:
            email.BCC = "; ".join(cco)

        email.Subject = "Relatórios All In"
        email.Body = (
            "Olá,\n\n"
            "Segue em anexo os relatórios gerados automaticamente pelo sistema.\n\n"
            "Caso tenha dúvidas ou precise de informações adicionais, fico à disposição.\n"
        )

        for arquivo in arquivos:
            if os.path.exists(arquivo):
                email.Attachments.Add(arquivo)
            else:
                print(f"⚠️ Arquivo não encontrado: {arquivo}")

        email.Display()  # Para revisão manual
        # email.Send()   # Use se quiser enviar automaticamente

        print("✅ E-mail preparado com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao preparar o e-mail: {e}")
