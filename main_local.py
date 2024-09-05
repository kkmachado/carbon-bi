import time
import subprocess
from datetime import datetime
import logging

# Configura o logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler("scripts_local.log"),  # Salva o log em um arquivo
        logging.StreamHandler()  # Também imprime no console
    ]
)

# Função para fazer o print e o log ao mesmo tempo
def log_e_print(mensagem, nivel='info'):
    if nivel == 'info':
        print(mensagem)
        logging.info(mensagem)
    elif nivel == 'error':
        print(mensagem)
        logging.error(mensagem)

# Lista de scripts a serem executados
scripts = [
    'python3 ph_paid_users_local.py',
    'python3 ph_overview_local.py',
    'python3 ph_rd_lp_pageviews_local.py',
    'python3 rd_station_all_deals_local.py',
    'python3 rd_station_BDR_deals_local.py',
    'python3 trello_local.py'
]

try:
    while True:
        log_e_print("### I N Í C I O ###")
        log_e_print("----------------------------------------")

        # Captura o tempo inicial do loop completo
        tempo_inicial_loop = time.time()

        # Itera sobre a lista de scripts e executa cada um
        for comando in scripts:
            try:
                log_e_print(f"Executando: {comando}")
                resultado = subprocess.run(comando, shell=True, check=True)
                log_e_print("-----------")
            except subprocess.CalledProcessError as e:
                log_e_print(f"O script {comando} falhou com o código de erro {e.returncode}", nivel='error')
            except Exception as e:
                log_e_print(f"Erro ao executar o script {comando}: {e}", nivel='error')

        # Calcula o tempo de execução total do loop
        tempo_execucao_loop = time.time() - tempo_inicial_loop

        # Captura o horário de término da execução do loop
        horario_termino_loop = datetime.now().strftime("%H:%M:%S")

        # Exibe o tempo de execução total e o horário de término do loop
        log_e_print(f"Tempo total de execução do loop: {tempo_execucao_loop:.2f} segundos")
        log_e_print(f"Horário de término do loop: {horario_termino_loop}")
        log_e_print("----------------------------------------")
        log_e_print("### F I M ###")

        # Aguarda 1 hora (3600 segundos) antes de repetir o loop
        time.sleep(3600)

except KeyboardInterrupt:
    log_e_print("Script interrompido pelo usuário.")