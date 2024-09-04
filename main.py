import time
import subprocess

# Lista de scripts a serem executados
scripts = [
    'python3 ph_paid_users.py',
    'python3 ph_overview.py',
    'python3 ph_rd_lp_pageviews.py',
    'python3 rd_station_all_deals.py',
    'python3 rd_station_BDR_deals.py',
    'python3 trello.py'
]

try:
    while True:
        print("### I N Í C I O ###")
        print("----------------------------------------")

        # Itera sobre a lista de scripts e executa cada um
        for comando in scripts:
            try:
                print(f"Executando: {comando}")
                resultado = subprocess.run(comando, shell=True, check=True)
                print(f"Saída do script {comando}:\n{resultado.stdout}")
            except subprocess.CalledProcessError as e:
                print(f"O script {comando} falhou com o código de erro {e.returncode}")
            except Exception as e:
                print(f"Erro ao executar o script {comando}: {e}")

        print("----------------------------------------")
        print("### F I M ###")
        time.sleep(5)
except KeyboardInterrupt:
    print("Script interrompido pelo usuário.")