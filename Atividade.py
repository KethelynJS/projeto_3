import sqlite3
from plyer import notification
from datetime import datetime
import requests
import pandas as pd

def alerta(nivel, base, etapa):
    
    if nivel == 1:
        titulo = "Alerta Baixo"
    elif nivel == 2:
        titulo = "Alerta Médio"
    elif nivel == 3:
        titulo = "Alerta Alto"
    
    mensagem = f"Falha no carregamento da base {base} na etapa {etapa}\nData: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    notification.notify(
        title = titulo,
        message = mensagem,
    )


url_bancos = "https://brasilapi.com.br/api/banks/v1"
try:
    response = requests.get(url_bancos)
    response.raise_for_status() 
    bancos_data = response.json()        
    
    df =  pd.DataFrame(bancos_data)
    print("tamanho do dataset original {}".format(df.shape))

    df.info() # tipo inicial das colunas

    df['code'].fillna(0.0, inplace=True) # preenchendo a coluna code Nan e atualizando dataset
    
    df2 = df.dropna() # excluindo linhas que contenham dados faltantes
    print("tamanho do dataset atualizado {}".format(df2.shape))

    # convertendo tipo das colunas 
    df2['ispb'] = df2['ispb'].astype('int') 
    df2['code'] = df2['code'].astype('int') 
    df2['name'] = df2['name'].astype('string')
    df2['fullName'] = df2['fullName'].astype('string')

    df2.info() # tipo das colunas   

    df2.iloc[:,-1] = df2.iloc[:,-1].str.upper() # Deixando nome completo em maiúsculo

    print(df2)

    df2_stacked = df2.stack() # transformando colunas em index da tabela

    print(df2_stacked)  

# Criando o banco de dados SQLite
    conn = sqlite3.connect('dados_bancos.db')

    # Inserindo DataFrame no banco de dados
    df2.to_sql('bancos', conn, if_exists='replace', index=False)    

    # Executando a consulta SQL para visualizar dados
    consulta = conn.execute('SELECT * FROM bancos')
    resultados = consulta.fetchall()
    
    print("\nDados no banco de dados:")
    for resultado in resultados:
        print(resultado)  
    

except Exception as e:  
    alerta(3, "bancos", "extração")
    print(f"Erro na extração dos bancos: {e}")

finally:
    conn.close()