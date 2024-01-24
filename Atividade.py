from plyer import notification
from datetime import datetime
import requests
import pandas as pd
from pyspark.sql import SparkSession

url_bancos = "https://brasilapi.com.br/api/banks/v1"
response = None
success_api_con = False

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

try:
    response = requests.get(url_bancos)
    response.raise_for_status() 
    success_api_con = True

except Exception as e:  
    alerta(3, "bancos", "extração")
    print(f"Erro na extração dos bancos: {e}")


def dados_api():
    
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

    # Criando uma sessão Spark 

    pandas_df = df2

    spark = SparkSession.builder.appName('PandasToSparkDF').getOrCreate()

    # Convertendo um Pandas Dataframe em Spark Dataframe
    df_spark = spark.createDataFrame(pandas_df)

    # Imprimindo o dataframe Spark
    # display(df_spark)

    # Criando uma tabela temporária a partir do dataframe Pandas convertido em Spark dataframe
    df_spark.createOrReplaceTempView("TB_TEMP_BANKS_BRASIL_API")

    # Validações das colunas do dataframe spark mediante consultas em mysql

    # Validando o campo fullName - Retorna apenas as linhas que no campo nome contêm apenas caracteres, espaços e acentos
    df = spark.sql("""

        SELECT *

        FROM  TB_TEMP_BANKS_BRASIL_API AS tb

    WHERE tb.fullName REGEXP '^[a-zA-ZÀ-ú ]+$'; 
    
    """)

    print("Quantidade de tuplas que contêm apenas caracteres, espaços e acentos no campo fullName:",df.count())
    display(df)


    # Validando o campo name - Retorna apenas as linhas que no campo nome que possuem número no nome
    df = spark.sql("""

        SELECT *

        FROM  TB_TEMP_BANKS_BRASIL_API AS tb
        WHERE tb.name NOT REGEXP '^[^0-9]+$'; -- CAMPO NOME QUE CONTÊM NÚMEROS   
        
    """)

    print("Quantidade de tuplas que contêm números no campo name:",df.count())
    display(df)



    # Validando o campo ispb - Retorna apenas as linhas que no campo ispb está contêm no máximo 8 digitos inteiros
    df = spark.sql("""

        SELECT *

        FROM  TB_TEMP_BANKS_BRASIL_API AS tb   

    WHERE tb.ispb  BETWEEN 0 AND 99999999 
        
    """)

    print("Quantidade de tuplas VÁLIDAS no campo ispb:",df.count())
    display(df)

    # Validando o campo Code - achando registros com mesmo código
    df = spark.sql("""
                
        SELECT 
        
        tb1.*,
        COD_REPETIDO.CONTADOR

        FROM  TB_TEMP_BANKS_BRASIL_API AS tb1,

        (SELECT   

            tb.Code,  
            COUNT(*) AS CONTADOR

        FROM TB_TEMP_BANKS_BRASIL_API AS tb

        GROUP BY 1
        
        HAVING CONTADOR > 1) AS COD_REPETIDO   

        WHERE COD_REPETIDO.Code = tb1.Code   
        
    """)

    print("Quantidade de tuplas que possuem o mesmo Code:",df.count())
    display(df)

if success_api_con == True:
    dados_api()