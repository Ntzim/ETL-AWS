import pandas as pd # para manipulação de dados
import boto3   # Importando o boto3, ele permite que os desenvolvedores Python interajam com serviços AWS, como S3.
from io import BytesIO  # Permite tratar bytes em memória como um "arquivo virtual" para leitura.
import psycopg2 # Para conectar e interagir com o banco de dados Redshift
import sys # Acesso a funções e variáveis do sistema.
from awsglue.utils import getResolvedOptions # Recupera parâmetros definidos no GlueJob.

########## Extração
print('Iniciando extração de dados')
# Inicializa o cliente do boto3 para interagir com o serviço Amazon S3
s3 = boto3.client('s3')

##Define o nome do bucket S3 onde o arquivo CSV está armazenado e o caminho (chave) do objeto dentro do bucket S3 que será acessado
bucket_name = "premierleague1"
object_key = "premier_league_all_matches.csv"

# Obtenção do objeto CSV do S3
response = s3.get_object(Bucket=bucket_name, Key=object_key)

# Lendo o conteúdo do objeto obtido em um buffer temporário.
buffer = BytesIO(response['Body'].read())

# Lendo o buffer diretamente com o pandas para obter um DataFrame.
print(f'Iniciando leitura dos dados do arquivo {object_key} no bucket {bucket_name}')
df = pd.read_csv(buffer)
print('Dados lidos com sucesso')

########## Transformação
# Demanda 1
#Criando a coluna Home Goals(Gols Casa)
df['Home_Goals'] = df['Score'].str.split('–', expand=True)[0]
#Criando a coluna Away Goals(Gols Visitante)
df['Away_Goals'] = df['Score'].str.split('–', expand=True)[1]

# Tratando Valores nulos
df['Attendance'].fillna(-1, inplace=True)

#Convertendo para INT
df['Home_Goals'] = pd.to_numeric(df['Home_Goals'], errors='coerce')
df['Away_Goals'] = pd.to_numeric(df['Away_Goals'], errors='coerce')

# Demanda 2
#Criando variavel game
df['Game'] = df['Home_Team'] + ' vs ' + df['Away_Team']

#Demanda 3
df['total_goals'] =(df['Home_Goals'] + df['Away_Goals'])

#Demanda 4
# Tratar duplicatas e nulos

#Dropando duplicatas
df.drop_duplicates()

########## Carregamento(Load)
print('Iniciando Carregamento de dados no Redshift')

# Informações de Conexão com o Redshift
args = getResolvedOptions(sys.argv, [
    'REDSHIFT_HOST',
    'REDSHIFT_DBNAME',
    'REDSHIFT_PORT',
    'REDSHIFT_USER',
    'REDSHIFT_PASSWORD'
])

# Atribui os valores dos argumentos a variáveis
redshift_host = args['REDSHIFT_HOST']
redshift_dbname = args['REDSHIFT_DBNAME']
redshift_port = args['REDSHIFT_PORT']
redshift_user = args['REDSHIFT_USER']
redshift_password = args['REDSHIFT_PASSWORD']

# Estabelecendo uma conexão com o banco de dados usando a biblioteca psycopg2.
conn = psycopg2.connect(
    host=redshift_host,         # Endereço do servidor do banco de dados.
    dbname=redshift_dbname,     # Nome do banco de dados.
    user=redshift_user,         # Nome de usuário para conexão.
    password=redshift_password, # Senha do usuário.
    port=redshift_port          # Porta para conexão.
)

# Criando um objeto cursor que permite executar comandos SQL no banco de dados.
cursor = conn.cursor()


# Query SQL para excluir a tabela 'dados_finais' se ela já existir no banco de dados.
# Esta ação é irreversível e, em cenários de produção, pode não ser o desejado.É apenas para fins didáticos
drop_table_query = """
DROP TABLE IF EXISTS Premier_League;
"""

# Executando o comando de exclusão
cursor.execute(drop_table_query)

# Query SQL para criar uma nova tabela chamada 'dados_finais' com determinadas colunas e tipos.
create_table_query = """
CREATE TABLE Premier_League (
    Semana VARCHAR(20),
    Dia DATE,
    Tempo TIME NOT NULL,
    Time_Casa VARCHAR(50),
    Casa_xG DECIMAL(5, 2),
    Resultado_Partida VARCHAR(10),
    Visitante_xG DECIMAL(5, 2),
    Time_Visitante VARCHAR(50),
    Torcida INT,
    Estadio VARCHAR(100),
    Juiz VARCHAR(50),
    Gols_Casa INT,
    Gols_Visitante INT,
    Partida VARCHAR(50),
    Total_Gols DECIMAL(4, 2)
);
"""

# Executando o comando de criar a tabela
cursor.execute(create_table_query)

# Confirmando (commit) as alterações no banco de dados
conn.commit()

# Inserindo dados do DataFrame na tabel, linha a linha
print('Inserindo dados no Redshift')
for index, row in df.iterrows():
    insert_query = """
    INSERT INTO Premier_League (
        Semana, Dia, Tempo, Time_Casa, Casa_xG, Resultado_Partida, Visitante_xG, Time_Visitante, Torcida, 
        Estadio, Juiz, Gols_Casa,  Gols_Visitante, Partida,  Total_Gols
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_query, tuple(row))
    conn.commit()

# Fechando o cursor e a conexão com o banco de dados, para liberar os recursos.
cursor.close()
conn.close()

print(f'Carregamento de dados no Redshift concluído com sucesso!Foram carregados {df.shape[0]} registros na tabela')