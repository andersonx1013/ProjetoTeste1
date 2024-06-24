#Arquivo db/createDataBase.py
import psycopg2
import os
from psycopg2 import OperationalError, Error

# Defina os parâmetros de conexão com o banco de dados a partir das variáveis de ambiente
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")

def create_table():
    try:
        # Tente estabelecer a conexão com o banco de dados
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
        print("Conexão com o banco de dados estabelecida com sucesso.")
    except OperationalError as e:
        print(f"Erro ao conectar com o banco de dados: {e}")
        return

    try:
        with conn.cursor() as cur:
            # Verifique se a tabela já existe
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'produtos'
                );
            """)
            table_exists = cur.fetchone()[0]
            
            if table_exists:
                print("A tabela 'produtos' já existe no banco de dados.")
                return

            # Defina o comando SQL para criar a tabela
            create_table_query = """
            CREATE TABLE IF NOT EXISTS produtos (
                id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description TEXT,
                pricing_amount NUMERIC(10, 2),
                pricing_currency VARCHAR(10),
                availability_quantity INT,
                availability_timestamp TIMESTAMP,
                category VARCHAR(255)
            );
            """

            # Tente executar a criação da tabela
            cur.execute(create_table_query)
            conn.commit()
            print("Tabela 'produtos' criada com sucesso!")
    except Error as e:
        print(f"Erro ao verificar/criar a tabela: {e}")
    finally:
        # Feche a conexão
        try:
            conn.close()
        except OperationalError as e:
            print(f"Erro ao fechar a conexão com o banco de dados: {e}")

if __name__ == "__main__":
    create_table()
