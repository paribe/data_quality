import sqlite3
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
import os

# 0. SETUP - Criar e popular o banco de dados SQLite
def create_sqlite_db(db_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Criar tabela 'empregados'
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS empregados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            idade INTEGER NOT NULL,
            salario REAL
        )
    ''')
    
    # Inserir dados na tabela
    empregados_data = [
        ('Ana', 25, 3000.00),
        ('Carlos', 32, 4500.00),
        ('Bianca', 17, 1500.00),  # Dado que vai falhar na validação (idade < 18)
        ('David', 40, None),      # Salário nulo, permitido
        ('Eva', 22, 2800.00)
    ]
    
    cursor.executemany('''
        INSERT INTO empregados (nome, idade, salario)
        VALUES (?, ?, ?)
    ''', empregados_data)
    
    conn.commit()
    conn.close()
    print("Banco de dados SQLite criado e populado com sucesso.")

# 1. EXTRACT - Conectar ao SQLite e extrair dados
def extract_data_from_sqlite(db_path: str, query: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# 2. TRANSFORM - Validar dados com Pandera
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    schema = DataFrameSchema({
        "id": Column(pa.Int, checks=pa.Check.greater_than_or_equal_to(1)),
        "nome": Column(pa.String, nullable=False),
        "idade": Column(pa.Int, pa.Check(lambda x: x >= 18)),  # Apenas maiores de 18 anos
        "salario": Column(pa.Float, nullable=True),
    })
    validated_df = schema.validate(df)
    return validated_df

# 3. LOAD - Carregar os dados em um "Data Lake" (simulação de um arquivo CSV)
def load_data_to_datalake(df: pd.DataFrame, output_path: str):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Dados carregados no Data Lake: {output_path}")

# Função principal para orquestrar o pipeline
def etl_pipeline():
    db_path = "database.sqlite"
    query = "SELECT * FROM empregados"
    output_path = "datalake/empregados.csv"
    
    # 0. Criar e popular o banco de dados SQLite
    create_sqlite_db(db_path)
    
    # ETL Pipeline
    print("Extraindo dados do SQLite...")
    df = extract_data_from_sqlite(db_path, query)
    
    print("Validando dados com Pandera...")
    try:
        validated_df = validate_data(df)
        print("Validação bem-sucedida.")
        
        print("Carregando dados no Data Lake...")
        load_data_to_datalake(validated_df, output_path)
        
    except pa.errors.SchemaError as e:
        print(f"Erro na validação dos dados: {e}")

# Executar o pipeline
etl_pipeline()
