# Validar campos antes da Ingestão de Dados através de Contrato

O que foi adicionado:
Função create_sqlite_db:

# Cria a tabela empregados no banco de dados SQLite se ela ainda não existir.
Insere os dados diretamente no banco de dados, incluindo um exemplo com dados válidos e inválidos (como idade = 17 que falhará na validação).
Autoincremento do id:

# A coluna id foi definida como AUTOINCREMENT, para que os registros inseridos automaticamente recebam um ID único.
Execução do pipeline completo:

Agora, o script cria e popula o banco SQLite na primeira execução e, em seguida, extrai, valida e carrega os dados.
Output esperado:
O banco de dados será criado com os registros especificados.
Durante a validação, o registro da Bianca (idade 17) não passará na validação.
Somente os registros válidos serão carregados no "Data Lake" (arquivo CSV).
Agora o código está totalmente autônomo, desde a criação do banco de dados até a execução do ETL!
