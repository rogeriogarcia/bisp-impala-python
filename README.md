# Pacote Conector Impala

Este pacote fornece uma maneira simples de conectar a um banco de dados Impala usando Python.

## Pré-requisitos

- Python 3.7+
- Acesso a um cluster Impala.

## Configuração

1.  **Baixar o Projeto (Clonar Repositório)**:
    Para obter o código do projeto, clone o repositório do GitHub usando o seguinte comando no seu terminal:
    ```bash
    git clone https://github.com/rogeriogarcia/bisp-impala-python.git
    ```
    Isso criará um diretório chamado `bisp-impala-python` com todos os arquivos do projeto. Navegue para dentro deste diretório para os próximos passos:
    ```bash
    cd bisp-impala-python
    ```

2.  **(Opcional, mas Recomendado) Criar e Ativar Ambiente Virtual**:
    Dentro do diretório `bisp-impala-python`, é uma boa prática criar um ambiente virtual para isolar as bibliotecas do projeto. Execute:

    *   Para criar o ambiente virtual (o nome `.venv` é uma convenção comum):
        ```bash
        python -m venv .venv
        ```

    *   Para ativar o ambiente virtual:
        *   No Windows:
            ```bash
            .\.venv\Scripts\activate
            ```
        *   No macOS e Linux:
            ```bash
            source .venv/bin/activate
            ```
    Você saberá que o ambiente virtual está ativo se o nome dele aparecer no início do seu prompt de comando (ex: `(.venv) ...\bisp-impala-python>`).

3.  **Instalar Dependências**:
    Com o ambiente virtual ativo (se você criou um), ainda dentro do diretório `bisp-impala-python`, instale os pacotes Python necessários:
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configurar Variáveis de Ambiente**:
    Na raiz do diretório `bisp-impala-python`, crie um arquivo `.env` copiando o arquivo `.env.example`:
    ```bash
    cp .env.example .env
    ```
    Edite o arquivo `.env` com os detalhes reais da sua conexão Impala:
    - `BISP_HOST`: Nome do host ou endereço IP do servidor Impala.
    - `BISP_PORT`: Número da porta para o serviço Impala (padrão: 21051).
    - `BISP_DATABASE`: Nome do banco de dados ao qual se conectar.
    - `BISP_USER`: Nome de usuário para autenticação.
    - `BISP_PASSWORD`: Senha para autenticação.
    - `BISP_AUTH_MECHANISM`: Mecanismo de autenticação (ex: `PLAIN`, `GSSAPI`).
    - `BISP_USE_SSL`: `True` ou `False`.
    - `BISP_CA_CERT_PATH`: Caminho absoluto para o arquivo do seu certificado CA se o SSL estiver habilitado. Você pode usar o arquivo `certs/example_ca.pem` fornecido como modelo, mas substitua-o pelo seu certificado CA real.

## Uso

Importe e use a função `get_bisp_connection` da biblioteca:

```python
from bisp_connector_lib import get_bisp_connection, ImpalaConnectionError

try:
    # Certifique-se de que seu arquivo .env está em um local que find_dotenv() possa encontrar,
    # ou que as variáveis de ambiente estejam definidas em todo o sistema.
    conn = get_bisp_connection()
    cursor = conn.cursor()

    print("Executando consulta: SHOW DATABASES")
    cursor.execute("SHOW DATABASES")
    databases = cursor.fetchall()

    print("Bancos de dados:")
    for db in databases:
        print(f"- {db[0]}")

    cursor.close()
    conn.close()
    print("Conexão fechada com sucesso.")

except ImpalaConnectionError as e:
    print(f"Erro ao conectar ao Impala: {e}")
except Exception as e:
    print(f"Ocorreu um erro inesperado: {e}")