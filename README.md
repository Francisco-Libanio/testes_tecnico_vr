# Teste técnico

---

## Pré-requisitos

Certifique-se de ter as seguintes ferramentas instaladas em seu sistema:

-   **Python 3.10 ou superior:** Você pode verificar sua versão com `python --version`.
-   **Poetry:** Gerenciador de dependências e empacotamento. Instale-o com as instruções oficiais [aqui](https://python-poetry.org/docs/#installation).

---

## Instalação e Configuração

Siga os passos abaixo para configurar o ambiente de desenvolvimento.

1.  **Clone o repositório:**

    ```bash
    git clone 
    cd seu-projeto
    ```

2.  **Instale as dependências com Poetry:**

    O Poetry vai ler o arquivo `pyproject.toml` e instalar todas as bibliotecas necessárias, criando um ambiente virtual isolado.

    ```bash
    poetry install
    ```

3.  **Ative o ambiente virtual:**

    Para garantir que você está usando o ambiente correto, ative-o com o comando `poetry shell`. A partir de agora, todos os comandos Python serão executados neste ambiente.

    ```bash
    poetry shell
    ```

---

## Executando o Projeto


## Testes

Para executar os testes do projeto, use o `pytest`. Certifique-se de que as dependências de desenvolvimento estejam instaladas (`poetry install`).

```bash
pytest
