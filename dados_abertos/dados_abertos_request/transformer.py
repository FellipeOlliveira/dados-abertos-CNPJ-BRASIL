import os
import zipfile
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from datetime import datetime

class Transformer:
    # Diretório onde estão os arquivos ZIP
    pasta_zip = r'C:\Users\est_mfoliveira\PycharmProjects\dados abertos\dados_abertos\dados_abertos_request\extracted'

    # Terminologias para a extração dos arquivos
    terminologias = {
        'EMPRECSV': 'empresas',
        'CNAECSV': 'cnae',
        'ESTABELE': 'estabelecimentos',
        'MOTICSV': 'motivos',
        'MUNICCSV': 'municipios',
        'NATJUCSV': 'natureza_juridica',
        'PAISCSV': 'pais',
        'QUALSCSV': 'qualificacoes',
        'SIMPLES': 'simples',
        'SOCIOCSV': 'socios'
    }

    def __init__(self, type=None):
        """
        Inicializa a classe Transformer.
        :param type: Tipo de saída ('csv' ou 'parquet').
        """
        if type == 'csv':
            self.type = r'C:\Users\est_mfoliveira\PycharmProjects\dados abertos\dados_abertos\dados_abertos_request\RF_csv'
        elif type == 'parquet':
            self.type = r'C:\Users\est_mfoliveira\PycharmProjects\dados abertos\dados_abertos\dados_abertos_request\RF_parquet'
        else:
            raise ValueError("Tipo de saída inválido. Use 'csv' ou 'parquet'.")

        # Configura o sistema de logs
        self._setup_logging()

        self.create_folders()

        self.process_zip_files()
    def _setup_logging(self):
        """
        Configura o sistema de logs para registrar em arquivo e no console.
        """
        # Cria a pasta de logs, se não existir
        log_folder = os.path.join(os.path.dirname(self.type), 'logs')
        if not os.path.exists(log_folder):
            os.makedirs(log_folder)

        # Define o nome do arquivo de log com base na data e hora
        log_file = os.path.join(log_folder, f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

        # Configura o logging
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)  # Define o nível de log

        # Formato das mensagens de log
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        # Handler para arquivo de log
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Handler para console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def log(self, message: str, level: str = 'info'):
        """
        Registra uma mensagem no log e no console.
        :param message: Mensagem a ser registrada.
        :param level: Nível do log ('info', 'warning', 'error').
        """
        if level == 'info':
            self.logger.info(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.logger.error(message)
        else:
            raise ValueError("Nível de log inválido. Use 'info', 'warning' ou 'error'.")

    def optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Otimiza os tipos de dados do DataFrame para melhor compressão.
        :param df: DataFrame a ser otimizado.
        :return: DataFrame com tipos de dados otimizados.
        """
        for column in df.columns:
            # Get column data
            col_data = df[column]

            # Handle numeric columns
            if pd.api.types.is_numeric_dtype(col_data):
                # Integer optimization
                if col_data.dtype in ['int64', 'int32']:
                    max_val = col_data.max()
                    min_val = col_data.min()

                    if min_val >= 0:  # Unsigned integers
                        if max_val <= 255:
                            df[column] = col_data.astype(np.uint8)
                        elif max_val <= 65535:
                            df[column] = col_data.astype(np.uint16)
                        elif max_val <= 4294967295:
                            df[column] = col_data.astype(np.uint32)
                    else:  # Signed integers
                        if min_val >= -128 and max_val <= 127:
                            df[column] = col_data.astype(np.int8)
                        elif min_val >= -32768 and max_val <= 32767:
                            df[column] = col_data.astype(np.int16)
                        elif min_val >= -2147483648 and max_val <= 2147483647:
                            df[column] = col_data.astype(np.int32)

                # Float optimization
                elif col_data.dtype in ['float64']:
                    df[column] = col_data.astype(np.float32)

            # Handle string columns
            elif pd.api.types.is_string_dtype(col_data):
                if col_data.nunique() / len(col_data) < 0.4:  # If less than 40% unique values
                    df[column] = col_data.astype('category')

        return df

    def create_folders(self):
        """
        Cria as pastas de destino, se não existirem.
        """
        for pasta in self.terminologias.values():
            caminho_pasta = os.path.join(self.type, pasta)
            if not os.path.exists(caminho_pasta):
                os.makedirs(caminho_pasta)
                self.log(f"Pasta criada: {caminho_pasta}")

    def process_zip_files(self):
        """
        Processa todos os arquivos ZIP na pasta e salva os arquivos CSV ou Parquet.
        """
        self.log("Iniciando processamento dos arquivos ZIP.")
        for arquivo_zip in os.listdir(self.pasta_zip):
            if not arquivo_zip.endswith('.zip'):
                continue  # Ignora arquivos que não são ZIP

            caminho_zip = os.path.join(self.pasta_zip, arquivo_zip)

            # Verifica se o arquivo é um ZIP válido
            if not zipfile.is_zipfile(caminho_zip):
                self.log(f"Arquivo não é um ZIP válido: {caminho_zip}", level='warning')
                continue

            # Abre o arquivo ZIP
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                nome_tabela = os.path.basename(caminho_zip).replace(".zip", "")
                self.log(f"Processando arquivo ZIP: {nome_tabela}")

                # Itera sobre os arquivos dentro do ZIP
                for nome_arquivo in zip_ref.namelist():
                    # Verifica a terminação do arquivo
                    for termo, pasta in self.terminologias.items():
                        if termo in nome_arquivo:
                            # Define o caminho de destino
                            nome_saida = f"{nome_tabela}.{self.type.split('_')[-1]}"  # Nome único para o arquivo final
                            caminho_destino = os.path.join(self.type, pasta, nome_saida)

                            # Abre o arquivo dentro do ZIP e processa em chunks
                            with zip_ref.open(nome_arquivo) as arquivo:
                                # Configura o leitor de CSV em chunks
                                chunk_size = 10000  # Número de linhas por chunk (ajuste conforme necessário)
                                reader = pd.read_csv(arquivo, encoding='ISO-8859-1', delimiter=';', chunksize=chunk_size)

                                # Processa e salva cada chunk
                                primeiro_chunk = True
                                for chunk in reader:
                                    # Otimiza os tipos de dados do chunk
                                    chunk = self.optimize_dtypes(chunk)

                                    # Salva o chunk no arquivo de saída
                                    if self.type.endswith('csv'):
                                        self._save_csv(chunk, caminho_destino, primeiro_chunk)
                                    elif self.type.endswith('parquet'):
                                        self._save_parquet(chunk, caminho_destino, primeiro_chunk)

                                    primeiro_chunk = False

                                self.log(f"Arquivo {nome_saida} processado e salvo em {caminho_destino}")
                            break  # Sai do loop após encontrar a terminação correta

        self.log("Processo concluído!")

    def _save_csv(self, chunk: pd.DataFrame, caminho_destino: str, primeiro_chunk: bool):
        """
        Salva um chunk em um arquivo CSV.
        :param chunk: DataFrame a ser salvo.
        :param caminho_destino: Caminho do arquivo de destino.
        :param primeiro_chunk: Indica se é o primeiro chunk.
        """
        if primeiro_chunk:
            # Salva o primeiro chunk com cabeçalho
            chunk.to_csv(caminho_destino, index=False, mode='w', encoding='ISO-8859-1', sep=';')
        else:
            # Salva os chunks seguintes sem cabeçalho
            chunk.to_csv(caminho_destino, index=False, mode='a', encoding='ISO-8859-1', sep=';', header=False)

    def _save_parquet(self, chunk: pd.DataFrame, caminho_destino: str, primeiro_chunk: bool):
        """
        Salva um chunk em um arquivo Parquet.
        :param chunk: DataFrame a ser salvo.
        :param caminho_destino: Caminho do arquivo de destino.
        :param primeiro_chunk: Indica se é o primeiro chunk.
        """
        if primeiro_chunk:
            # Salva o primeiro chunk como um novo arquivo Parquet
            chunk.to_parquet(caminho_destino, index=False)
        else:
            # Anexa os chunks seguintes ao arquivo Parquet existente
            tabela_chunk = pa.Table.from_pandas(chunk)
            with pq.ParquetWriter(caminho_destino, tabela_chunk.schema) as writer:
                writer.write_table(tabela_chunk)


# Exemplo de uso
if __name__ == "__main__":
    Transformer(type='csv')