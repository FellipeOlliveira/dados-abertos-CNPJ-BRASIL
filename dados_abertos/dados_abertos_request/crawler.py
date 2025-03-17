import datetime
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import fake_useragent
import os
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import zipfile
import pandas as pd
import logging

# Configuração básica do logging
logging.basicConfig(
    level=logging.INFO,  # Define o nível mínimo de log
    format='%(asctime)s - %(levelname)s - %(message)s',  # Formato da mensagem de log
    handlers=[
        logging.FileHandler('extractor.log'),  # Salva os logs em um arquivo
        logging.StreamHandler()  # Exibe os logs no console
    ]
)

class Extractor:
    current_year = str(datetime.date.today().year)
    base_url = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/'
    session = requests.session()
    ua = fake_useragent.UserAgent()
    headers = {
        'User-Agent': ua.random
    }

    def __init__(self):
        logging.info("Iniciando o Extractor...")
        self.data_url = urljoin(self.base_url, self.extract_year_page_url())
        logging.info(f"URL base do ano atual: {self.data_url}")

        self.downloading_links = self.extract_zip_file_url()
        logging.info(f"Links para download encontrados: {self.downloading_links}")

        self.download_data()

    def extract_year_page_url(self):
        logging.info("Extraindo URL da página do ano atual...")
        response = self.session.get(self.base_url, headers=self.headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.find_all('a', href=True)

            for link in links:
                if self.current_year in link['href']:
                    logging.info(f"URL do ano atual encontrada: {link['href']}")
                    return link['href']
        else:
            logging.error(f"Erro ao acessar a URL base: {response.status_code}")

    def extract_zip_file_url(self):
        logging.info("Extraindo links dos arquivos ZIP...")
        response = self.session.get(self.data_url)
        links_zip = []
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.find_all('a', href=True)

            links_names = [link['href'] for link in links if link['href'].endswith('.zip')]
            logging.info(f"{len(links_names)} arquivos ZIP encontrados.")

            for link_name in links_names:
                links_zip.append(urljoin(self.data_url, link_name))

            return links_zip
        else:
            logging.error(f"Erro ao acessar a URL dos dados: {response.status_code}")

    def _downloading_file(self, file_url):
        file_name = os.path.basename(file_url)
        logging.info(f"Iniciando download do arquivo: {file_name}")
        try:
            with requests.Session() as session:
                response = session.get(file_url, headers=self.headers, stream=True)
                response.raise_for_status()

                with open(r'extracted/' + file_name, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=131072):  # 128KB por chunk
                        if chunk:
                            file.write(chunk)
                logging.info(f"{file_name} baixado com sucesso")
                return file.name
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao baixar {file_url}: {e}")
            return f"Erro ao baixar {file_url}: {e}"

    def download_data(self):
        logging.info("Iniciando o processo de download dos arquivos...")
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [executor.submit(self._downloading_file, link_for_download) for link_for_download in self.downloading_links]

            for future in as_completed(futures):
                logging.info(future.result())
        logging.info("Download dos arquivos concluído.")

if __name__ == '__main__':
    Extractor()