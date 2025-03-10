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

from utils.observed_list import ObservableList

class Extractor:
  current_year = str(datetime.date.today().year)
  base_url = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/'
  session = requests.session()
  ua = fake_useragent.UserAgent()
  observed_list = ObservableList()
  headers = {
    'User-Agent': ua.random
  }

  def __init__(self):
    self.data_url = urljoin(self.base_url, self.extract_year_page_url())

    self.downloading_links = self.extract_zip_file_url()
    print(self.downloading_links)

    self.download_data()

  def extract_year_page_url(self):

    response = self.session.get(self.base_url,headers=self.headers)

    if response.status_code == 200:
      soup = BeautifulSoup(response.text, 'html.parser')
      links = soup.find_all('a', href=True)

      for link in links:
        if self.current_year in link['href']:
          return link['href']

  def extract_zip_file_url(self):
    response = self.session.get(self.data_url)
    links_zip = []
    if response.status_code == 200:
      soup = BeautifulSoup(response.text, 'html.parser')
      links = soup.find_all('a', href=True)

      links_names = [link['href'] for link in links if link['href'].endswith('.zip')]

      for link_name in links_names:
        # Constrói a URL completa do arquivo
        links_zip.append(urljoin(self.data_url, link_name))

      return links_zip

  def _downloading_file(self,file_url):
    file_name = os.path.basename(file_url)
    try:
      with requests.Session() as session:

        response = session.get(file_url, headers=self.headers, stream=True)
        response.raise_for_status()

        with open(r'extracted/' + file_name, 'wb') as file:
          for chunk in response.iter_content(chunk_size=131072):  # 128KB por chunk
            if chunk:
              file.write(chunk)
        print(f"{file_name} baixado com sucesso")
        return file.name
    except requests.exceptions.RequestException as e:
      return f"Erro ao baixar {file_url}: {e}"

  def download_data(self):
    print('Começou')
    with ThreadPoolExecutor(max_workers=6) as executor:  # Ajuste o número downloads executados em paralelos (OBS: n colocar mais que 6)
      futures = [executor.submit(self._downloading_file, link_for_download) for link_for_download in self.downloading_links]

      for future in as_completed(futures):
        print(future.result())
      #   with zipfile.ZipFile.open(future.result(),mode='r') as zip_ref:#o nome zip do arquivo
      #     arquivos_no_zip = zip_ref.namelist()
      #
      #     for arquivo in arquivos_no_zip:
      #       if arquivo.endswith('.csv'):
      #         print(f"Processando arquivo CSV: {arquivo}")
      #         with zip_ref.open(arquivo) as arquivo_csv:
      #           df = pd.read_csv(arquivo_csv)
      #           df.to_csv(future.result().replace(".zip",'.csv'), index=False)
      #           print(f"Arquivo CSV salvo em: {future.result().replace(".zip",'.csv')}")

if __name__ == '__main__':
  crawler = Extractor()

