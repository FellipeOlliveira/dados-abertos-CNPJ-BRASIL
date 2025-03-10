import datetime
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import fake_useragent
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

base_url = 'https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/'

session = requests.session()
ua = fake_useragent.UserAgent()

headers = {
  'User-Agent': ua.random
}

response = session.get(base_url,headers=headers)

_ano_link = None

if response.status_code == 200:
  soup = BeautifulSoup(response.text, 'html.parser')
  links = soup.find_all('a', href=True)

  for link in links:
    if str(datetime.date.year) in link['href']:
      _ano_link = link['href']


url = urljoin(base_url, _ano_link)
### NICE ###

response = session.get(url)

links_zip = []
if response.status_code == 200:
  soup = BeautifulSoup(response.text, 'html.parser')
  links = soup.find_all('a', href=True)

  links_zip = [link['href'] for link in links if link['href'].endswith('.zip')]

  for file_name in links_zip:
    # Constrói a URL completa do arquivo
    url_file = urljoin(url, file_name)


def download_file(file_name):
  url_file = urljoin(url, file_name)

  try:
    # Usa uma sessão para reutilizar conexões
    with requests.Session() as session:

      response = session.get(url_file,headers=headers, stream=True)
      response.raise_for_status()

      with open(r'extracted/'+file_name, 'wb') as file:
        for chunk in response.iter_content(chunk_size=131072):  # 128KB por chunk
          if chunk:
            file.write(chunk)
      return f"Download de {file_name} concluído!"
  except requests.exceptions.RequestException as e:
    return f"Erro ao baixar {file_name}: {e}"

with ThreadPoolExecutor(max_workers=6) as executor:  # Ajuste o número de workers
  futures = [executor.submit(download_file, file_name) for file_name in links_zip]

  for future in as_completed(futures):
    print(future.result())