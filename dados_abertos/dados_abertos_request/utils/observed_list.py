import zipfile
import pandas as pd

class ObservableList:
  def __init__(self):
    self.items = []

  def append(self, item):
    self.items.append(item)
    self.process_item()

  def process_item(self):
    if self.items:
      item = self.items.pop(0)  # Remove o primeiro item da lista
      self.print_item(item)

  def print_item(self, item):
    csv_file = item.replace(".zip",'.csv')
    with zipfile.ZipFile(item, 'r') as zip_ref:
      arquivos_no_zip = zip_ref.namelist()

      for arquivo in arquivos_no_zip:
        if arquivo.endswith('.csv'):
          print(f"Processando arquivo CSV: {arquivo}")
          with zip_ref.open(arquivo) as arquivo_csv:
            df = pd.read_csv(arquivo_csv)
            caminho_novo_csv = arquivo.replace(".csv", "_salvo.csv")
            df.to_csv(caminho_novo_csv, index=False)
            print(f"Arquivo CSV salvo em: {caminho_novo_csv}")

if __name__ == '__main__':

  # Exemplo de uso
  observable_list = ObservableList()

  # Adicionando itens à lista
  observable_list.append("Item 1")
  observable_list.append("Item 2")
  observable_list.append("Item 3")