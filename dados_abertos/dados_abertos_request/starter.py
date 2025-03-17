from crawler import Extractor
from transformer import Transformer

class Start_Project:
  def __init__(self):
    Extractor()
    Transformer(type='csv')


if __name__ == '__main__':
    Start_Project()