# -*- coding: utf-8 -*-
import codecs


class partBert():

    def __init__(self):
        pass

    def load_vocabulary(vocab_path):
        """
        Функция формирования словаря на основе файла
        
        :vocab_path: путь к словарю
        :return: словарь {токен: длина токена}
        """
        token_dict = {}
        with codecs.open(vocab_path, 'r', 'utf8') as reader:
            for line in reader:
                token = line.strip()
                token_dict[token] = len(token_dict)
        return token_dict