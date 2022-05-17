# -*- coding: utf-8 -*-
from TextParser import TextParser
from loader import partBert as pb
from tokenizer import Tokenizer

class Vocabular:

    def __init__(self, path):
        self.voc = pb.load_vocabulary(path)
        self.cleaner = TextParser

    def getVectorFromText(self, text):
        """
        Функция токенезирования текста

        :param text: исходный текст
        :return: токены входного текста
        """
        tokenizer = Tokenizer(self.voc)
        indices, _ = tokenizer.encode(self.cleaner.clearText(text))

        return indices

