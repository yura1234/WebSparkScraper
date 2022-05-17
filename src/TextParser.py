# -*- coding: utf-8 -*-
from re import split, compile, escape
from tokenizer import Tokenizer
import nltk
from nltk.tokenize import sent_tokenize

class TextParser():
    """
    Класс обработки и парсинга текстовой последовательности
    """
    @staticmethod
    def __splitText(text):
        """
        Функция разбиения текста на слова по пробельным символам

        :param [str] text: входной текст

        :return: список слов, составляющих входной текст

        """
        return split(r"\s+", text)

    @staticmethod
    def clearText(text):
        """
        Функция очистки текста от определенного набора символов

        :param [str] text: входной текст

        :return: входной текст, очищенный от определенного количества
        символов

        """
        # Из входного текста будут удалены все символы, кроме букв русского
        # и английского алфавитов, цифр, дефиса и пробельных символов
        regex = compile(r"[^a-zA-Z1-9,.!?А-Яа-я\s-]")
        txt = regex.sub("", text)

        # for t in range(0, len(txt)):
        #     txt[t] = txt[t].replace('\n', ' ')

        txt = txt.replace('\n', ' ')
        # return regex.sub("", text)
        return txt

    @staticmethod
    def makeVecWords(text):
        text = TextParser.clearText(text)
        vec = list(filter(None, [v.strip() for v in text.split(' ')]))

        return vec


    @staticmethod
    def getClearText(text):
        nltk.data.path.append('/mnt/tmpfs/nltk_data/')
        txt = TextParser.clearText(text)

        sentences = sent_tokenize(txt.strip(), 'russian')
        sentences = [x.replace('\n', '') for x in sentences]

        return sentences

    @staticmethod
    def getParsedWordsList(text):
        """
        Функция очистки текста и разбиения его на слова

        :param [str] text: входной текст

        :return: список слов, составляющих очищенный входной текст

        """
        return TextParser.__splitText(
                                     TextParser.__clearText(text)
                                     )

