# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod


class Interface():
    """
    Класс представляет собой интерфейс для классов-наследников, реализующих
    функцию обработки данных, используемую преобразованием RDD.MapPartitions
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def getData(self, params):
        """
        Интерфейс функции преобразования данных
        """
        pass

    @abstractmethod
    def __exit__(self, exception_type, exception_value, traceback):
        pass
