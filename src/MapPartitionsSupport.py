# -*- coding: utf-8 -*-
from datetime import datetime
import pytz

class MapPartitionsSupport():

    """
    Класс, обеспечивающий преобразование RDD посредством MapPartitions
    """
    @staticmethod
    def __mapPartitions_Func(pos_accumulator,
                             neg_accumulator,
                             in_class,
                             params):
        """
        Функция, реализующая преобразование данных в RDD.MapPartitions() и
        сбор статистики посредством аккумуляторов

        :params pos_accumulator: аккумулятор, подсчитывающий количество
        успешных выполнений функции in_class.getData(...)
        :type pos_accumulator: pyspark.SparkContext.accumulator
        :params neg_accumulator: аккумулятор, подсчитывающий количество
        отказов при выполнении функции in_class.getData(...)
        :type neg_accumulator: pyspark.SparkContext.accumulator
        :param in_class: класс, реализующий интерфейс Interface()
        :param params: кортеж из параметров инициализатора объекта
        класса in_class

        :return: функция для передачи в метод RDD.MapPartitions()

        """
        def _map_partitions_func(data):


            output_vector = []
            with in_class(params) as p:
                for item in data:
                    result_data = p.getData(item)
                    if result_data:
                        output_vector.extend(result_data)
                        pos_accumulator.add(1)
                    else:
                        neg_accumulator.add(1)

            return output_vector
        return _map_partitions_func

    @staticmethod
    def mapPartitions_RDD(pos_accumulator,
                          neg_accumulator,
                          in_rdd,
                          in_class,
                          *params):
        """
        Функция, реализующая трансформацию RDD.MapPartitions() и сбор
        статистики посредством аккумуляторов

        :params pos_accumulator: аккумулятор, подсчитывающий количество
        успешных выполнений функции in_class.getData(...)
        :type pos_accumulator: pyspark.SparkContext.accumulator
        :params neg_accumulator: аккумулятор, подсчитывающий количество
        отказов при выполнении функции in_class.getData(...)
        :type neg_accumulator: pyspark.SparkContext.accumulator
        :param in_rdd: RDD, над которым выполняется трансформация
        MapPartitions()
        :type in_rdd: pyspark.RDD
        :param in_class: класс, реализующий интерфейс Interface()
        :param params: список параметров инициализатора объекта класса in_class

        :return: новый RDD, полученный после преобразования
        in_rdd.MapPartitions()

        """
        map_func = MapPartitionsSupport.__mapPartitions_Func(pos_accumulator,
                                                             neg_accumulator,
                                                             in_class,
                                                             params)
        result = in_rdd.mapPartitions(map_func)
        return result
