# -*- coding: utf-8 -*-
from pyspark import SparkConf
from pyspark import SparkContext
from MapPartitionsSupport import MapPartitionsSupport
from TextParser import TextParser
from WordToVector import WordToVector
from MQClient import MQClient
from datetime import datetime
from time import sleep
import time as pytime
from operator import add
from tldextract import extract
from numpy import linalg
import numpy as np
import keras
from keras_bert import load_vocabulary, load_trained_model_from_checkpoint, Tokenizer, get_checkpoint_paths
from keras_bert.layers import MaskedGlobalMaxPool1D
from loader import partBert as pb
from voc import Vocabular
import pytz
from WebScraper2 import WebScraper2
import csv
import pickle
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.random import RandomRDDs
from numpy import divide
from keras_bert import extract_embeddings
from collections import OrderedDict
from operator import itemgetter
from datetime import timedelta
from gap import Gap

class RecurrentWebScraper():
    """
    Класс, реализующий периодический скрепинг веб-сайтов
    """
    def __init__(self,
                 chrome_path="/usr/bin/chromedriver",
                 load_timeout=60,
                 bert_model_path="/mnt/tmpfs/rubert_cased_L-12_H-768_A-12_v2",
                 urls_textfile_path="urls.txt",
                 nesting_depth=1,
                 is_nesting_within_domain=True,
                 timeframe_minutes=15,
                 num_partitions=64,
                 mq_hostname="node39.cluster",
                 mq_port=5672,
                 mq_queue="scraping",
                 mq_username="guest",
                 mq_password="guest",
                 num_refs=50,
                 num_take=10,
                 path_to_voc='/home/kachanov/BERT/vocab.txt'):
        """
        Конструктор класса

        :param [str] chrome_path: расположение драйвера браузера Google Chrome
        :param [int] load_timeout: время в секундах, в течение которого
        браузер будет ожидать загрузку страницы
        :param [str] bert_model_path: расположение файлов обученной модели BERT
        :param [str] urls_textfile_path: расположение в HDFS текстового файла,
        содержащего список веб-сайтов для скрепинга
        :param [int] nesting_depth: глубина обработки ссылок при скрепинге
        :param [bool] is_nesting_within_domain: обрабатывать только те
        вложенные ссылки, которые принадлежат к тому же домену, что и
        родительские
        :param [int] timeframe_minutes: период времени в минутах, через
        который выполняется скрепинг сайтов
        :param [int] num_partitions: количество разделов данных. Используется
        для задания степени параллелизма скрепера
        :param [str] mq_hostname: доменное имя или IP-адрес сервера сообщений
        :param [int] mq_port: порт сервера сообщений
        :param [str] mq_queue: название очереди, в которую отправляются
        сообщения
        :param [str] mq_username: имя пользователя сервера сообщений
        :param [str] mq_password: пароль пользователя сервера сообщений
        :param [int] num_refs: количество итераций для функции GAP статистики
        :param [int] num_take: количество предложений отбираемых из самого большого кластера
        :param [str] path_to_voc: путь к словарю слов/частей слов

        """
        self.__chrome_path = chrome_path
        self.__load_timeout = load_timeout
        self.__bert_model_path = bert_model_path
        self.__urls_textfile_path = urls_textfile_path
        self.__nesting_depth = nesting_depth
        self.__is_nesting_within_domain = is_nesting_within_domain
        self.__timeframe_seconds = timeframe_minutes * 60
        self.__num_partitions = num_partitions
        self.__mq_hostname = mq_hostname
        self.__mq_port = mq_port
        self.__mq_queue = mq_queue
        self.__mq_username = mq_username
        self.__mq_password = mq_password
        self.__num_refs = int(num_refs)
        self.__num_take = int(num_take)
        self.__path_to_voc = path_to_voc

        self.__configuration = SparkConf()
        self.__configuration.setAppName('spark-scraper')
        self.__spark_context = SparkContext(conf=self.__configuration)

    def __getVectorAndLinks(self, pos_accumulator, neg_accumulator, urls, path):
        web_content = MapPartitionsSupport.mapPartitions_RDD(
            pos_accumulator,
            neg_accumulator,
            urls,
            WebScraper2,
            self.__chrome_path,
            self.__load_timeout
        )
        web_content = web_content.persist()

        urls = web_content.filter(
            lambda line: ("link" in line[0]) and line[1][0]
        ).map(
            lambda line: line[1]
        ).distinct().subtractByKey(urls)

        vocab = Vocabular(path)

        filtered_content = web_content.filter(lambda line: line[0].startswith('http') and len(line[1]) > 0).persist()

        # Отфильтровываем записи RDD с содержимим сайтов длиной больше одного
        # и получаем вектор из слов текста
        vec_words = filtered_content.mapValues(vocab.cleaner.makeVecWords)
        # формируем вектор из всего текста 
        vec_txt = filtered_content.mapValues(vocab.getVectorFromText)
        # формируем только текст
        txt = filtered_content.mapValues(vocab.cleaner.getClearText)

        return urls, vec_txt, vec_words, txt

    @staticmethod
    def isDomainsEqual(params):
        """
        Функция сравнения доменного имени текущего и родительского веб-адресов

        :param params:  tuple(current_url, parent_url), где current_url -
        строка адреса текущей веб-страницы, parent_url - адрес родительской
        веб-страницы

        :return: совпадают ли домены текущего и родительского адресов

        """
        current_url = params[0]
        parent_url = params[1]

        _, cur_td, cur_tsu = extract(current_url)
        _, par_td, par_tsu = extract(parent_url)

        if (cur_td, cur_tsu) == (par_td, par_tsu):
            return True
        else:
            return False

    def __clearUrlsByDomain(self, urls):
        """
        Функция фильтрации RDD по критерию единства доменных имен
        родительского и текущего веб-адресов

        :param urls: входной RDD
        :type urls: pyspark.RDD

        :return: отфильтрованный входной RDD

        """
        if self.__is_nesting_within_domain:
            return urls.filter(RecurrentWebScraper.isDomainsEqual)
        else:
            return urls

    def __loopWebScrapingAndGetVector(self, urls, path):
        """
        Функция скрепинга сайтов и получения результирующего вектора -
        интегральной зарактеристики слов всех сайтов после скрепинга

        :param urls: RDD, содержащий веб-адреса сайтов для скрепинга
        :type urls: pyspark.RDD
        :param words: RDD(words, counts), содержащий список слов до
        скрепинга и количество повторений каждого из них
        :type words: pyspark.RDD

        :return: tuple(norm_vector, number_of_links, bad_sites_percent,
        bad_words_percent), где norm_vector - нормированный вектор
        результата, number_of_links - число ссылок на сайтах, полученных
        в результате скрепинга, bad_sites_percent - процент сайтов,
        скрепинг которых прервался с ошибкой, bad_words_percent -
        процент слов, не найденных в словаре word embeddings

        """
        pos_sites_accumulator = self.__spark_context.accumulator(0)
        neg_sites_accumulator = self.__spark_context.accumulator(0)


        acc = self.__spark_context.emptyRDD().map(lambda x: (x,x))
        acc_words = self.__spark_context.emptyRDD().map(lambda x: (x,x))
        acc_txt = self.__spark_context.emptyRDD().map(lambda x: (x,x))


        for _ in range(self.__nesting_depth):
            urls, vec, words, txt = self.__getVectorAndLinks(pos_sites_accumulator,
                                                 neg_sites_accumulator,
                                                 urls,
                                                 path)
            urls = self.__clearUrlsByDomain(urls)

            acc = acc + vec
            acc_words = acc_words + words
            acc_txt = acc_txt + txt

        return acc, acc_words, acc_txt

    def __pauseToTime(self, until_datetime_seconds):
        """
        Функция временной задержки

        :param [int] until_datetime_seconds: время в секундах с начала
        эпохи, до которого будет приостановлено выполнение программы

        """
        # Ожидаем в цикле начала нового периода скрепинга
        while True:
            time_diff = until_datetime_seconds - datetime.now().timestamp()
            if time_diff < 0:
                break
            sleep(time_diff / 2)

    def __pauseToTime_(self, time):
        end = time

        if isinstance(time, datetime):
            zoneDiff = pytime.time() - (datetime.now()- datetime(1970, 1, 1)).total_seconds()
            end = (time - datetime(1970, 1, 1)).total_seconds() + zoneDiff

        while True:
            diff = end - pytime.time()
            if diff <= 0:
                break
            else:
                sleep(diff / 2)

    def __getSleepTime(self):
        now = datetime.now()

        if now.minute < 30: 
            dif = 30 - now.minute
        else:
            dif = 60 - now.minute

        return (now + timedelta(minutes=dif)).replace(second = 0, microsecond = 0)

    def __get_now_formatted(self):
        """
		Функция, которая формирует время по часовому поясу Moscow

        :return: текущее время в часовом поясе Moscow
        """
        tz = pytz.timezone("Europe/Moscow")
        now = datetime.now(tz)
        return now

    def run(self):
        """
        Функция основного цикла программы
        """
        urls = self.__spark_context.textFile(
            self.__urls_textfile_path,
            self.__num_partitions
        )

        # Формируем ключ RDD, состоящий из текущего адреса
        urls = urls.map(lambda x: (x, x))
        
        sleep_time = self.__getSleepTime()

        # Бесконечный цикл периодического скрепинга веб-сайтов
        while True:
            # Время начала очередного временного периода скрепинга в секундах
            # next_timeframe_seconds = (
            #     (self.__get_now_formatted().timestamp() // self.__timeframe_seconds) + 1
            # ) * self.__timeframe_seconds

            # Дата и время начала очередного временного периода скрепинга
            # next_timeframe = datetime.fromtimestamp(sleep_time)
            string_time = sleep_time.strftime("%d-%m-%y_%H-%M-%S_GMT+0300")
            part_path = '/user/kachanov/'

            print(sleep_time.strftime("Ожидаем время: %H:%M:%S ..."))
            # Ожидаем начала следующего временного периода скрепинга
            # self.__pauseToTime(next_timeframe_seconds)
            self.__pauseToTime_(sleep_time)
            sleep_time = sleep_time + timedelta(minutes=self.__timeframe_seconds / 60)

            vectors, words, txt = self.__loopWebScrapingAndGetVector(urls, self.__path_to_voc)

            # vectors.saveAsTextFile(part_path + 'bert_vec_a/' + string_time)
            # words.saveAsTextFile(part_path + 'bert_words_a/' + string_time)
            # txt.saveAsTextFile(part_path + 'bert_txt_a/' + string_time)

            _out = []
            K_max = 30
            find = False
            k_model = None
            path_model = self.__bert_model_path

            def makeTextVec(text):
                """
                Функция формирует векторы из текста, далее векторы суммируются и нормализуются
                :param text: текст
                :return: список tuple(предложение, вектор)
                """
                text = list(text)
                vec = extract_embeddings(path_model, text)
                sum_vec = [np.sum(v, axis=0) for v in vec]
                norm_vec = [divide(v, linalg.norm(v)) for v in sum_vec]

                return list(zip(text, norm_vec))

            all_sentences = txt.map(lambda x: x[1]).flatMap(lambda x: x)

            textVec = all_sentences.mapPartitions(makeTextVec).cache()
            vec_rdd = textVec.map(lambda x: x[1])

            # if not vec_rdd.isEmpty():
            for K in range(1, K_max + 1):
                statistics, model = Gap.gap_func(self.__spark_context, vec_rdd, K, self.__num_refs, (vec_rdd.count(), 768))
                _out.append((K, statistics))

                if len(_out) > 1:
                    for i in range(0, len(_out) - 1):
                        if (_out[i][1]['gap_value'] - _out[i + 1][1]['gap_value'] - _out[i + 1][1]['sk']) > 0 or \
                                (_out[i][1]['gap_star'] - _out[i + 1][1]['gap_star'] - _out[i + 1][1]['sk_star']) > 0:
                            find = True
                            break

                if find:
                    k_model = model
                    break

            if find:
                centers = k_model.clusterCenters

                # формирование рдд (номер кластера, (точка принадлежащая данному кластеру, расстояние до центра кластера))
                labelsDist = textVec.map(lambda p: (k_model.predict(p[1]), (p[0], Gap.disp(p[1], k_model, centers))))
                # получение номера кластера содержащего наибольшее количество точек
                max_num_cluster = list(OrderedDict(sorted(labelsDist.countByKey().items(), key = itemgetter(1), reverse = True)).keys())[0]
                # фильтруем только точки принадлежащие максимальному кластеру
                maxLabelsDist = labelsDist.filter(lambda x: x[0] == max_num_cluster)
                # отбираем только __num_take кол-во точек из максимального кластера
                orderedLabels = maxLabelsDist.takeOrdered(self.__num_take, key=lambda x: x[1][1])
                sent_array = [s[1][0] for s in orderedLabels]

                print("Out Sentences:\n")
                [print(s) for s in sent_array]

                vec = extract_embeddings(path_model, sent_array)
                sum_vec = [np.sum(v, axis=0) for v in vec]
                norm_vec = [divide(v, linalg.norm(v)) for v in sum_vec]
                timeframe_sum_vec = np.sum(norm_vec, axis=0)

                # print('TOTAL TIME FOR N_REFS = ' + str(self.__num_refs) + ' IS = ' +\
                #  str(timedelta(seconds=(datetime.now() - start).total_seconds())) + '(HH:MM:SS.ms)')


                # Отправляем результаты скрепинга на сервер сообщений
                with MQClient(self.__mq_hostname,
                              self.__mq_port,
                              self.__mq_queue,
                              self.__mq_username,
                              self.__mq_password) as client:
                    client.send(
                        { 
                          'time' : string_time,
                          'vector' : timeframe_sum_vec
                        }
                    )
                    
                vectors.saveAsTextFile(part_path + 'bert_vec_a/' + string_time)
                words.saveAsTextFile(part_path + 'bert_words_a/' + string_time)
                txt.saveAsTextFile(part_path + 'bert_txt_a/' + string_time)

            if sleep_time < datetime.now():
                sleep_time = self.__getSleepTime()

        return 0
