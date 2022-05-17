# -*- coding: utf-8 -*-
from pickle import dumps
from pika import PlainCredentials
from pika import ConnectionParameters
from pika import BlockingConnection


class MQClient:
    """
    Класс, реализующий клиентское взаимодействие с сервером сообщений
    """
    def __init__(self, hostname, port, queue, username, password):
        """
        Конструктор класса

        :param [str] hostname: Доменное имя или IP-адрес сервера сообщений
        :param [int] port: Порт сервера сообщений
        :param [str] queue: Название очереди, в которую будут отправляться
        сообщения
        :param [str] username: Имя пользователя сервера сообщений
        :param [str] password: Пароль пользователя сервера сообщений

        """
        self.__credentials = PlainCredentials(username, password)
        self.__parameters = ConnectionParameters(hostname,
                                                 port,
                                                 r"/",
                                                 self.__credentials)
        self.__queue_name = queue

    def __enter__(self):
        self.__connection = BlockingConnection(self.__parameters)
        self.__channel = self.__connection.channel()
        self.__channel.queue_declare(queue=self.__queue_name)
        return self

    def send(self, result):
        """ Функция отправки данных на сервер сообщений

        :param [bytes] result: отправляемые на сервер сообщений данные

        """
        self.__channel.basic_publish(exchange='',
                                     routing_key=self.__queue_name,
                                     body=dumps(result))

    def __exit__(self, exception_type, exception_value, traceback):
        self.__connection.close()
