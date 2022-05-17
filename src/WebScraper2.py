# -*- coding: utf-8 -*-
from Interface import Interface
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from datetime import datetime
import pytz

# from pyvirtualdisplay import Display
# from selenium.webdriver.firefox.options import Options as fOptions
#https://peter.sh/experiments/chromium-command-line-switches/
class WebScraper2(Interface):
    """
    Класс, обеспечивающий загрузку и скрепинг веб-страниц
    """
    def __init__(self, params):
        """
        Конструктор класса

        :param params: tuple(chrome_path, load_timeout), где chrome_path -
        расположение драйвера браузера Google Chrome, load_timeout - время
        в секундах, в течение которого браузер будет ожидать загрузку страницы

        """

        self.__chrome_path = params[0]
        self.__load_timeout = params[1]
        self.__chrome_option = webdriver.ChromeOptions()

        # Отключаем загрузку изображений веб-браузером
        self.__prefs = {"profile.managed_default_content_settings.images": 2}
        self.__chrome_option.add_experimental_option("prefs", self.__prefs)

        # Отключаем графический интерфейс браузера и его плагины
        self.__chrome_option.add_argument("--no-sandbox")
        self.__chrome_option.add_argument("--headless")
        self.__chrome_option.add_argument("--disable-extensions")

        # Веб-адрес по умолчанию отсутствует
        self.__current_url = ""


    def __enter__(self):
        self.__driver = webdriver.Chrome(
            executable_path=self.__chrome_path,
            chrome_options=self.__chrome_option
        )
        # self.__driver.set_page_load_timeout(25)
        self.__driver.set_page_load_timeout(self.__load_timeout)
        return self

    def __openURL(self, url):
        """
        Функция загрузки веб-страницы браузером

        :param [str] url: веб-адрес страницы

        """
        # Если веб-страница уже загружена, повторно ее загружать не нужно
        if self.__current_url != url:
            self.__driver.get(url)
            self.__current_url = url

    def __get_now_formatted(self):
        tz = pytz.timezone("Europe/Volgograd")
        now = datetime.now(tz)
        return now

    def getData(self, params):
        """
        Функция получения ссылок и содержимого сайтов путем скрепинга

        :param params: tuple(current_url, parent_url), где current_url -
        строка адреса текущей веб-страницы, parent_url - адрес родительской
        веб-страницы

        :return: список значений (record_type, record), где
        record_type = {'link', 'content'} - поле, определяющее тип записи, а
        record - сама запись: tuple(текущая ссылка, родительская ссылка) либо
        текст содержимого. Если список по какой-либо причине не сформирован,
        возвращается None

        """
        current_url = params[0]
        parent_url = params[1]

        # out_time = {}
        # out_time = []

        try:
            self.__openURL(current_url)

            # Получение всех ссылок на веб-странице и формирование их списка
            output_vector = [
                (
                    "link",
                    (
                        elem.get_attribute("href"),
                        parent_url
                    )
                ) for elem in self.__driver.find_elements_by_css_selector(
                    r"a[href]"
                )
            ]

            #
            output_vector.append(
                (
                    current_url,
                    self.__driver.find_element_by_tag_name("body").text
                )

            )


            return output_vector

        # В случае ошибки при обработке страницы возвращается None
        except WebDriverException:
            return None

    def __exit__(self, exception_type, exception_value, traceback):
        self.__driver.quit()
