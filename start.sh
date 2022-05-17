#!/bin/bash
# Файл запуска периодического распределенного веб-скрепера на Apache Spark
# Определяем расположение виртуального окружения Python
export VIRTUAL_PYTHON_ENV="/opt/kachanov_spark/spark-scraper-env-keras-bert"

# Загружаем виртуальное окружение Python
source /opt/miniconda3/etc/profile.d/conda.sh
conda activate $VIRTUAL_PYTHON_ENV

# Устанавливаем переменные окружения для Apache Spark
export HADOOP_CONF_DIR=/etc/hadoop_spark/conf
export PYSPARK_PYTHON=`which python`
export PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON
export HDP_VERSION=2.7.3.2.6.3.0-235

# Устанавливаем переменные окружения для веб-скрепера
# Расположение драйвера браузера Google Chrome
export CHROME_PATH="/usr/bin/chromedriver"

# Время в секундах, в течение которого браузер будет ожидать загрузку
# страницы
export CHROME_PAGE_LOAD_TIMEOUT=120

# Расположение файлов модели BERT
export BERT_MODEL_PATH="/mnt/tmpfs/rubert_cased_L-12_H-768_A-12_v2"

# Расположение в HDFS текстового файла, содержащего список веб-сайтов
# для скрепинга
export URLS_TEXTFILE_HDFS_PATH="urls.txt"

# Режим работы скрепера на YARN: "client" или "cluster"
export DEPLOY_MODE="client"

# Глубина обработки ссылок при скрепинге
export NESTING_DEPTH=1

# Обрабатывать только те вложенные ссылки, которые принадлежат к тому
# же домену, что и родительские
export IS_NESTING_WITHIN_DOMAIN="True"

# Период времени в минутах, через который выполняется скрепинг сайтов
export TIMEFRAME_MINUTES=360

# Изначальное количество разделов данных
export NUM_PARTITIONS=20

# Количество потоков для экзекутора
export EXECUTOR_THREADS=48

# Количество экзекуторов
export NUM_EXECUTORS=2

# Доменное имя или IP-адрес сервера сообщений
export MQ_HOSTNAME="node39.cluster"

# Порт сервера сообщений
export MQ_PORT=5672

# Название очереди, в которую отправляются сообщения
export MQ_QUEUE="scraping"

# Имя пользователя сервера сообщений
export MQ_USERNAME="scraper_user"

# Пароль пользователя сервера сообщений
export MQ_PASSWORD="scraper_pass"

# Кол-во итераций для функции GAP-статистики
export NUM_REFS=50

# Кол-во предложений отбираемых из самого большого кластера
export NUM_TAKE=10

# Пусть к словарю слов/частей слов
export PATH_TO_VOC="/home/kachanov/BERT/vocab.txt"

# Копируем файл с адресами веб-сайтов для скрепинга в HDFS
echo "Выполняется копирование файла с веб-адресами в HDFS ..."
hadoop fs -copyFromLocal -f urls/urls.txt urls.txt || exit 1
echo "Файл со списком веб-адресов успешно скопирован в HDFS."

# Формируем zip-архив со всеми файлами исходного кода
echo "Выполняется создание ZIP архива с исходными кодами ..."
cd src/ || exit 1
zip -R scraper.zip "*.py" || exit 1
cd .. || exit 1
echo "ZIP архив с исходными кодами успешно создан."
echo "Запускается скрепер ..."

# Запускаем периодический распределенный web-скрепер
/usr/hdp/current/spark2-client/bin/spark-submit --master yarn \
    --deploy-mode $DEPLOY_MODE --py-files src/scraper.zip \
    --executor-cores $EXECUTOR_THREADS --num-executors $NUM_EXECUTORS \
    --executor-memory=64G --driver-memory=20G \
    src/scraper.py $CHROME_PATH $CHROME_PAGE_LOAD_TIMEOUT $BERT_MODEL_PATH \
    $URLS_TEXTFILE_HDFS_PATH $NESTING_DEPTH $IS_NESTING_WITHIN_DOMAIN \
    $TIMEFRAME_MINUTES $NUM_PARTITIONS \
    $MQ_HOSTNAME $MQ_PORT $MQ_QUEUE $MQ_USERNAME $MQ_PASSWORD \
    $NUM_REFS $NUM_TAKE $PATH_TO_VOC
