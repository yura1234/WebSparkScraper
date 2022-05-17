# -*- coding: utf-8 -*-
import numpy as np
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.random import RandomRDDs
from numpy import divide

class Gap(object):
    def __init__(self):
        pass

    def disp(point, clusters, centers):
        """
        Функция, которая рассчитывает квадрат расстояния от центра кластера до его точки

        :param point: точка-вектор
        :param clusters: переменная класса k-средних
        :param centers: массив центров кластеров
        :return: сумма квадрата разности от цента до точки
        """
        num_cluster = clusters.predict(point)
        return np.sum((centers[num_cluster] - point) ** 2)

    def calc_dispersion(rdd, K):
        """
        Функция обучения модели k-средних и подсчета расстояния от точки до центра кластера

        :param rdd: вектор рдд
        :param K: кол-во кластеров
        :return: расстояния от центра кластера до его точки, обученная модель
        """
        clusters = KMeans.train(rdd, K, maxIterations=10, initializationMode="random")
        centers = clusters.clusterCenters

        return rdd.map(lambda p: Gap.disp(p, clusters, centers)).reduce(lambda a, b: a + b), clusters

    def gap_func(sc, vec_rdd, K, n_refs, rdd_shape):
        """
        Функция подсчета Gap статистики

        :param sc: переменная SparkContext
        :param vec_rdd: вектор рдд
        :param K: кол-во кластеров
        :param n_refs: кол-во итераций 
        :param rdd_shape: размер рдд 
        :return: словарь состоящий из статистических переменных и обученной модели k-средних
        """
        a = vec_rdd.reduce(lambda x, y: np.minimum(x, y))
        b = vec_rdd.reduce(lambda x, y: np.maximum(x, y))

        ref_dispersions = []
        for i in range(0, n_refs):
            random_rdd = RandomRDDs.uniformVectorRDD(sc, rdd_shape[0], rdd_shape[1]).map(lambda v: v * (b - a) + a)

            dispersion, _ = Gap.calc_dispersion(random_rdd, K)
            ref_dispersions.append(dispersion)

        dispersion, model = Gap.calc_dispersion(vec_rdd, K)

        # Подсчет Gap статистики
        ref_log_dispersion = np.mean(np.log(ref_dispersions))
        log_dispersion = np.log(dispersion)
        gap_value = ref_log_dispersion - log_dispersion
        # вычисление стандартного отклонения
        sdk = np.sqrt(np.mean((np.log(ref_dispersions) - ref_log_dispersion) ** 2.0))
        sk = np.sqrt(1.0 + 1.0 / n_refs) * sdk

        gap_star = np.mean(ref_dispersions) - dispersion
        sdk_star = np.sqrt(np.mean((ref_dispersions - np.mean(ref_dispersions)) ** 2.0))
        sk_star = np.sqrt(1.0 + 1.0 / n_refs) * sdk_star

        return {'gap_value': gap_value,
                'sk': sk,
                'gap_star': gap_star,
                'sk_star': sk_star}, model