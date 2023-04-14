#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Exporter basado en:
https://trstringer.com/quick-and-easy-prometheus-exporter/
'''


import os
import time
from prometheus_client import start_http_server, Gauge, Enum
import requests
from FUNCAUX.APIS.api_redis import BdRedis
import pickle

class AppMetrics:
    """
    Representation of Prometheus metrics and loop to fetch and transform
    application metrics into Prometheus metrics.
    """

    def __init__(self, polling_interval_seconds=60):

        self.rh = BdRedis()
        self.polling_interval_seconds = polling_interval_seconds
        # Prometheus metrics to collect
        self.ping_frames_x_min = Gauge("app_frames_x_min", "frames_x_min")
        self.config_base_frames_x_min = Gauge("app_base_frames_x_min", "base_frames_x_min")
        self.data_frames_x_min = Gauge("app_data_frames_x_min", "data_frames_x_min")
        self.accesos_sql_x_min = Gauge("app_accesos_sql_x_min", "accesos_sql_x_min")
        self.accesos_redis_x_min = Gauge("app_accesos_redis_x_min", "accesos_redis_x_min")
        self.length_stats_queue = Gauge("app_length_stats_queue", "length_stats_queue")
        self.duracion_frames_avg_x_min = Gauge("app_duracion_frames_avg_x_min", "duracion_frames_avg_x_min")

    def run_metrics_loop(self):
        """Metrics fetching loop"""
        while True:
            self.fetch()
            time.sleep(self.polling_interval_seconds)

    def fetch(self):
        """
        Get metrics from application and refresh Prometheus metrics with
        new values.
        #
        Las metricas (spc_stats.py) son:
        count_frame_ping
        count_frame_config_base
        count_frame_data
        count_accesos_SQL
        count_accesos_REDIS
        duracion_frame
        length_stats_queue

        Leo toda la cola de datos de stats de REDIS (STATS_QUEUE) y calculo las estadisticas
        que guardo en los gauges correspondientes.
        """
        # Fetch raw status data from the application
        nro_items = self.rh.llen('STATS_QUEUE')
        # Leo todos los elementos de la lista
        l_pickled_stats = self.rh.lrange('STATS_QUEUE',0, nro_items)
        # y los borro.
        #_=self.rh.ltrim('STATS_QUEUE',0,nro_items)
        # Los proceso.
        count_frame_ping = 0
        count_frame_config_base = 0
        count_frame_data = 0
        count_accesos_SQL = 0
        count_accesos_REDIS = 0
        duracion_frame = 0
        length_stats_queue = 0
        for pdict in l_pickled_stats:
            d_stats = pickle.loads(pdict)
            count_frame_ping += d_stats.get('count_frame_ping',0)
            count_frame_config_base += d_stats.get('c count_frame_config_base',0)
            count_frame_data += d_stats.get('count_frame_data',0)
            count_accesos_SQL += d_stats.get('count_accesos_SQL',0)
            count_accesos_REDIS += d_stats.get('count_accesos_REDIS',0)
            duracion_frame += d_stats.get('duracion_frame',0)
            length_stats_queue = d_stats.get('count_frame_ping',0)  # El ultimo es el maximo.
        #
        duracion_frame_avg = duracion_frame / nro_items * 60 / self.polling_interval_seconds
        # Actualizo los gauges
        self.ping_frames_x_min.set(count_frame_ping)
        self.config_base_frames_x_min.set(count_frame_config_base)
        self.data_frames_x_min.set(count_frame_data)
        self.accesos_sql_x_min.set(count_accesos_SQL)
        self.accesos_redis_x_min.set(count_accesos_REDIS)
        self.length_stats_queue.set(length_stats_queue)
        self.duracion_frames_avg_x_min.set(duracion_frame_avg)

def main():

    """Main entry point"""

    polling_interval_seconds = 60
    exporter_port = 8022

    app_metrics = AppMetrics(
        polling_interval_seconds=polling_interval_seconds
    )
    start_http_server(exporter_port)
    app_metrics.run_metrics_loop()

if __name__ == "__main__":
    main()