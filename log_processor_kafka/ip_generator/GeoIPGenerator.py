import pandas as pd
import numpy as np
import socket
import struct


class GeoIPGenerator:

    def __init__(self, geoip_file_path):
        self.geoip_df = self.read_geoip_csv(geoip_file_path)

    @staticmethod
    def read_geoip_csv(file_path):
        df = pd.read_csv(file_path, delimiter=',', encoding='utf-8')
        return df

    def generate_random_ip(self):
        # Choisissez une ligne aléatoire du DataFrame
        random_row = self.geoip_df.sample(n=1)

        # Obtenez les valeurs de start_ip, end_ip et country
        start_ip = random_row['start_ip'].values[0]
        end_ip = random_row['end_ip'].values[0]
        country = random_row['country'].values[0]

        # Générez une adresse IP aléatoire entre start_ip et end_ip
        random_ip = self.generate_random_ip_within_range(start_ip, end_ip)

        return random_ip, country

    @staticmethod
    def generate_random_ip_within_range(start_ip, end_ip):
        # Convertir les adresses IP en entiers non signés
        start_ip_int = struct.unpack('!I', socket.inet_aton(start_ip))[0]
        end_ip_int = struct.unpack('!I', socket.inet_aton(end_ip))[0]

        # Générer une adresse IP aléatoire entre start_ip_int et end_ip_int
        random_ip_int = np.random.randint(start_ip_int, end_ip_int + 1)

        # Convertir l'entier en adresse IP
        random_ip = socket.inet_ntoa(struct.pack('!I', random_ip_int))

        return random_ip
