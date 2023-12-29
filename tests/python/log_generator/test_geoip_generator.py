import os
import unittest
from src.main.python.log_generator.geo_ip_generator import GeoIPGenerator


class TestGeoIPGenerator(unittest.TestCase):

    def setUp(self):
        self.ipv4_country_db_file_path = os.path.abspath('./resources/geolite2-country-ipv4.csv')
        self.geo_ip_generator = GeoIPGenerator(self.ipv4_country_db_file_path)

    def test_generate_random_ip(self):
        random_ip, country = self.geo_ip_generator.generate_random_ip()

        self.assertIsNotNone(random_ip)
        self.assertIsNotNone(country)

    def test_generate_random_ip_within_range(self):
        start_ip = '192.168.1.1'
        end_ip = '192.168.1.10'
        random_ip = self.geo_ip_generator.generate_random_ip_within_range(start_ip, end_ip)

        ip_parts = list(map(int, random_ip.split('.')))
        start_ip_parts = list(map(int, start_ip.split('.')))
        end_ip_parts = list(map(int, end_ip.split('.')))

        self.assertGreaterEqual(ip_parts, start_ip_parts)
        self.assertLessEqual(ip_parts, end_ip_parts)

    def test_read_geoip_csv(self):
        geoip_df = self.geo_ip_generator.read_geoip_csv(self.ipv4_country_db_file_path)

        self.assertFalse(geoip_df.empty)
