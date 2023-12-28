import random
import string
import datetime

from log_processor_kafka.ip_generator.GeoIPGenerator import GeoIPGenerator


class ConnexionLogGenerator:

    TRUE_FALSE_CHOICES = [True, False]

    def __init__(self, ipv4_country_db_file_path):
        self.common_usernames = ["user", "admin", "root", "john_doe", "jane_doe", "user123", "admin123"]
        self.common_passwords = ["password", "admin123", "123456"]
        self.common_ports = [22, 80, 443, 3306, 5432, 8080]
        self.common_connexion_types = ["ssh", "http", "ftp", "telnet", "rdp"]
        self.geoip_generator = GeoIPGenerator(ipv4_country_db_file_path)

    def generate_log(self):
        timestamp = datetime.datetime.now().strftime("%b %d %H:%M:%S")
        user = self.generate_random_username()
        r_ip_address, country = self.geoip_generator.generate_random_ip()
        port = self.generate_random_port()
        connexion_type = random.choice(self.common_connexion_types)
        auth_success = random.choices(self.TRUE_FALSE_CHOICES, weights=[0.2, 0.8])[0]
        password = self.generate_random_password()

        return {
            "timestamp": timestamp,
            "user": user,
            "ip_address": r_ip_address,
            "country": country,
            "port": port,
            "connexion_type": connexion_type,
            "auth_success": auth_success,
            "password": password,
        }

    def generate_random_port(self):
        if random.choice(self.TRUE_FALSE_CHOICES):
            return random.randint(1024, 65535)
        else:
            return random.choice(self.common_ports)

    def generate_random_username(self):
        if random.choice(self.TRUE_FALSE_CHOICES):
            return ''.join(random.choice(string.ascii_lowercase) for _ in range(8))
        else:
            return random.choice(self.common_usernames)

    def generate_random_password(self):
        if random.choice(self.TRUE_FALSE_CHOICES):
            length = random.randint(0, 16)
            if length == 0:
                return ''
            characters = string.ascii_letters + string.digits + string.punctuation
            return ''.join(random.choice(characters) for _ in range(length))
        else:
            return random.choice(self.common_usernames + self.common_passwords)
