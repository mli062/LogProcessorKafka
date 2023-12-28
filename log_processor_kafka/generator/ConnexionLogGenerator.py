import random
import string
import datetime

from log_processor_kafka.generator.GeoIPGenerator import GeoIPGenerator


class ConnexionLogGenerator:

    TRUE_FALSE_CHOICES = [True, False]

    def __init__(self, ipv4_country_db_file_path):
        self.common_usernames = ["user", "admin", "root", "john_doe", "jane_doe", "user123", "admin123"]
        self.common_passwords = ["password", "admin123", "123456"]
        self.common_ports = [22, 80, 443, 3306, 5432, 8080]
        self.common_connexion_types = ["ssh", "http", "ftp", "telnet", "rdp"]
        self.geoip_generator = GeoIPGenerator(ipv4_country_db_file_path)
        self.authorized_users = [
            {"username": "root", "password": "securepasswordroot", "ip_address": "185.7.72.105", "port": 22,
             "connexion_type": "ssh", "country": "FR", "connection_hours": (1, 6)},
            {"username": "admin", "password": "securepasswordadmin", "ip_address": "185.7.72.106", "port": 22,
             "connexion_type": "ssh", "country": "FR", "connection_hours": (1, 6)},
            {"username": "authorized_user1", "password": "securepassword1", "ip_address": "185.7.72.101", "port": 22,
             "connexion_type": "ssh", "country": "FR", "connection_hours": (7, 12)},
            {"username": "authorized_user2", "password": "securepassword2", "ip_address": "185.7.73.102", "port": 22,
             "connexion_type": "ssh", "country": "FR", "connection_hours": (12, 17)},
            {"username": "authorized_user3", "password": "securepassword3", "ip_address": "185.7.73.103", "port": 22,
             "connexion_type": "ssh", "country": "FR", "connection_hours": (17, 22)},
        ]

    def generate_log(self):
        current_time = datetime.datetime.now()

        authorized_user = None
        for user in self.authorized_users:
            start_hour, end_hour = user["connection_hours"]
            if start_hour <= current_time.hour <= end_hour:
                authorized_user = user
                break

        if authorized_user is not None and random.choices(self.TRUE_FALSE_CHOICES, weights=[0.2, 0.8])[0]:
            user = authorized_user
            auth_success = True
        else:
            user = self.generate_fake_user()
            auth_success = False

        timestamp = current_time.strftime("%b %d %H:%M:%S")
        r_ip_address = user["ip_address"]
        country = user["country"]
        port = user["port"]
        connexion_type = user["connexion_type"]
        password = user["password"]

        return {
            "timestamp": timestamp,
            "user": user["username"],
            "ip_address": r_ip_address,
            "country": country,
            "port": port,
            "connexion_type": connexion_type,
            "auth_success": auth_success,
            "password": password,
        }

    def generate_fake_user(self):
        r_ip_address, country = self.geoip_generator.generate_random_ip()
        return {
            "username": self.generate_random_username(),
            "password": self.generate_random_password(),
            "ip_address": r_ip_address,
            "port": self.generate_random_port(),
            "connexion_type": random.choice(self.common_connexion_types),
            "country": country,
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
