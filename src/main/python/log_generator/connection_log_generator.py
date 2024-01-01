import random
import string
import datetime

from .geo_ip_generator import GeoIPGenerator


class ConnectionLogGenerator:
    TRUE_FALSE_CHOICES = [True, False]

    def __init__(self, ipv4_country_db_file_path):
        self.common_usernames = ["user", "admin", "root", "john_doe", "jane_doe", "user123", "admin123"]
        self.common_passwords = ["password", "admin123", "123456"]
        self.common_ports = [22, 80, 443, 3306, 5432, 8080]
        self.common_connection_types = ["ssh", "http", "ftp", "telnet", "rdp"]
        self.geoip_generator = GeoIPGenerator(ipv4_country_db_file_path)
        self.authorized_users = [
            {"username": "root", "password": "securepasswordroot", "ip_address": "185.7.72.105", "port": 22,
             "connection_type": "ssh", "country": "FR", "connection_hours": (1, 6),
             "is_connected": False, "connection_timestamp": None},
            {"username": "admin", "password": "securepasswordadmin", "ip_address": "185.7.72.106", "port": 22,
             "connection_type": "ssh", "country": "FR", "connection_hours": (1, 6),
             "is_connected": False, "connection_timestamp": None},
            {"username": "authorized_user1", "password": "securepassword1", "ip_address": "185.7.72.101", "port": 22,
             "connection_type": "ssh", "country": "FR", "connection_hours": (7, 12),
             "is_connected": False, "connection_timestamp": None},
            {"username": "authorized_user2", "password": "securepassword2", "ip_address": "185.7.73.102", "port": 22,
             "connection_type": "ssh", "country": "FR", "connection_hours": (12, 17),
             "is_connected": False, "connection_timestamp": None},
            {"username": "authorized_user3", "password": "securepassword3", "ip_address": "185.7.73.103", "port": 22,
             "connection_type": "ssh", "country": "FR", "connection_hours": (17, 22),
             "is_connected": False, "connection_timestamp": None},
        ]

    @staticmethod
    def connect_user(authorized_users, user, current_timestamp):
        return [
            {
                **existing_user,
                "is_connected": True,
                "connection_timestamp": current_timestamp
            }
            if existing_user.get("username") == user.get("username")
            else existing_user
            for existing_user in authorized_users
        ]

    @staticmethod
    def disconnect_authorized_user(authorized_user_list, current_timestamp):
        return [
            {
                **user,
                "is_connected": False,
                "connection_timestamp": None
            }
            if user.get("is_connected") and user.get("connection_timestamp") is not None
               and current_timestamp - user["connection_timestamp"] > datetime.timedelta(hours=1)
            else user
            for user in authorized_user_list
        ]

    def generate_log(self):
        current_time = datetime.datetime.now()
        current_timestamp = datetime.datetime.fromtimestamp(current_time.timestamp())

        self.authorized_users = self.disconnect_authorized_user(self.authorized_users, current_timestamp)

        authorized_user = None
        for user in self.authorized_users:
            start_hour, end_hour = user["connection_hours"]
            if start_hour <= current_time.hour <= end_hour:
                authorized_user = user
                break

        if (authorized_user is not None and not authorized_user.get("is_connected", True)
                and random.choices(self.TRUE_FALSE_CHOICES, weights=[0.1, 0.9])[0]):
            user = authorized_user
            auth_success = True
            self.authorized_users = self.connect_user(self.authorized_users, user, current_timestamp)
        else:
            user = self.generate_fake_user()
            auth_success = False

        r_ip_address = user["ip_address"]
        country = user["country"]
        port = user["port"]
        connection_type = user["connection_type"]
        password = user["password"]

        return {
            "timestamp": int(current_timestamp.timestamp()),
            "username": user["username"],
            "ip_address": r_ip_address,
            "country": country,
            "port": port,
            "connection_type": connection_type,
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
            "connection_type": random.choice(self.common_connection_types),
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
