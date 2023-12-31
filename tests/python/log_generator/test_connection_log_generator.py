import os
import datetime
import unittest
from unittest.mock import patch
from src.main.python.log_generator.connection_log_generator import ConnectionLogGenerator


class TestConnectionLogGenerator(unittest.TestCase):

    def setUp(self):
        self.ipv4_country_db_file_path = os.path.abspath('./resources/geolite2-country-ipv4.csv')
        self.connection_log_generator = ConnectionLogGenerator(self.ipv4_country_db_file_path)

    def test_connect_user(self):
        current_timestamp = datetime.datetime.fromtimestamp(datetime.datetime.now().timestamp())

        user = {
                "username": "authorized_user3",
                "password": "securepassword3",
                "ip_address": "185.7.73.103",
                "port": 22,
                "connection_type": "ssh",
                "country": "FR",
                "connection_hours": (17, 22),
                "is_connected": False,
                "connection_timestamp": None,
            }

        authorized_users = [
            {
                "username": "admin",
                "password": "securepasswordadmin",
                "ip_address": "185.7.72.106",
                "port": 22,
                "connection_type": "ssh",
                "country": "FR",
                "connection_hours": (1, 6),
                "is_connected": False,
                "connection_timestamp": None
            },
            {
                "username": "authorized_user3",
                "password": "securepassword3",
                "ip_address": "185.7.73.103",
                "port": 22,
                "connection_type": "ssh",
                "country": "FR",
                "connection_hours": (17, 22),
                "is_connected": False,
                "connection_timestamp": None,
            }
        ]

        authorized_users_expected = [
            {
                "username": "admin",
                "password": "securepasswordadmin",
                "ip_address": "185.7.72.106",
                "port": 22,
                "connection_type": "ssh",
                "country": "FR",
                "connection_hours": (1, 6),
                "is_connected": False,
                "connection_timestamp": None
            },
            {
                "username": "authorized_user3",
                "password": "securepassword3",
                "ip_address": "185.7.73.103",
                "port": 22,
                "connection_type": "ssh",
                "country": "FR",
                "connection_hours": (17, 22),
                "is_connected": True,
                "connection_timestamp": current_timestamp,
            }
        ]

        authorized_users_res = self.connection_log_generator.connect_user(
            authorized_users, user, current_timestamp
        )
        self.assertEqual(authorized_users_expected, authorized_users_res)

    def test_disconnect_authorized_user_disconnect_case(self):
        current_timestamp = datetime.datetime.fromtimestamp(datetime.datetime.now().timestamp())

        # Disconnect case
        timestamp_minus_two_hours = datetime.datetime.fromtimestamp(((current_timestamp - datetime.timedelta(hours=2))
                                                                     .timestamp()))
        authorized_users = [
            {
                "username": "authorized_user3",
                "password": "securepassword3",
                "ip_address": "185.7.73.103",
                "port": 22,
                "connection_type": "ssh",
                "country": "FR",
                "connection_hours": (17, 22),
                "is_connected": True,
                "connection_timestamp": timestamp_minus_two_hours,
            }
        ]
        authorized_users_expected = [
            {
                "username": "authorized_user3",
                "password": "securepassword3",
                "ip_address": "185.7.73.103",
                "port": 22,
                "connection_type": "ssh",
                "country": "FR",
                "connection_hours": (17, 22),
                "is_connected": False,
                "connection_timestamp": None,
            }
        ]
        authorized_users_res = self.connection_log_generator.disconnect_authorized_user(
            authorized_users, current_timestamp
        )
        self.assertEqual(authorized_users_expected, authorized_users_res)

    def test_disconnect_authorized_user_no_update(self):
        current_timestamp = datetime.datetime.fromtimestamp(datetime.datetime.now().timestamp())

        # No update
        timestamp_minus_thirty_mins = datetime.datetime.fromtimestamp(((current_timestamp -
                                                                        datetime.timedelta(minutes=30)).timestamp()))
        authorized_users = [
            {
                "username": "authorized_user3",
                "password": "securepassword3",
                "ip_address": "185.7.73.103",
                "port": 22,
                "connection_type": "ssh",
                "country": "FR",
                "connection_hours": (17, 22),
                "is_connected": True,
                "connection_timestamp": timestamp_minus_thirty_mins,
            }
        ]
        authorized_users_expected = [
            {
                "username": "authorized_user3",
                "password": "securepassword3",
                "ip_address": "185.7.73.103",
                "port": 22,
                "connection_type": "ssh",
                "country": "FR",
                "connection_hours": (17, 22),
                "is_connected": True,
                "connection_timestamp": timestamp_minus_thirty_mins,
            }
        ]
        authorized_users_res = self.connection_log_generator.disconnect_authorized_user(
            authorized_users, current_timestamp
        )
        self.assertEqual(authorized_users_expected, authorized_users_res)

    @patch('src.main.python.log_generator.connection_log_generator.datetime')
    @patch('src.main.python.log_generator.connection_log_generator.random')
    def test_generate_log_for_authorized_user(self, mock_random, mock_datetime):
        mock_datetime.datetime.now.return_value = datetime.datetime(2023, 1, 1, 8, 0, 0)
        mock_random.choices.return_value = [True]

        log = self.connection_log_generator.generate_log()
        self.assertIn(log["username"], ["root", "admin", "authorized_user1", "authorized_user2", "authorized_user3"])
        self.assertTrue(log["auth_success"])

    @patch('src.main.python.log_generator.connection_log_generator.datetime')
    @patch('src.main.python.log_generator.connection_log_generator.random')
    @patch.object(ConnectionLogGenerator, 'generate_random_username')
    @patch.object(ConnectionLogGenerator, 'generate_random_password')
    def test_generate_log_for_fake_user(self, mock_generate_random_password, mock_generate_random_username,
                                        mock_random, mock_datetime):
        mock_datetime.datetime.now.return_value = datetime.datetime(2023, 1, 1, 15, 0, 0)
        mock_random.choices.return_value = [False]
        mock_generate_random_username.return_value = "fakeuser"
        mock_generate_random_password.return_value = "fakepassword"

        log = self.connection_log_generator.generate_log()
        mock_generate_random_username.assert_called_with()
        mock_generate_random_password.assert_called_with()
        self.assertFalse(log["auth_success"])
