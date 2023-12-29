import os
import datetime
import unittest
from unittest.mock import patch
from src.main.python.log_generator.connexion_log_generator import ConnexionLogGenerator


class TestConnexionLogGenerator(unittest.TestCase):

    def setUp(self):
        self.ipv4_country_db_file_path = os.path.abspath('./resources/geolite2-country-ipv4.csv')
        self.connexion_log_generator = ConnexionLogGenerator(self.ipv4_country_db_file_path)

    @patch('src.main.python.log_generator.connexion_log_generator.datetime')
    @patch('src.main.python.log_generator.connexion_log_generator.random')
    def test_generate_log_for_authorized_user(self, mock_random, mock_datetime):
        mock_datetime.datetime.now.return_value = datetime.datetime(2023, 1, 1, 8, 0, 0)
        mock_random.choices.return_value = [True]

        log = self.connexion_log_generator.generate_log()
        self.assertIn(log["user"], ["root", "admin", "authorized_user1", "authorized_user2", "authorized_user3"])
        self.assertTrue(log["auth_success"])

    @patch('src.main.python.log_generator.connexion_log_generator.datetime')
    @patch('src.main.python.log_generator.connexion_log_generator.random')
    @patch.object(ConnexionLogGenerator, 'generate_random_username')
    @patch.object(ConnexionLogGenerator, 'generate_random_password')
    def test_generate_log_for_fake_user(self, mock_generate_random_password, mock_generate_random_username,
                                        mock_random, mock_datetime):
        mock_datetime.datetime.now.return_value = datetime.datetime(2023, 1, 1, 15, 0, 0)
        mock_random.choices.return_value = [False]
        mock_generate_random_username.return_value = "fakeuser"
        mock_generate_random_password.return_value = "fakepassword"

        log = self.connexion_log_generator.generate_log()
        mock_generate_random_username.assert_called_with()
        mock_generate_random_password.assert_called_with()
        self.assertFalse(log["auth_success"])
