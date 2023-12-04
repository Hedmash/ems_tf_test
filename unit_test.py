import unittest
from unittest.mock import patch, MagicMock
from tf_json import DataProcessor, ConfigHandler

class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = DataProcessor()

    def test_flatten_json(self):
        test_data = {'key1': 'value1', 'key2': {'sub_key': 'sub_value'}}
        expected_output = [('key1', 'value1'), ('key2_sub_key', 'sub_value')]
        flattened_data = list(self.processor.flatten_json(test_data))
        self.assertEqual(flattened_data, expected_output)

    def test_load_data(self):
        test_file = 'test_data.json'
        expected_data = {'key': 'value'}
        with patch('builtins.open') as mock_open:
            mock_file = MagicMock()
            mock_file.__enter__.return_value = mock_file
            mock_file.read.return_value = '{"key": "value"}'
            mock_open.return_value = mock_file

            data = self.processor.load_data(test_file)
            self.assertEqual(data, expected_data)

class TestConfigHandler(unittest.TestCase):
    def setUp(self):
        self.config_handler = ConfigHandler()

    def test_load_config(self):
        with patch('builtins.open') as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = 'cfg_data'

            config_data = self.config_handler.load_config('db_config.yml')
            self.assertEqual(config_data, 'cfg_data')

if __name__ == '__main__':
    cfg_data = {
        'postgres': {
            'host': 'localhost',
            'port': 5432,
            'database': 'db',
            'user': 'user',
            'password': 'password'
        },
        'mongo': {
            'host': 'localhost',
            'port': 27017,
            'database': 'mongo_db',
            'user': 'mongo_user',
            'password': 'mongo_password'
        },
        'mysql': {
            'host': 'localhost',
            'port': 3306,
            'database': 'mysql_db',
            'user': 'mysql_user',
            'password': 'mysql_password'
        }
    }
    unittest.main()
