'''
This tests file concern all functions in the imgbb_actions module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch, mock_open
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import imgbb_actions

def test_push_capture_online():

    # this test the push_capture_online function
    image_path = "image.png" 
    expected_url = "https://fakeurl.com/image.png"
    mock_response = type("MockResponse", (), {"json": lambda self: {'data': {'url': expected_url}}})()

    with patch("builtins.open", mock_open(read_data=b"fake image data")), \
         patch("requests.post", return_value=mock_response), \
         patch("os.getenv", return_value="fake_api_key"):
            
            result = imgbb_actions.push_capture_online(image_path)
            assert result == expected_url

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_push_capture_online))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)