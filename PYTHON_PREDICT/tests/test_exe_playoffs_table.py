'''
This tests file concern all functions in the exe_playoffs_table module.
It units test the happy path for each function
'''
import unittest
from unittest.mock import patch, MagicMock,mock_open
import pandas as pd
import builtins
import json
import matplotlib.pyplot as plt
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import exe_playoffs_table

def test_get_matchups_strings():
    # this test the get_matchups_strings function
    matchups = [["Team A", "Team B"], ["Team C", "Team D"]]
    result = exe_playoffs_table.get_matchups_strings(matchups)
    expected = ["Team A\nTeam B", "Team C\nTeam D"]
    unittest.TestCase().assertEqual(result, expected)

def test_get_results_strings_with_scores():
    # this test the get_results_strings function with scores non 0
    results = [["101", "99"], ["89", "95"]]
    result = exe_playoffs_table.get_results_strings(results)
    expected = ["101\n99", "89\n95"]
    unittest.TestCase().assertEqual(result, expected)

def test_get_results_strings_with_zeros():
    # this test the get_results_strings function with scores non 0
    results = [["0", "0"], ["120", "110"]]
    result = exe_playoffs_table.get_results_strings(results)
    expected = ["", "120\n110"]
    unittest.TestCase().assertEqual(result, expected)

def test_display_textbox_without_results():
    # this test the display_textbox function without result
    fig, ax = plt.subplots()
    ax_out = exe_playoffs_table.display_textbox(ax, 1, 2, "Team A\nTeam B", "", size=10)
    unittest.TestCase().assertIsInstance(ax_out, type(ax))

def test_display_textbox_with_results():
    # this test the display_textbox function with result
    fig, ax = plt.subplots()
    ax_out = exe_playoffs_table.display_textbox(ax, 1, 2, "Team A\nTeam B", "101\n99", size=10)
    unittest.TestCase().assertIsInstance(ax_out, type(ax))

def test_draw_line():
    # this test the draw_line function
    fig, ax = plt.subplots()
    ax_out = exe_playoffs_table.draw_line(ax, 1, 2, 3, 4)
    unittest.TestCase().assertIsInstance(ax_out, type(ax))

def test_display_pass():
    # this test the display_pass function
    fig, ax = plt.subplots()
    ax_out = exe_playoffs_table.display_pass(ax, 1, 2, "WIN")
    unittest.TestCase().assertIsInstance(ax_out, type(ax))

@patch("exe_playoffs_table.download_file", return_value={"str_playoffs_table": "playoffs_matchups=[['A','B'],['C','D'],['E','F'],['G','H'],['I','J'],['K','L'],['M','N'],['O','P'],['Q','R'],['S','T'],['Champion']];s='Champion';playoffs_results=[['1','0'],['0','1'],['0','0'],['0','0'],['0','0'],['0','0'],['0','0'],['0','0'],['0','0'],['0','0'],['0','0']];playsoffs_title='Playoffs';playoffs_round=['Round1'];playoffs_message='Good luck';playoffs_passvalues=['P']*11"})
@patch("exe_playoffs_table.create_jpg")
@patch("exe_playoffs_table.Image.open")
@patch("exe_playoffs_table.requests.post")
@patch("exe_playoffs_table.config")
@patch("builtins.open", new_callable=mock_open, read_data=b"fake image data")
def test_draw_playoffs_image(mock_file, mock_config, mock_requests, mock_open_img, mock_create_jpg, mock_download_file):
    # this test the draw_playoffs_image function
    
    # mock config values
    mock_config.TMPF = "."
    mock_config.playoffs_table_code = "dummy"
    mock_config.trophy_file_path = "dummy_trophy"
    mock_config.create_local_folder = MagicMock()
    mock_config.destroy_local_folder = MagicMock()

    # mock PIL image
    mock_img = MagicMock()
    mock_open_img.return_value.convert.return_value = mock_img

    # mock requests
    mock_requests.return_value.json.return_value = {"data": {"url": "http://fake.url"}}

    exe_playoffs_table.draw_playoffs_image()

    mock_create_jpg.assert_called()
    mock_requests.assert_called()
    mock_config.destroy_local_folder.assert_called()
    mock_file.assert_called()  # ensure our fake open was used


if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_matchups_strings))
    test_suite.addTest(unittest.FunctionTestCase(test_get_results_strings_with_scores))
    test_suite.addTest(unittest.FunctionTestCase(test_get_results_strings_with_zeros))
    test_suite.addTest(unittest.FunctionTestCase(test_display_textbox_without_results))
    test_suite.addTest(unittest.FunctionTestCase(test_display_textbox_with_results))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_line))
    test_suite.addTest(unittest.FunctionTestCase(test_display_pass))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_playoffs_image))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
