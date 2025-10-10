'''
This tests file concern all functions in the exe_playoffs_table module.
It units test unexpected path for each function
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

def test_get_matchups_strings_empty_list():
    # this test the get_matchups_strings function with empty list. Must return empty list
    assert exe_playoffs_table.get_matchups_strings([]) == []

def test_get_matchups_strings_invalid_type():
    # this test the get_matchups_strings function with list with invalid types. Must exit the program
    try:
        exe_playoffs_table.get_matchups_strings([None])    
    except SystemExit as e:
        assert e.code == 1

def test_get_results_strings_all_zero_results():
    # this test the get_results_strings function with only zeros results. Must return an empty string
    results = [["0", "0", "0"]]
    assert exe_playoffs_table.get_results_strings(results) == [""]

def test_get_results_strings_invalid_type():
    # this test the get_results_strings function with invalid type. Must exit the program
    try:
        exe_playoffs_table.get_results_strings([[123, 456]])
    except SystemExit as e:
        assert e.code == 1

def test_display_textbox_with_result_adds_artist():
    # this test the display_textbox function with one artist
    fig, ax = plt.subplots()
    ax = exe_playoffs_table.display_textbox(ax, 1, 1, "TeamA\nTeamB", "2-1")
    assert len(ax.artists) == 1

def test_display_textbox_without_result_adds_text():
    # this test the display_textbox function without results
    fig, ax = plt.subplots()
    ax = exe_playoffs_table.display_textbox(ax, 1, 1, "TeamA\nTeamB", "")
    assert any("TeamA" in t.get_text() for t in ax.texts)

def test_draw_line_and_display_pass():
    # this test the draw_line and display_pass function
    fig, ax = plt.subplots()
    ax = exe_playoffs_table.draw_line(ax, 0, 1, 0, 1)
    ax = exe_playoffs_table.display_pass(ax, 1, 1, "Pass")
    assert len(ax.lines) == 1
    assert any("Pass" in t.get_text() for t in ax.texts)

@patch("exe_playoffs_table.download_file", side_effect=Exception("dropbox error"))
def test_draw_playoffs_image_download_fails(mock_download):
    # this test the draw_playoffs_image function, with an error in download_file fails. Must exit the program
    try:
        exe_playoffs_table.draw_playoffs_image()
    except SystemExit as e:
        assert e.code == 1

@patch("exe_playoffs_table.download_file", return_value={"str_playoffs_table": "invalid_code"})
def test_draw_playoffs_image_exec_fails(mock_download):
    # this test the draw_playoffs_image function, with an invalid code read in exe. Must exit the program
    try:
        exe_playoffs_table.draw_playoffs_image()
    except SystemExit as e:
        assert e.code == 1

@patch("exe_playoffs_table.download_file", return_value={"str_playoffs_table": "playoffs_matchups=[]\ns='X'\nplayoffs_results=[]\nplaysoffs_title='Title'\nplayoffs_round=[]\nplayoffs_message='msg'\nplayoffs_passvalues=[]"})
@patch("exe_playoffs_table.create_jpg", side_effect=Exception("create_jpg failed"))
def test_draw_playoffs_image_create_jpg_fails(mock_create, mock_download):
    # this test the draw_playoffs_image function, with create_jpg failing. Must exit the program
    try:
        exe_playoffs_table.draw_playoffs_image()
    except SystemExit as e:
        assert e.code == 1

@patch("exe_playoffs_table.download_file", return_value={"str_playoffs_table": "playoffs_matchups=[['A']]\ns='A'\nplayoffs_results=[['1']]\nplaysoffs_title='T'\nplayoffs_round=['R1']\nplayoffs_message='msg'\nplayoffs_passvalues=['X']*11"})
@patch("exe_playoffs_table.create_jpg")
@patch("builtins.open", new_callable=mock_open, read_data="fake")
@patch("requests.post")
def test_draw_playoffs_image_imgbb_bad_json(mock_post, mock_file, mock_create, mock_download):
    # this test the draw_playoffs_image function, with ca bad json for pushing image online. Must exit the program
    mock_post.return_value.json = lambda: {"bad": "json"}
    try:
        exe_playoffs_table.draw_playoffs_image()
    except SystemExit as e:
        assert e.code == 1

if __name__ == "__main__":
    test_suite = unittest.TestSuite()

    test_suite.addTest(unittest.FunctionTestCase(test_get_matchups_strings_empty_list))
    test_suite.addTest(unittest.FunctionTestCase(test_get_matchups_strings_invalid_type))
    test_suite.addTest(unittest.FunctionTestCase(test_get_results_strings_all_zero_results))
    test_suite.addTest(unittest.FunctionTestCase(test_get_results_strings_invalid_type))
    test_suite.addTest(unittest.FunctionTestCase(test_display_textbox_with_result_adds_artist))
    test_suite.addTest(unittest.FunctionTestCase(test_display_textbox_without_result_adds_text))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_line_and_display_pass))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_playoffs_image_download_fails))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_playoffs_image_exec_fails))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_playoffs_image_create_jpg_fails))
    test_suite.addTest(unittest.FunctionTestCase(test_draw_playoffs_image_imgbb_bad_json))

    runner = unittest.TextTestRunner()
    runner.run(test_suite)