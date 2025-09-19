'''
This tests file concern all functions in the get_game_details_lnb.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from pandas.testing import assert_frame_equal
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import get_game_details_lnb as game_details

#example of this game: https://www.lnb.fr/elite/game-center-resume/?id=28846 - copied in game_html.txt
with open("materials/game_html.txt", 'r', encoding='utf-8') as file:
        SAMPLE_HTML = file.read() 

def test_get_gameday_lnb():
    # this test the function get_gameday_lnb
    expected = "Finale - Episode 1"
    result = game_details.get_gameday_lnb(SAMPLE_HTML,"log")
    assert result == expected

def test_get_teams_lnb():
    # this test the function get_teams_lnb
    expected = ["Paris", "Monaco"]
    result = game_details.get_teams_lnb(SAMPLE_HTML,"log")
    assert result == expected

def test_get_date_lnb():
    # this test the function get_date_lnb
    expected = "2025-06-15"
    result = game_details.get_date_lnb(SAMPLE_HTML,"log")
    assert result == expected

def test_get_time_lnb():
    # this test the function get_time_lnb
    result = game_details.get_time_lnb(SAMPLE_HTML)
    assert result is None

def test_get_scores_lnb():
    # this test the function get_scores_lnb
    expected = [94,82]
    result = game_details.get_scores_lnb(SAMPLE_HTML,"log")
    assert result == expected

game_row = {
    'COMPETITION_SOURCE': 'LNB',
    'COMPETITION_ID': '123',
    'SEASON_ID': '2024',
    'GAME_SOURCE_ID': '456'
}

@patch("get_game_details_lnb.urllib.urlopen")
def test_get_game_details(mock_urlopen):
    # this test the function get_game_details
    # It mocks the url connection to SAMPLE_HTML

    expected = pd.DataFrame({
        'COMPETITION_SOURCE': ['LNB'],
        'COMPETITION_ID': ['123'],
        'SEASON_ID': ['2024'],
        'GAMEDAY': ['Finale - Episode 1'],
        'DATE_GAME_LOCAL': ['2025-06-15'],
        'TIME_GAME_LOCAL': [None],
        'TEAM_HOME': ['Paris'],
        'SCORE_HOME': [94],
        'TEAM_AWAY': ['Monaco'],
        'SCORE_AWAY': [82],
        'GAME_SOURCE_ID': ['456']
    })

    mock_response = MagicMock()
    mock_response.read.return_value = SAMPLE_HTML.encode("utf-8")
    mock_urlopen.return_value = mock_response

    result = game_details.get_game_details_lnb("http://fake.url", game_row)
    assert_frame_equal(result.reset_index(drop=True), expected.reset_index(drop=True))

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_gameday_lnb))
    test_suite.addTest(unittest.FunctionTestCase(test_get_teams_lnb))
    test_suite.addTest(unittest.FunctionTestCase(test_get_date_lnb))
    test_suite.addTest(unittest.FunctionTestCase(test_get_time_lnb))
    test_suite.addTest(unittest.FunctionTestCase(test_get_scores_lnb))
    test_suite.addTest(unittest.FunctionTestCase(test_get_game_details))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
