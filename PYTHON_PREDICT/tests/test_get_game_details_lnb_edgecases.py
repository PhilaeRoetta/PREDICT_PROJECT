'''
This tests file concern all functions in the get_game_details_lnb.
It units test the enexpected path for each function, which return exception
'''
import unittest
from assertRaises import assertRaises
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import get_game_details_lnb as game_details


log_print = "test-log"

def test_get_gameday_lnb_missing_tag():
    # this test the function get_gameday_lnb without gameday tag
    html = "<html><body><div>No gameday here</div></body></html>"
    assertRaises(ValueError, game_details.get_gameday_lnb, html, log_print)

def test_get_teams_lnb_not_two():
    # this test the function get_teams_lnb with only one team (expected two)
    html = '<div class="team-name display-on-mobile">Team A</div>'
    assertRaises(ValueError, game_details.get_teams_lnb, html, log_print)

def test_get_date_lnb_invalid_month():
    # this test the function get_date_lnb with an invalid date which can't be parsed
    html = '<span class="date">5 unicornvember 2024</span>'
    assertRaises(KeyError, game_details.get_date_lnb, html, log_print)

def test_get_date_lnb_missing_tag():
    # this test the function get_date_lnb without date tag
    html = "<html><body><div>No date here</div></body></html>"
    assertRaises(ValueError, game_details.get_date_lnb, html, log_print)

def test_get_scores_lnb_missing_scores():
    # this test the function get_scores_lnb without score tag
    html = "<html><body><p>No scores</p></body></html>"
    assertRaises(ValueError, game_details.get_scores_lnb, html, log_print)

def test_get_scores_lnb_not_two():
    # this test the function get_scores_lnb with only one score (expected two)
    html = '<span class="score-team">80</span>'
    assertRaises(ValueError, game_details.get_scores_lnb, html, log_print)

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_gameday_lnb_missing_tag))
    test_suite.addTest(unittest.FunctionTestCase(test_get_teams_lnb_not_two))
    test_suite.addTest(unittest.FunctionTestCase(test_get_date_lnb_invalid_month))
    test_suite.addTest(unittest.FunctionTestCase(test_get_date_lnb_missing_tag))
    test_suite.addTest(unittest.FunctionTestCase(test_get_scores_lnb_missing_scores))
    test_suite.addTest(unittest.FunctionTestCase(test_get_scores_lnb_not_two))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
