'''
This tests file concern all functions in the output_actions module.
It units test unexpected path for each function
'''
import unittest
from unittest import mock
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from testutils import assertExit
import output_actions

def test_translate_param_for_country_missing_country_key():
    # this test the function translate_param_for_country with missing country key. Must exit the program.
    translations = {"FR": {"hello": "bonjour"}}
    assertExit(lambda: output_actions.translate_param_for_country("hello", "DE", translations))

def test_translate_param_for_country_invalid_type():
    # this test the function translate_param_for_country with invalid type (int instead of string). Must exit the program.
    translations = {"FR": {"hello": "bonjour"}}
    assertExit(lambda: output_actions.translate_param_for_country(12345, "FR", translations))

def test_format_message_invalid_type():
    # this test the function format_message with invalid type (int instead of string). Must exit the program.
    assertExit(lambda: output_actions.format_message(123))

def test_format_message_invalid_pattern():
    # this test the function format_message with bad pattern. It must be accepted, without change.
    result = output_actions.format_message("Hello |X| world")
    assert result == "Hello |X| world"

def test_replace_conditionally_message_empty_text():
    # this test the function replace_conditionally_message with empty text. It must be return an empty text.
    result = output_actions.replace_conditionally_message("", "[b]", "[/b]", True)
    assert result == ""

def test_replace_conditionally_message_tags_missing():
    # this test the function replace_conditionally_message with missing tags. It must be accepted without changing text.
    text = "Hello world"
    result = output_actions.replace_conditionally_message(text, "[b]", "[/b]", False)
    assert result == "Hello world"

def test_define_filename_missing_columns():
    # this test the function define_filename with missing columns (GAMEDAY for example). It must exit the program.
    sr_gameday_output_init = pd.Series({"SEASON_ID": "S1"})
    assertExit(lambda: output_actions.define_filename("forumoutput", sr_gameday_output_init, "txt"))

def test_define_filename_none_country():
    # this test the function define_filename with missing country. It must be accepted, and write a name without country
    sr_gameday_output_init = pd.Series({"SEASON_ID": "S1", "GAMEDAY": "1ere journee"})
    fname = output_actions.define_filename("forumoutput", sr_gameday_output_init, "txt", country=None)
    assert fname == "forumoutput_s1_1erejournee.txt"

def test_display_rank_empty_df():
    # this test the function define_filename with empty dataframe. It must be accepted, and return an empty dataframe
    df = pd.DataFrame(columns=['rank'])
    result = output_actions.display_rank(df, 'rank')
    unittest.TestCase().assertTrue(result.empty)

def test_display_rank_duplicate_ranks():
    # this test the function define_filename with duplicate column rank. It must return a rank '-'
    df = pd.DataFrame({'rank': [1,1,2]})
    result = output_actions.display_rank(df, 'rank')
    unittest.TestCase().assertEqual(list(result['rank']), [1,'-',2])

def test_capture_df_oneheader_empty_df():
    # this test the function capture_df_oneheader with an empty df. It must exit the program.
    df = pd.DataFrame()
    assertExit(lambda: output_actions.capture_df_oneheader(df, "capture_empty"))
'''
def test_push_capture_online_missing_file():
    # this test the function push_capture_online with a missing jpg file. Must exit the program
    assertExit(lambda: output_actions.push_capture_online("non_existing_file.jpg"))
        
def test_push_capture_online_invalid_api_key():
    # this test the function push_capture_online with an invalid API key. Must exit the program
    # Patch 'open' in builtins
    with mock.patch("builtins.open", mock.mock_open(read_data="data")):
        # Patch requests.post to simulate invalid API key
        with mock.patch("requests.post") as mock_post:
            mock_response = mock.Mock()
            mock_response.json.side_effect = Exception("Invalid API key")
            mock_post.return_value = mock_response
            
        assertExit(lambda: output_actions.push_capture_online("../materials/example.jpg"))
'''            
if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_translate_param_for_country_missing_country_key))
    test_suite.addTest(unittest.FunctionTestCase(test_translate_param_for_country_invalid_type))
    test_suite.addTest(unittest.FunctionTestCase(test_format_message_invalid_type))
    test_suite.addTest(unittest.FunctionTestCase(test_format_message_invalid_pattern))
    test_suite.addTest(unittest.FunctionTestCase(test_replace_conditionally_message_empty_text))
    test_suite.addTest(unittest.FunctionTestCase(test_replace_conditionally_message_tags_missing))
    test_suite.addTest(unittest.FunctionTestCase(test_define_filename_missing_columns))
    test_suite.addTest(unittest.FunctionTestCase(test_define_filename_none_country))
    test_suite.addTest(unittest.FunctionTestCase(test_display_rank_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_display_rank_duplicate_ranks))
    test_suite.addTest(unittest.FunctionTestCase(test_capture_df_oneheader_empty_df))
    '''test_suite.addTest(unittest.FunctionTestCase(test_push_capture_online_missing_file))
    test_suite.addTest(unittest.FunctionTestCase(test_push_capture_online_invalid_api_key))'''
    runner = unittest.TextTestRunner()
    runner.run(test_suite)