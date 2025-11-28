'''
This tests file concern all functions in the output_actions module.
It units test unexpected path for each function
'''
import unittest
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from testutils import assertExit
import output_actions

def test_translate_param_for_country_missing_country_key():
    
    # this test the function translate_param_for_country with missing country key. Must exit the program.
    param_to_translate = 'hello world'
    country = 'ITALIA'
    translations = {'FRANCE': {'hello': 'bonjour'}}
    assertExit(lambda: output_actions.translate_param_for_country(param_to_translate, country, translations))

def test_translate_param_for_country_invalid_type():
    
    # this test the function translate_param_for_country with invalid type (int instead of string). Must exit the program.
    param_to_translate = 12345
    country = 'FRANCE'
    translations = {'FRANCE': {'hello': 'bonjour'}}
    assertExit(lambda: output_actions.translate_param_for_country(param_to_translate, country, translations))

def test_format_message_invalid_type():
    
    # this test the function format_message with invalid type (int instead of string). Must exit the program.
    message = 123
    assertExit(lambda: output_actions.format_message(message))

def test_format_message_invalid_pattern():
    
    # this test the function format_message with bad pattern. It must be accepted, without change.
    message = "Hello |X| world"
    result = output_actions.format_message(message)
    expected = "Hello |X| world"
    assert result == expected

def test_replace_conditionally_message_empty_text():
    
    # this test the function replace_conditionally_message with empty text. It must be return an empty text.
    output_text = ""
    begin_tag = "[START]"
    end_tag = "[END]"
    condition = True
    result = output_actions.replace_conditionally_message(output_text, begin_tag, end_tag, condition)
    expected = ""
    assert result == expected

def test_replace_conditionally_message_tags_missing():
    
    # this test the function replace_conditionally_message with missing tags. It must be accepted without changing text.
    output_text = "Hello World!"
    begin_tag = "[START]"
    end_tag = "[END]"
    condition = True
    result = output_actions.replace_conditionally_message(output_text, begin_tag, end_tag, condition)
    expected = "Hello World!"
    assert result == expected

def test_define_filename_missing_columns():
    
    # this test the function define_filename with missing columns (SEASON_ID for example). It must exit the program.
    input_type = "forumoutput_inited"
    sr_gameday_output_init = pd.read_csv("materials/edgecases/sr_gameday_output_init_noseasonid.csv").iloc[0]
    extension = "txt"
    country = "FRANCE"
    assertExit(lambda: output_actions.define_filename(input_type, sr_gameday_output_init, extension, country))

def test_define_filename_none_country():
    
    # this test the function define_filename with missing country. It must be accepted, and write a name without country
    input_type = "forumoutput_inited"
    sr_gameday_output_init = pd.read_csv("materials/sr_gameday_output_init.csv").iloc[0]
    extension = "txt"
    country = None
    result = output_actions.define_filename(input_type, sr_gameday_output_init, extension, country)
    expected = 'forumoutput_inited_s1_1erejournee.txt'
    assert result == expected

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
    capture_name = "test_capture.jpg"

    assertExit(lambda: output_actions.capture_df_oneheader(df, capture_name))

           
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
    runner = unittest.TextTestRunner()
    runner.run(test_suite)