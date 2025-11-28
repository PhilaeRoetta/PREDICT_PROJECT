'''
This tests file concern all functions in the message_actions module.
It units test the happy path for each function
'''

import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os
from pandas.testing import assert_frame_equal
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import message_actions

def test_extract_messages_from_topic():
    
    # this test the function extract_messages_from_topic
    topic_row = pd.read_csv("materials/qTopics_Calculate.csv").iloc[0]
    ts_message_extract_min_utc = pd.Timestamp('2025-02-04 21:00:00')
    ts_message_extract_max_utc = pd.Timestamp('2025-02-04 22:30:00')

    mock_response = MagicMock()
    mock_response.read.return_value = b"<html></html>"
    mock_df = pd.read_csv("materials/message_check.csv")
    with patch('message_actions.urllib.urlopen') as mock_urlopen, \
         patch.dict('message_actions.messages_info_functions', {'BI': MagicMock()}):
        
        mock_urlopen.return_value = mock_response
        message_actions.messages_info_functions['BI'].return_value = mock_df

        result = message_actions.extract_messages_from_topic(topic_row,ts_message_extract_min_utc,ts_message_extract_max_utc)
        assert_frame_equal(result.reset_index(drop=True), mock_df.iloc[[1]].reset_index(drop=True))

def test_extract_messages():

    # this test the function extract_messages
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_output_need = pd.read_csv("materials/output_need_calculate.csv").iloc[0]

    mock_topics_scope_id = pd.read_csv("materials/qTopics_Calculate.csv")
    mock_ts_message_extract_min_utc = pd.Timestamp('2025-02-04 21:00:00')
    mock_ts_message_extract_max_utc = pd.Timestamp('2025-02-04 22:30:00')
    mock_results = pd.read_csv("materials/message_check.csv")

    with patch('message_actions.get_list_topics_from_need', return_value=mock_topics_scope_id), \
         patch('message_actions.get_extraction_time_range', return_value=(mock_ts_message_extract_min_utc,mock_ts_message_extract_max_utc)), \
         patch('message_actions.config.multithreading_run', return_value=[mock_results]), \
         patch('message_actions.create_csv'):

        df_messages, ts_message_extract_max_utc = message_actions.extract_messages(sr_snowflake_account, sr_output_need)
        assert_frame_equal(df_messages.reset_index(drop=True), mock_results.reset_index(drop=True))
        assert ts_message_extract_max_utc == mock_ts_message_extract_max_utc

def test_get_list_topics_from_need():
    
    # this test the function get_list_topics_from_need
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_output_need = pd.read_csv("materials/output_need_calculate.csv").iloc[0]

    mock_topics_scope_id = pd.read_csv("materials/qTopics_Calculate.csv")
    
    with patch('message_actions.snowflakeA.snowflake_execute', return_value=mock_topics_scope_id):
    
        df_topics = message_actions.get_list_topics_from_need(sr_snowflake_account, sr_output_need)
        assert_frame_equal(df_topics.reset_index(drop=True), mock_topics_scope_id.reset_index(drop=True))

def test_get_extraction_time_range():
    
    # this test the function get_extraction_time_range
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_output_need = pd.read_csv("materials/output_need_check_with_message_check_ts.csv").iloc[0]
    mock_df_time_max = pd.read_csv("materials/qGameDayEnd.csv")
    
    with patch('message_actions.snowflakeA.snowflake_execute', return_value = mock_df_time_max):
        min_ts, max_ts = message_actions.get_extraction_time_range(sr_snowflake_account, sr_output_need)
        assert min_ts == pd.Timestamp('2025-06-14 15:00:00')
        assert max_ts == pd.Timestamp('2025-10-31 18:30:00')

def test_post_message_bi():
    
    # this test the function post_message_bi
    topic_row = pd.read_csv("materials/qTopics_Calculate.csv").iloc[0]
    message_content = "Test message"
    forum_source = topic_row['FORUM_SOURCE']

    # Mock session instance and os variables
    with patch.dict('os.environ', {
        f"{forum_source}_URL": "https://fakeforum.com",
        f"{forum_source}_USERNAME": "fake_user",
        f"{forum_source}_PASSWORD": "fake_pass"
    }),patch("message_actions.requests.Session") as mock_session:
        mock_sess_instance = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_sess_instance

        # Mock login page GET
        login_html = '''
        <input name="sid" value="sid123">
        <input name="form_token" value="token123">
        <input name="creation_time" value="time123">
        <input name="topic_cur_post_id" value="topic123">
        Déconnexion
        '''
        mock_sess_instance.get.return_value.text = login_html

        # Mock POST responses (login and message post)
        mock_sess_instance.post.return_value.text = "Déconnexion"

        # Should not raise SystemExit
        message_actions.post_message(topic_row, message_content)

def test_post_message():

    topic_row = pd.read_csv("materials/qTopics_Calculate.csv").iloc[0]
    message_content = "Hello, forum!"

    with patch("message_actions.post_message_bi") as mock_post_message_bi:
        message_actions.post_message(topic_row, message_content)
        mock_post_message_bi.assert_called_once_with(topic_row, message_content)

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_extract_messages_from_topic))
    test_suite.addTest(unittest.FunctionTestCase(test_extract_messages))
    test_suite.addTest(unittest.FunctionTestCase(test_get_list_topics_from_need))
    test_suite.addTest(unittest.FunctionTestCase(test_get_extraction_time_range))
    test_suite.addTest(unittest.FunctionTestCase(test_post_message_bi))
    test_suite.addTest(unittest.FunctionTestCase(test_post_message))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)