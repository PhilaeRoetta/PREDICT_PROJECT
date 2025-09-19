'''
This tests file concern all functions in the get_messages_details_bi.
It units test the happy path for each function
'''

import unittest
from datetime import datetime
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import get_messages_details_bi as messages_details

#example of this page of messages: https://basketinforum.com/viewtopic.php?t=108506 - copied in messages_html.txt
with open("materials/messages_html.txt", 'r', encoding='utf-8') as file:
        SAMPLE_HTML = file.read() 


def test_translate_french_special_date_to_english():
    # this test the function translate_french_special_date_to_english
    fr_date = "mar. 07 janv. 2025 9:54"
    expected = "Tue 07 Jan 2025 9:54"
    result = messages_details.translate_french_special_date_to_english(fr_date)
    assert result == expected

def test_transform_forum_time_to_datetime():
    # this test the function transform_forum_time_to_datetime
    date_str = "Mon 01 Jan 2025 9:08"
    result = messages_details.transform_forum_time_to_datetime(date_str)
    assert isinstance(result, datetime)
    assert result.year == 2025 and result.month == 1 and result.day == 1
    assert result.hour == 9 and result.minute == 8

def test_get_users_bi():
    # this test the function get_users_bi
    expected = ['fabdu92', 'fabdu92', 'fabdu92', 'Novice24', 'lampero', 
                     'fabdu92', 'RICO 29', 'fanyoda', 'Jaco11', 'fabdu92', 
                     'Flom27', 'fabdu92', 'fanyoda', 'fabdu92', 'lampero']
    
    users = messages_details.get_users_bi(SAMPLE_HTML)
    assert users == expected

def test_get_ids_bi():
    # this test the function get_ids_bi
    expected = [3855011, 3855012, 3855013, 3855017, 3855286, 
                     3855300, 3855426, 3855555, 3855575, 3856595, 
                     3856694, 3856863, 3856962, 3858027, 3858048]
    
    ids = messages_details.get_ids_bi(SAMPLE_HTML)
    assert ids == expected

def test_get_creationtimes_bi():
    # this test the function get_creationtimes_bi

    expected = [datetime(2025, 1, 7, 9, 54), 
                datetime(2025, 1, 7, 9, 55), 
                datetime(2025, 1, 7, 9, 56), 
                datetime(2025, 1, 7, 10, 53), 
                datetime(2025, 1, 8, 11, 35), 
                datetime(2025, 1, 8, 12, 17), 
                datetime(2025, 1, 8, 22, 59), 
                datetime(2025, 1, 9, 19, 43), 
                datetime(2025, 1, 9, 21, 6), 
                datetime(2025, 1, 13, 18, 47), 
                datetime(2025, 1, 14, 12, 59), 
                datetime(2025, 1, 15, 14, 33), 
                datetime(2025, 1, 15, 21, 55), 
                datetime(2025, 1, 20, 9, 42), 
                datetime(2025, 1, 20, 12, 39)]

    creation_times = messages_details.get_creationtimes_bi(SAMPLE_HTML)
    assert creation_times == expected

def test_get_contents_outerblockquote_bi():
    contents = messages_details.get_contents_outerblockquote_bi(SAMPLE_HTML)
    assert len(contents) == 15
    assert "<blockquote>" in contents[5]
    assert "\n" not in contents[0]


def test_get_editiontimes_bi():

    expected = [datetime(2025, 6, 15, 18, 17), 
                datetime(2025, 1, 8, 12, 18), 
                None, 
                None, 
                datetime(2025, 1, 10, 11, 20), 
                datetime(2025, 1, 18, 9, 42), 
                None, None, None, None, None, None, None, None, None]
    edition_times =  messages_details.get_editiontimes_bi(SAMPLE_HTML)
    assert edition_times == expected

def test_get_messages_details_bi():

    TOPIC_ROW = {
    'FORUM_SOURCE': 'BI',
    'FORUM_TIMEZONE': 'Europe/Paris',
    'TOPIC_NUMBER': 108504,
    }
    
    df = messages_details.get_messages_details_bi(SAMPLE_HTML, TOPIC_ROW, start=0)
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) == {
        'FORUM_SOURCE', 'TOPIC_NUMBER', 'USER', 'MESSAGE_FORUM_ID',
        'CREATION_TIME_LOCAL', 'EDITION_TIME_LOCAL', 'MESSAGE_CONTENT'
    }
    assert len(df) == 15
    assert all(df['FORUM_SOURCE'] == 'BI')
    assert all(df['TOPIC_NUMBER'] == 108504)


if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_translate_french_special_date_to_english))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_forum_time_to_datetime))
    test_suite.addTest(unittest.FunctionTestCase(test_get_users_bi))
    test_suite.addTest(unittest.FunctionTestCase(test_get_ids_bi))
    test_suite.addTest(unittest.FunctionTestCase(test_get_creationtimes_bi))
    test_suite.addTest(unittest.FunctionTestCase(test_get_contents_outerblockquote_bi))
    test_suite.addTest(unittest.FunctionTestCase(test_get_editiontimes_bi))
    test_suite.addTest(unittest.FunctionTestCase(test_get_messages_details_bi))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
