'''
This tests file concern all functions in the get_messages_details_bi.
It units test the unexpected path for each function, which return exception
'''
import unittest
import pandas as pd
from assertRaises import assertRaises
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import get_messages_details_bi as messages_details


def test_translate_invalid_date_string():
    # this test the function translate_french_special_date_to_english with unexpected date
    date_str = "foobar 99 xxx 2020 25:61"
    translated = messages_details.translate_french_special_date_to_english(date_str)
    assert translated == date_str

def test_transform_invalid_date_format():
    # this test the function translate_french_special_date_to_english with no time
    invalid_date = "Sun 01 Apr 2020"  # Missing time
    assertRaises(ValueError, messages_details.transform_forum_time_to_datetime, invalid_date)

def test_get_users_empty_html():
    # this test the function get_users_bi without users
    assert messages_details.get_users_bi("") == []

def test_get_users_malformed_html():
    # this test the function get_users_bi without tags
    html = "<span class='responsive-hide'><a>Missing class</a></span>"
    assert messages_details.get_users_bi(html) == []

def test_get_ids_malformed_ids():
    # this test the function get_ids_bi with id not int
    html = "<dl class='postprofile' id='badprefix123'></dl>"
    assertRaises(ValueError, messages_details.get_ids_bi, html)

def test_get_ids_empty():
    # this test the function get_ids_bi with empty id
    assert messages_details.get_ids_bi("") == []

def test_get_creationtimes_invalid_date():
    # this test the function get_creationtimes_bi with invalid date
    html = "<time>Invalid Date</time>"
    assertRaises(ValueError, messages_details.get_creationtimes_bi, html)

def test_content_outer_blockquote_nested():
    # this test the function get_contents_outerblockquote_bi with a lot of nested blocks
    html = "<div class='content'><blockquote><blockquote>Nested</blockquote>Outer</blockquote></div>"
    expected = "<blockquote>NestedOuter</blockquote>"
    assert messages_details.get_contents_outerblockquote_bi(html)[0] == expected

def test_content_empty_div():
    # this test the function get_contents_outerblockquote_bi with an empty content
    html = "<div class='content'></div>"
    assert messages_details.get_contents_outerblockquote_bi(html)[0] == ""

def test_editiontime_malformed_notice():
    # this test the function get_editiontimes_bi with a malformed date
    html = "<div class='postbody'><div class='notice'>Modifié en dernier par Someone but missing date</div></div>"
    assert messages_details.get_editiontimes_bi(html)[0] is None

def test_editiontime_no_notice():
    # this test the function get_editiontimes_bi with no edition
    html = "<div class='postbody'><div class='something_else'>No edition here</div></div>"
    assert messages_details.get_editiontimes_bi(html)[0] is None

def test_get_messages_details_inconsistent_lengths():
    # this test the function get_messages_details_bi with two users for one message
    html = """
    <span class='responsive-hide'><a class='username'>User1</a></span>
    <dl class='postprofile' id='profile123'></dl>
    <dl class='postprofile' id='profile124'></dl>
    <time>lun. 01 janv. 2024 10:00</time>
    <div class='content'>Hello</div>
    <div class='postbody'><div class='notice'>Modifié en dernier par Admin le lun. 01 janv. 2024 11:00, édité 1 fois.</div></div>
    """

    topic_row = pd.Series({"FORUM_SOURCE": "BI_FORUM", "TOPIC_NUMBER": 42})
    assertRaises(ValueError, messages_details.get_messages_details_bi, html, topic_row, start=0)

def test_get_messages_details_empty_html():
    # this test the function get_messages_details_bi with an empty text
    html = ""
    topic_row = pd.Series({"FORUM_SOURCE": "BI_FORUM", "TOPIC_NUMBER": 42})
    df = messages_details.get_messages_details_bi(html, topic_row, start=0)
    assert isinstance(df, pd.DataFrame)
    assert df.empty

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_translate_invalid_date_string))
    test_suite.addTest(unittest.FunctionTestCase(test_transform_invalid_date_format))
    test_suite.addTest(unittest.FunctionTestCase(test_get_users_empty_html))
    test_suite.addTest(unittest.FunctionTestCase(test_get_users_malformed_html))
    test_suite.addTest(unittest.FunctionTestCase(test_get_ids_malformed_ids))
    test_suite.addTest(unittest.FunctionTestCase(test_get_ids_empty))
    test_suite.addTest(unittest.FunctionTestCase(test_get_creationtimes_invalid_date))
    test_suite.addTest(unittest.FunctionTestCase(test_content_outer_blockquote_nested))
    test_suite.addTest(unittest.FunctionTestCase(test_content_empty_div))
    test_suite.addTest(unittest.FunctionTestCase(test_editiontime_malformed_notice))
    test_suite.addTest(unittest.FunctionTestCase(test_editiontime_no_notice))
    test_suite.addTest(unittest.FunctionTestCase(test_get_messages_details_inconsistent_lengths))
    test_suite.addTest(unittest.FunctionTestCase(test_get_messages_details_empty_html))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)
