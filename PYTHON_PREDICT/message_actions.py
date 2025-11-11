''' 
    The purpose of this module is to interact with forums website by:
    - getting the scope of topics: messages we want to extract
    - then extract the messages, and their details
    - and / or post messages calculated by the program
'''

import logging
logging.basicConfig(level=logging.INFO)
import warnings
warnings.filterwarnings("ignore")
import os
import ssl
import pandas as pd
import urllib.request as urllib
import pytz
import requests
from bs4 import BeautifulSoup as bs
import time
from typing import Tuple

from get_messages_details_bi import get_messages_details_bi
import config
from file_actions import create_csv
import snowflake_actions as snowflakeA
import sql_queries as sqlQ

messages_info_functions = {
    "BI": get_messages_details_bi
}

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('topic_row',)})
@config.retry_function(log_filter=lambda args: {k: args[k] for k in ('topic_row',)})
def extract_messages_from_topic(topic_row: pd.Series,ts_message_extract_min_utc: pd.Timestamp,ts_message_extract_max_utc: pd.Timestamp) -> pd.DataFrame | None:
        
    """
        Gets all messages in the time range of a topic
        Args:
            topic_row (serie) : Contains basic info about the topic
            ts_message_extract_min_utc (timestamp utc): the min of the range of time for messages extraction
            ts_message_extract_max_utc (timestamp utc): the max of the range of time for messages extraction
        Returns:
            dataframe: contains all message in the time range from this topic, or None if there are no topics
        Raises:
            Retry 3 times and exits the program if error with url extraction (using retry decorator)
    """

    #we get min and max time range in the local time of the topic
    local_timezone = pytz.timezone(topic_row['FORUM_TIMEZONE'])
    ts_message_extract_min_local = pytz.UTC.localize(ts_message_extract_min_utc).astimezone(local_timezone).strftime("%Y-%m-%d %H:%M:%S")
    ts_message_extract_max_local = pytz.UTC.localize(ts_message_extract_max_utc).astimezone(local_timezone).strftime("%Y-%m-%d %H:%M:%S")
    
    #Initial parameters
    forum_url = os.getenv(topic_row['FORUM_SOURCE'] + '_URL')
    start = 0
    seen_message_ids = set()
    topic_messages = []
    # we overwrite security for extracting from the website
    ssl_context = ssl._create_unverified_context()
    
    while True:
        #we extract from the website page
        page_url = f"{forum_url}/viewtopic.php?t={topic_row['TOPIC_NUMBER']}&start={start}"
        logging.info(f"MESSAGES -> EXTRACTING FORUM {topic_row['FORUM_SOURCE']} / TOPIC {topic_row['TOPIC_NUMBER']} / MESSAGES {start+1} -> X [START] ")
        response = urllib.urlopen(page_url, context=ssl_context)
        messagetext = response.read()
        messagetext = messagetext.decode('utf-8') 
        #we get all messages from the page
        get_messages_infos = messages_info_functions.get(topic_row['FORUM_SOURCE'])
        df = get_messages_infos(messagetext, topic_row, start)
        current_ids = set(df['MESSAGE_FORUM_ID'])
        #if we already saw messages we stop here
        if current_ids.intersection(seen_message_ids):
            logging.info(f"MESSAGES -> BREAKING TOPIC {topic_row['TOPIC_NUMBER']}")
            break
        #else we add those new message of the list of seen messages
        seen_message_ids.update(current_ids)
        start += len(df)

        # Filter on timestamp range (either creation date with none edition or edition date)
        creation_with_editionNone = df['CREATION_TIME_LOCAL'].between(ts_message_extract_min_local, ts_message_extract_max_local) & df['EDITION_TIME_LOCAL'].isna()
        edition = df['EDITION_TIME_LOCAL'].between(ts_message_extract_min_local, ts_message_extract_max_local)
        df_filtered = df[creation_with_editionNone | edition]

        if not df_filtered.empty:
            topic_messages.append(df_filtered)
        
        logging.info(f"MESSAGES -> EXTRACTING FORUM {topic_row['FORUM_SOURCE']} / TOPIC {topic_row['TOPIC_NUMBER']} / MESSAGES X -> {start} [DONE] ")
    return pd.concat(topic_messages, ignore_index=True) if topic_messages else None

@config.exit_program(log_filter=lambda args: {})
def extract_messages(sr_snowflake_account: pd.Series, sr_output_need: pd.Series) -> Tuple[pd.DataFrame, pd.Timestamp]:

    """
        Gets all messages we need to extract from the list of topics and time range
        Args:
            sr_snowflake_account (series - one row) : Contains snowflake parameters to run the query
            sr_output_need (series - one row): the output need file to get the range minimum of extraction
        Returns:
            - dataframe: contains all messages needed
            - datetime of extraction of messages
        Raises:
            Exits the program if error running the function (using decorator)
    """

    logging.info(f"MESSAGES -> EXTRACTING MESSAGES [START]")

    # we get the scope of topics and datetime range we need to extract messages
    topics_scope_id = get_list_topics_from_need(sr_snowflake_account, sr_output_need) 
    ts_message_extract_min_utc,ts_message_extract_max_utc = get_extraction_time_range(sr_snowflake_account, sr_output_need) 
    
    df_messages = pd.DataFrame(columns=['FORUM_SOURCE','TOPIC_NUMBER','USER',
                                            'MESSAGE_FORUM_ID','CREATION_TIME_LOCAL',
                                            'EDITION_TIME_LOCAL','MESSAGE_CONTENT'])

    # if the min >= max then we don't need to extract messages, there won't be any
    if ts_message_extract_min_utc >= ts_message_extract_max_utc:
        logging.info(f"MESSAGES -> no need to extract messages")
    else:
        # We parallelize the extraction of each topic    
        messages_args = [(row,ts_message_extract_min_utc,ts_message_extract_max_utc) 
                    for _, row in topics_scope_id.iterrows()]
        results = config.multithreading_run(extract_messages_from_topic, messages_args)
        messages_extracted = [r for r in results if r is not None]

        if len(messages_extracted) > 0:
            df_messages = pd.concat(messages_extracted, ignore_index=True)

    create_csv(os.path.join(config.TMPF, 'message_check.csv'), df_messages, config.message_encapsulated)
    logging.info(f"MESSAGES -> EXTRACTING MESSAGES [DONE]")
    return df_messages, ts_message_extract_max_utc

@config.exit_program(log_filter=lambda args: {})
def get_list_topics_from_need(sr_snowflake_account: pd.Series,sr_output_need: pd.Series) -> pd.DataFrame:

    """
        Gets list of topics where we extract messages
        Args:
            sr_snowflake_account (series - one row) : Contains snowflake paramaters to run the query
            sr_output_need (series - one row): the output_need we process - we extract only topics from its season
        Returns:
            dataframe: contains all topics number 
        Raises:
            Exits the program if error running the function (using decorator)
    """

    logging.info(f"GAME -> GETTING TOPICS LIST [START]")

    topics_scope_number = snowflakeA.snowflake_execute(sr_snowflake_account,sqlQ.message_actions_qTopics_to_extract,(sr_output_need['SEASON_ID'],))

    logging.info(f"Number of topics to extract: {len(topics_scope_number)}")
    logging.info(f"GAME -> GETTING TOPICS LIST [DONE]")
    return topics_scope_number

@config.exit_program(log_filter=lambda args: {})
def get_extraction_time_range(sr_snowflake_account: pd.Series, sr_output_need: pd.Series) -> Tuple[pd.Timestamp, pd.Timestamp]:

    """
        Gets the time range for extraction of messages
        - min using check date detail from sr_output_need
        - max using the end time of the gameday on gameday snowflake table
        Args:
            sr_snowflake_account (series - one row) : Contains snowflake paramaters to run the query
            sr_output_need (series - one row): the output need file to get the range minimum of extraction
        Returns:
            min & max datetime of extraction in utc
        Raises:
            Exits the program if error running the function (using decorator)
    """

    # we extract the min with check time from need, we will limit extraction using it for range utc
    ts_message_extract_min_utc = pd.to_datetime(sr_output_need['LAST_MESSAGE_CHECK_TS_UTC'], errors='coerce')
    
    # we extract the max with Snowflake
    df_time_max = snowflakeA.snowflake_execute(sr_snowflake_account,sqlQ.message_actions_qGameDayEnd,(sr_output_need['SEASON_ID'],sr_output_need['GAMEDAY']))
    ts_message_extract_max_utc = pd.to_datetime(f"{df_time_max.at[0, 'END_DATE_UTC']} {df_time_max.at[0, 'END_TIME_UTC']}")
    
    #we get the current time utc
    current_time_utc = pd.Timestamp.utcnow().replace(microsecond=0).tz_localize(None)

    # if current_time_utc is before ts_message_extract_max_utc we replace ts_message_extract_max_utc
    ts_message_extract_max_utc = min(ts_message_extract_max_utc,current_time_utc)
    logging.info(f"MESSAGES -> Extracting between utc time {ts_message_extract_min_utc} and {ts_message_extract_max_utc} - extracting at {current_time_utc}")

    return ts_message_extract_min_utc,ts_message_extract_max_utc

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('topic_row',)})
@config.retry_function(log_filter=lambda args: {k: args[k] for k in ('topic_row',)})
def post_message_bi(topic_row: pd.Series, message_content: str):

    '''
        Posts a message on the "BI" forum
        Inputs:
            topic_row (series - one row) details about the topic to know where to post
            message_content (str): message to post
        Raises:
            If it didn't work to post, we'll retry 3 times then exit program
    '''

    #We will try to post for a specific time max
    time_max = config.time_message_wait
    logging.info(f"MESSAGES -> POSTING OUTPUT ON {topic_row['FORUM_SOURCE']} / TOPIC {topic_row['TOPIC_NUMBER']} - Trying for {time_max} secs [START] ")
        
    begin_time = time.time()
    
    forum_url = os.getenv(topic_row['FORUM_SOURCE'] + '_URL')
    post_url = forum_url +"/posting.php?mode=reply&t="+ str(topic_row['TOPIC_NUMBER'])
    login_url = forum_url +'/ucp.php?mode=login'

    connect_dict = {
        "username": os.getenv(topic_row['FORUM_SOURCE']+'_USERNAME'),
        "password": os.getenv(topic_row['FORUM_SOURCE']+'_PASSWORD'),
        "autologin": "on", 
        "login": "Connexion" 
    }

    messagepost_dict = {
        'subject': 'Re:',
        'message': message_content,
        'post': 'Envoyer'
    }
    message_post_worked = False

    while (message_post_worked == False and time.time() - begin_time <= time_max):
        with requests.Session() as session:
            #We get the login page
            connect_get = session.get(login_url, verify=False)
            soup = bs(connect_get.text, "html.parser")
            connect_dict['sid'] = soup.find("input", {"name": "sid"})["value"]
            connect_dict['form_token'] = soup.find("input", {"name": "form_token"})["value"]
            connect_dict['creation_time'] = soup.find("input", {"name": "creation_time"})["value"]        

            # We post the dictionary to login
            connect_post = session.post(login_url, data=connect_dict, verify=False)
            connect_post_worked = ('Déconnexion' in connect_post.text)
            if connect_post_worked:
                reply_get = session.get(post_url, verify=False)
                reply_get_worked = ('Déconnexion' in reply_get.text)
                if reply_get_worked:
                    # if it worked we post the dictionary to post the message
                    soup = bs(reply_get.text, "html.parser")
                    messagepost_dict['sid'] = connect_dict['sid']
                    messagepost_dict['form_token'] = soup.find("input", {"name": "form_token"})["value"]
                    messagepost_dict['creation_time'] = soup.find("input", {"name": "creation_time"})["value"]
                    messagepost_dict['topic_cur_post_id'] = soup.find("input", {"name": "topic_cur_post_id"})["value"]
                    message_post  = session.post(post_url, data=messagepost_dict, verify=False)   
                    message_post_worked = not('formulaire' in message_post.text)
                else:
                    #otherwise we wait one second and retry
                    time.sleep(1)

    if (message_post_worked == False):
        raise ValueError(f"{topic_row['FORUM_SOURCE']} / TOPIC {topic_row['TOPIC_NUMBER']} NOT POSTED -> Time expiration")
    else:
        logging.info(f"MESSAGES -> POSTING OUTPUT ON {topic_row['FORUM_SOURCE']} / TOPIC {topic_row['TOPIC_NUMBER']} [DONE] ")

@config.exit_program(log_filter=lambda args: {k: args[k] for k in ('topic_row',) })       
def post_message(topic_row: pd.Series, message_content: str):

    '''
        Calls the function to run according to the forum source
        in order to post a message 
        Inputs:
            topic_row (series - one row) to get the forum source
            message_content (str) the message to post
        Raises:
            Exits the program if error running the function (using decorator)
    '''
    messages_post_functions = {
        "BI": post_message_bi
    }

    post_function = messages_post_functions.get(topic_row['FORUM_SOURCE'])
    post_function(topic_row,message_content)