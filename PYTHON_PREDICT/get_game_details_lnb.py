'''
    The purpose of this module is to extract game details coming from the LNB website 
'''   
from bs4 import BeautifulSoup as bs
from re import compile as re_compile
import pandas as pd
import urllib.request as urllib

import config

@config.raise_issue_to_caller(log_filter=lambda args: {})
def get_gameday_lnb(gametext: str, log_print: str) -> str:

    """
        Gets the gameday in a game HTML page
        Args:
            gametext (string) : The HTML text of the game page
            log_print (string): for logging purpose
        Returns:
            the gameday (str)
        Raises:
            Raise the issue to the caller if exception
    """

    # We parse using BeautifulSoup
    soup = bs(gametext, "html.parser")
    gameday_tag = soup.find('span', class_='championship-day')
    if gameday_tag:
        gameday = gameday_tag.get_text(strip=True)
    else:
        raise ValueError(f"GameDay could not be parsed {log_print}")
    return gameday

@config.raise_issue_to_caller(log_filter=lambda args: {})
def get_teams_lnb(gametext: str, log_print: str) -> list:

    """
        Gets teams in a game HTML page
        Args:
            gametext (string) : The HTML text of the game page
            log_print (string): for logging purpose
        Returns:
            the list of teams

        Raises:
            Raise the issue to the caller if:
            - exception
            - if there are more or less than 2 teams parsed
    
    """
    # We parse using BeautifulSoup
    soup = bs(gametext, "html.parser")
    teams_tag = soup.find_all('div', class_='team-name display-on-mobile')
    if teams_tag:
        teams = [t.text.strip() for t in teams_tag]
        if len(teams) != 2:
            raise ValueError(f"There are not two teams {log_print} ")
    else:
        raise ValueError(f"Teams could not be parsed {log_print}")
    return teams

@config.raise_issue_to_caller(log_filter=lambda args: {})
def get_date_lnb(gametext: str, log_print: str) -> str:

    """
        Gets date of the game in a game HTML page
        Args:
            gametext (string) : The HTML text of the game page
            log_print (string): for logging purpose
        Returns:
            the date we transform on format YYYY-MM-DD
        Raises:
            Raise the issue to the caller if exception
    """
    # We parse using BeautifulSoup
    soup = bs(gametext, "html.parser")
    date_tag = soup.find('span', class_='date')
    if date_tag:
        date = date_tag.text.strip()
    else:
        raise ValueError(f"Date could not be parsed {log_print}")
    
    month_mapping = {
        "janvier": 1, "février": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6,
        "juillet": 7, "août": 8, "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12
    }
    parts = date.split()
    day = int(parts[0])
    month = month_mapping[parts[1].lower()]
    year = int(parts[2])

    date = f"{year:04d}-{month:02d}-{day:02d}"

    return date

@config.raise_issue_to_caller(log_filter=lambda args: {})
def get_time_lnb(gametext: str) -> str:
    
    """
        Gets time in a game HTML page
        If the game is already played there is no time
        Args:
            gametext (string) : The HTML text of the game page
        Returns:
            the time of the game (str) if exists else None
        Raises:
            Raise the issue to the caller if exception
    """
    
    # We parse using BeautifulSoup and regex
    soup = bs(gametext, "html.parser")
    text = soup.get_text()
    time_pattern = re_compile(r"\b\d{1,2}:\d{2}\b")
    times = time_pattern.search(text)
    if times:
        gametime = times.group()
    else:
        gametime = None
    return gametime

@config.raise_issue_to_caller(log_filter=lambda args: {})
def get_scores_lnb(gametext: str,log_print: str) -> list:

    """
        Gets scores in a game HTML page
        Args:
            gametext (string) : The HTML text of the game page
            log_print (string): for logging purpose
        Returns:
            the list of scores (int)
        Raises:
            Raise the issue to the caller if:
             - exception
             - there is more or less than two scores
    """
    # We parse using BeautifulSoup
    soup = bs(gametext, "html.parser")
    scores_tag = soup.find_all('span', class_='score-team')
    if scores_tag:
        scores = [int(t.text) for t in scores_tag]
        if len(scores) != 2:
            raise ValueError(f"There are not two scores {log_print}")
    else:
        raise ValueError(f"Scores could not be parsed {log_print}")
    return scores

@config.retry_function(log_filter=lambda args: {k: args[k] for k in ('game_url',)})
def get_game_details_lnb(game_url: str, game_row: pd.Series) -> pd.DataFrame:

    """
        Gets all details from a game
        Args:
            game_url (string) : The url page of the game
            game_row (series): basic details about the game
        Returns:
            the dataframe corresponding to the details extracted
        Raises:
            Retry 3 times and exits the program if error with url extraction (using retry decorator)
    """
    
    #we calculate for logging purpose
    log_print = f"game source: {game_row['COMPETITION_SOURCE']} / game id: {game_row['GAME_SOURCE_ID']}"

    # We open the url
    response = urllib.urlopen(game_url, timeout=config.game_extraction_wait_time)
    gametext = response.read()
    gametext = gametext.decode('utf-8')

    # We get the all details
    gameday = get_gameday_lnb(gametext,log_print)
    team_home , team_away = get_teams_lnb(gametext,log_print)
    date = get_date_lnb(gametext,log_print)
    time1 = get_time_lnb(gametext)
    score_home , score_away = get_scores_lnb(gametext,log_print)
    
    df_game_infos = pd.DataFrame({
        'COMPETITION_SOURCE': [game_row['COMPETITION_SOURCE']],
        'COMPETITION_ID': [game_row['COMPETITION_ID']],
        'SEASON_ID': [game_row['SEASON_ID']],
        'GAMEDAY': [gameday],
        'DATE_GAME_LOCAL': [date],
        'TIME_GAME_LOCAL': [time1],
        'TEAM_HOME': [team_home],
        'SCORE_HOME': [score_home],
        'TEAM_AWAY': [team_away],
        'SCORE_AWAY': [score_away],
        'GAME_SOURCE_ID': [game_row['GAME_SOURCE_ID']]
    })
    return df_game_infos