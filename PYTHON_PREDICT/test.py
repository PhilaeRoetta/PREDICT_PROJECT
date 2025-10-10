import requests
import pandas as p

url = "https://api-prod.lnb.fr/match/getCalendar"
payload = {
    "competition_external_id": 288,
    "start_date": "2000-01-01",
    "end_date": "2999-12-31"
}
headers = {
    "Content-Type": "application/json"
}

response = requests.post(url, json=payload, headers=headers)
print(response)
games_per_day = response.json()['data']

rows = []
for day in games_per_day:
    games = day['data']
    for game in games:
        match_gameday = game.get('round_description')
        match_status = game.get('match_status')
        match_time_utc = game.get('match_time_utc')
        match_timezone = game.get('timezone')
        match_old_id = game.get('id')
        match_id = game.get('match_id')
        match_teams = game.get('teams')
        match_team1 = match_teams[0].get('team_name')
        match_score1 = match_teams[0].get('score_string')
        match_team2 = match_teams[1].get('team_name')
        match_score2 = match_teams[1].get('score_string')
        rows.append({
            "OLD_ID": match_old_id,
            "ID": match_id,
            "STATUS": match_status,
            "GAMEDAY": match_gameday,
            "TEAM1": match_team1,
            "TEAM2": match_team2,
            "DATETIME_UTC": match_time_utc,
            "TIMEZONE": match_timezone,
            "SCORE1": match_score1,
            "SCORE2": match_score2
        })

# Create the DataFrame
df = p.DataFrame(rows)
print(df)