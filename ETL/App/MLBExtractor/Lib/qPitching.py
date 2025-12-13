
import pybaseball
import statsapi
import pandas as pd
from datetime import date, timedelta


class Pitching:

    def __init__(self, dateEntry = None):

        if dateEntry is None:
            self.evaluationDate = date.today() - timedelta(days = 1) 
        else:
            self.evaluationDate = dateEntry
    
    def getDaily(self):

        games = statsapi.schedule(date = self.evaluationDate)

        Pitchers = pd.DataFrame()
        for game in games:

            gameID = game['game_id']
            gameDate = game['game_date']
            awayTeam = game['away_id']
            homeTeam = game['home_id']
            
            ### Pull Box Score for game iteration
            box = statsapi.boxscore_data(gameID)

            ### Pull Data for Home Players
            homePlayers = pd.DataFrame(box['homePitchers']).iloc[1:]
            homePlayers['TeamID'] = homeTeam
            homePlayers['Date'] = gameDate
            homePlayers['IsHome'] = 1
            homePlayers['OpposingTeamID'] = awayTeam

            Pitchers = pd.concat([Pitchers, homePlayers])
            del homePlayers

            ### Pull Data for Away Players
            awayPlayers = pd.DataFrame(box['awayPitchers']).iloc[1:]
            awayPlayers['TeamID'] = awayTeam
            awayPlayers['Date'] = gameDate
            awayPlayers['IsHome'] = 0
            awayPlayers['OpposingTeamID'] = homeTeam
            Pitchers = pd.concat([Pitchers, awayPlayers])
            del awayPlayers 

        return Pitchers

