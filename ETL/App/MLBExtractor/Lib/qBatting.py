
import pybaseball
import statsapi
import pandas as pd
from datetime import date, timedelta


class Batting:

    def __init__(self, dateEntry = None):

        if dateEntry is None:
            self.evaluationDate = date.today() - timedelta(days = 1) 
        else:
            self.evaluationDate = dateEntry
    
    def getDaily(self):

        games = statsapi.schedule(date = self.evaluationDate)

        Batters = pd.DataFrame()
        for game in games:

            gameID = game['game_id']
            gameDate = game['game_date']
            awayTeam = game['away_name']
            homeTeam = game['home_name']
            
            ### Pull Box Score for game iteration
            box = statsapi.boxscore_data(gameID)

            ### Pull Data for Home Players
            homePlayers = pd.DataFrame(box['homeBatters']).iloc[1:]
            homePlayers['TeamName'] = homeTeam
            homePlayers['Date'] = gameDate
            homePlayers['IsHome'] = 1
            homePlayers['OpposingTeamName'] = awayTeam
            Batters = pd.concat([Batters, homePlayers])
            del homePlayers

            ### Pull Data for Away Players
            awayPlayers = pd.DataFrame(box['awayBatters']).iloc[1:]
            awayPlayers['TeamName'] = awayTeam
            awayPlayers['Date'] = gameDate
            awayPlayers['IsHome'] = 0
            awayPlayers['OpposingTeamName'] = homeTeam
            Batters = pd.concat([Batters, awayPlayers])
            del awayPlayers 

        return Batters

