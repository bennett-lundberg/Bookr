import pandas as pd
import statsapi

from datetime import date, timedelta


class DimensionImporter:

    def __init__(self, dateEntry = None):

        if dateEntry is None:
            self.evaluationDate = date.today() - timedelta(days = 1) 
        else:
            self.evaluationDate = dateEntry


    ### Class method for checking the games at the evaluation date
    #### Pull all Team IDs and Team Names
    #### If Team ID Team Name does not exist in source table then insert that row to the table
    @classmethod
    def ImportTeams(self, table: str):

        games = statsapi.schedule(date = self.evaluationDate)


    ### Class method for checking the players at the evaluation date
    #### Pull all Player IDs and Player Names
    #### If Player ID Player Name does not exist in source table then insert that row to the table
    @classmethod
    def ImportPlayers(self, table: str):

        games = statsapi.schedule(date = self.evaluationDate)