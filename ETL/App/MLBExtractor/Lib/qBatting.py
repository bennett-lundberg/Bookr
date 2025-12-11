
import pybaseball
import pandas as pd


class Batting:

    @staticmethod
    def seasonal():

        dat = pd.DataFrame()
        for season in range(1950, 2026, 1):
            iter = pybaseball.batting_stats(season)
            dat = pd.concat([dat, iter], axis = 1)

        return dat
    
    
