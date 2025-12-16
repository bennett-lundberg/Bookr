
from Lib.qBatting import Batting
from Lib.qPitching import Pitching
from Lib.qDimension import DimensionImporter
from Lib.Executor import Executor

import pandas as pd
import json
import os

with open(f'{os.path.dirname(os.path.abspath(__file__))}/Properties/launchSettings.json', 'r') as f:
    args = json.load(f)

def main():

    exit = 0
    Executor.send('Running Extraction for Batting Statistics')
    batting = Batting('2025-07-11')
    battingDaily = batting.getDaily()
    ### Write function to append to table
    battingDaily.to_parquet('M:\\Bennett\\Private\\Bookr\\ETL\\App\\MLBExtractor\\bin\\StagingMLBGameLogBatting.parquet', index = False)
    
    Executor.send('Running Extraction for Pitching Statistics')
    pitching = Pitching('2025-07-11')
    pitchingDaily = pitching.getDaily()
    ### Write function to append to table
    pitchingDaily.to_parquet('M:\\Bennett\\Private\\Bookr\\ETL\\App\\MLBExtractor\\bin\\StagingMLBGameLogPitching.parquet', index = False)

    importer = DimensionImporter(dateEntry = '2025-07-11')
    importer.ImportTeams(f'{os.path.dirname(os.path.abspath(__file__))}/bin/DimMLBTeams.parquet')
    importer.ImportPlayers(f'{os.path.dirname(os.path.abspath(__file__))}/bin/DimMLBPlayers.parquet', battingDaily.personId.values, pitchingDaily.personId.values)


    
if __name__ == '__main__':
    main()