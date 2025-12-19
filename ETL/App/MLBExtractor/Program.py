
from Lib.qBatting import Batting
from Lib.qPitching import Pitching
from Lib.qDimension import DimensionImporter
from Lib.Executor import Executor
from Lib.Engine import TableWriter

import pandas as pd
import json
import os

with open(f'{os.path.dirname(os.path.abspath(__file__))}/Properties/launchSettings.json', 'r') as f:
    args = json.load(f)

def main(args = args):

    BookrDev = TableWriter(args)

    exit = 0
    Executor.send('Running Extraction for Batting Statistics')
    batting = Batting('2025-07-11')
    battingDaily = batting.getDaily()
    for _, row in battingDaily.iterrows(): 
        formatted = [] 
        for val in row.values: 
            if isinstance(val, str): 
                formatted.append(f"'{val}'") # wrap strings in single quotes 
            else: 
                formatted.append(str(val)) # leave numbers as-is 
        iter = ",".join(formatted)

        try: BookrDev.run(f"INSERT INTO StagingMLBGameLogBatting SELECT {iter}")
        except: continue
    
    Executor.send('Running Extraction for Pitching Statistics')
    pitching = Pitching('2025-07-11')
    pitchingDaily = pitching.getDaily()
    for _, row in pitchingDaily.iterrows(): 
        formatted = [] 
        for val in row.values: 
            if isinstance(val, str): 
                formatted.append(f"'{val}'")
            else: 
                formatted.append(str(val))
        iter = ",".join(formatted)

        try: BookrDev.run(f"INSERT INTO StagingMLBGameLogPitching SELECT {iter}")
        except: continue

    importer = DimensionImporter(dateEntry = '2025-07-11')
    importer.ImportTeams()
    importer.ImportPlayers(battingDaily.PlayerID.values, pitchingDaily.PlayerID.values)

    
if __name__ == '__main__':
    main()