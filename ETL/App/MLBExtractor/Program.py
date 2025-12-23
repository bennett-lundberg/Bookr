
from Lib.qBatting import Batting
from Lib.qPitching import Pitching
from Lib.qDimension import DimensionImporter
from Lib.Executor import Executor
from Lib.Engine import TableWriter

import argparse
import json
import os

with open(f'{os.path.dirname(os.path.abspath(__file__))}/Properties/launchSettings.json', 'r') as f:
    args = json.load(f)

def main(date):

    BookrDev = TableWriter(args)

    Executor.send('Running Extraction for Batting Statistics')
    batting = Batting(date)
    battingDaily = batting.getDaily()
    if battingDaily is not None and len(battingDaily) > 0:
        for _, row in battingDaily.iterrows(): 
            formatted = [] 
            for val in row.values: 
                if isinstance(val, str): 
                    formatted.append(f"'{val}'") # wrap strings in single quotes 
                else: 
                    formatted.append(str(val)) # leave numbers as-is 
            iter = ",".join(formatted)

            try: BookrDev.run(f"INSERT INTO MLB.StagingGameLogBatting SELECT {iter}")
            except: continue
    
    Executor.send('Running Extraction for Pitching Statistics')
    pitching = Pitching(date)
    pitchingDaily = pitching.getDaily()
    if pitchingDaily is not None and len(pitchingDaily) > 0:
        for _, row in pitchingDaily.iterrows(): 
            formatted = [] 
            for val in row.values: 
                if isinstance(val, str): 
                    formatted.append(f"'{val}'")
                else: 
                    formatted.append(str(val))
            iter = ",".join(formatted)

            try: BookrDev.run(f"INSERT INTO MLB.StagingGameLogPitching SELECT {iter}")
            except: continue

    if pitchingDaily is not None and len(pitchingDaily) > 0 and battingDaily is not None and len(battingDaily) > 0:
        importer = DimensionImporter(dateEntry = date)
        importer.ImportTeams()
        importer.ImportPlayers(battingDaily.PlayerID.values, pitchingDaily.PlayerID.values)
        importer.ImportGames()
        importer.ImportVenues()

    
if __name__ == '__main__':
    main()