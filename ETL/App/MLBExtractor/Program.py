
from Lib.qBatting import Batting
from Lib.qPitching import Pitching

import pandas as pd
import json
import os
import pyarrow

with open(f'{os.path.dirname(os.path.abspath(__file__))}\\Properties\\launchSettings.json', 'r') as f:
    args = json.load(f)

def main(args = args):

    ### Read arguments
    localContainer = args['container']

    ### Source Data; Create DataFrame
    battingExtractor = Batting()
    battingSeasonal = battingExtractor.getSeasonal()

    ## Pitching
    pitchingExtractor = Pitching()
    pitchingSeasonal - picthingExtractor.getSeasonal()

    ### Assign pandas objects to table
    tablePairs = [
        (battingSeasonal, 'StagingMLBOffensiveSeasonalLog'),
        (pitchingSeasonal, 'StagingMLBPitchingSeasonalLog')
    ]

    ### Iterate through table pairs; write to file
    for tuple in tablePairs:

        dat: pd.DataFrame = tuple[0]
        dat.to_parquet(f'{localContainer}{tuple[1]}.parquet', engine = 'pyarrow')
    
    
if __name__ == '__main__':
    main()