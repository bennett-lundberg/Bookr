
from Lib.qBatting import Batting
from Lib.qPitching import Pitching

import pandas as pd
import json
import os
import pyarrow

with open(f'{os.path.dirname(os.path.abspath(__file__))}/Properties/launchSettings.json', 'r') as f:
    args = json.load(f)

def main():

    batting = Batting('2025-07-11')
    dat = batting.getDaily()
    print(dat)
    
    batting = Pitching('2025-07-11')
    dat = batting.getDaily()
    print(dat)

    
if __name__ == '__main__':
    main()