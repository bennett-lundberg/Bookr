
from Lib.qBatting import Batting

import pandas as pd


def main():

    extractor = Batting()
    seasonal = extractor.seasonal()
    print(seasonal)
    
if __name__ == '__main__':
    main()