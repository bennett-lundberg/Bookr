
from Lib.Executor import Executor
from Lib.Engine import TableWriter
from BatchGameImporter import BatchGameImporter

import argparse
import multiprocessing
import statsapi
import json
import os

with open(f'{os.path.dirname(os.path.abspath(__file__))}/Properties/launchSettings.json', 'r') as f:
    args = json.load(f)

def main(args = args):

    #evaluationDate: str = args['date']
    from datetime import date, timedelta
    startDate = date(2021, 5, 30)
    endDate = date(2024, 12, 1)
    current = startDate
    while current <= endDate:
        evaluationDate = current.strftime("%Y-%m-%d")
        print(f"Evaluation date: {evaluationDate}")

        gameDict = statsapi.schedule(evaluationDate)

        if not gameDict:
            print("No games found for the given date. Exiting.")
            #return

        with multiprocessing.Pool() as pool:
            try:
                pool.map(runJob, gameDict)
            except Exception as e:
                import traceback
                print("Exception during pool.map:", e)
                traceback.print_exc()

        current += timedelta(days=1)

def runJob(payload: dict):
    try:
        batchImporter = BatchGameImporter(gameData = payload, args = args['credentials'])
        batchImporter.ImportGame()
        batchImporter.ImportTeams()
        batchImporter.ImportVenues()
        batchImporter.DailyBattingStats()
        batchImporter.DailyPitchingStats()
        batchImporter.ImportPlayers()
    except Exception as e:
        import traceback
        print(f"Exception in runJob for payload: {payload}")
        traceback.print_exc()

if __name__ == '__main__':
    main()