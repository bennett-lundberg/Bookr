import Program
from datetime import date, timedelta

year = 2025
start = date(year, 1, 1)
end = date(year, 6, 30)

current = start
while current <= end:

    iter = current.strftime("%Y-%m-%d")
    Program.main(iter)
    current += timedelta(days = 1)
    