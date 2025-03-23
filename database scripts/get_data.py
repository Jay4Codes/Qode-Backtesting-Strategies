from maticalgos import historical
import datetime

ma = historical("jay4emails@gmail.com")

ma.login("919252")

dates = ma.get_dates("nifty")

dates = [date for date in dates if date.startswith("2024")]

dates = [datetime.datetime.strptime(date, "%Y%m%d").date() for date in dates]

for date in dates:
    data = ma.get_data("nifty", date)
    data.to_csv(f"nifty_{date.strftime('%Y%m%d')}.csv")
