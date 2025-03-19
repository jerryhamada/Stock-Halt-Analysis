from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
from bs4 import BeautifulSoup
import yfinance as yf
from datetime import datetime, timedelta
import os

today = datetime.today().strftime('%Y-%m-%d')
script_dir = os.path.dirname(os.path.realpath(__file__))
global todayshalts
todayshalts = pd.DataFrame()


# Function to download one-minute interval data for a given symbol and halt time
def getCandle(symbol, halt_dt):
    #returns the one minute candle rounded down

    # Floor the halt time to the minute
    halt_ts = pd.Timestamp(halt_dt)
    candle_time = halt_ts.floor('min')
    print("candle_time:", candle_time)

    # Define the time window: from the start of the minute to one minute later
    start_dt = candle_time
    end_dt = candle_time + timedelta(minutes=1)

    # Download one-minute interval data for this window using yfinance
    data = yf.download(symbol, start=start_dt, end=end_dt, interval='1m')
    return data

#delete bad tickers
def deleteBadTickers(halts):
    badTickeridx = []
    for idx, row in halts.iterrows():
        symbol = row['Ticker']
        halt_dt = row['Halt_dt']

        candle = getCandle(symbol, halt_dt)
        if len(candle['Open']) == 0:
            print("No data for this symbol: " + symbol)
            badTickeridx.append(idx)
    halts.drop(index=badTickeridx, inplace=True)
    print(halts.to_string())
    return halts

# helper function to get the halts
def getHalts():
    driver = webdriver.Chrome()
    url = "https://nasdaqtrader.com/Trader.aspx?id=TradeHalts"
    driver.get(url)

    # Find the div containing the trading halts results
    div_trade_halt = driver.find_element(By.ID, "divTradeHaltResults")
    div_html = div_trade_halt.get_attribute("innerHTML")

    # Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(div_html, 'html.parser')
    table = soup.find("table")

    # Extract headers from the first row
    headers = [th.get_text(strip=True) for th in table.find("tr").find_all("th")]

    # Extract data rows, skipping the header row
    data_rows = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        if not cells:
            continue
        row = [td.get_text(strip=True) for td in cells]
        data_rows.append(row)

    # Create a pandas DataFrame
    halts = pd.DataFrame(data_rows, columns=headers)

    # I only want the halts that are due to volatility
    halts = halts.drop(['Issue Name', 'Resumption Date', 'Pause Threshold Price', 'Resumption Quote Time'], axis=1)
    halts = halts[halts['Reason Codes'] == 'LUDP']
    halts = halts.iloc[::-1].reset_index(drop=True)
    halts['Halt_dt'] = pd.to_datetime(halts['Halt Date'] + ' ' + halts['Halt Time'], format='%m/%d/%Y %H:%M:%S')
    halts['Resume_dt'] = pd.to_datetime(halts['Halt Date'] + ' ' + halts['Resumption Trade Time'], format='%m/%d/%Y %H:%M:%S')
    halts = halts.drop(['Halt Date', 'Halt Time', 'Market', 'Reason Codes', 'Resumption Trade Time'], axis=1)
    halts.rename(columns={"Issue Symbol": "Ticker"}, inplace=True)
    halts = deleteBadTickers(halts)
    return halts

# Get the halts for today or not 
def haltSaverChecker():
    user_input = input("Do you want to update today's halts and update the file? (y/n): ").strip().lower()
    file_path = os.path.join(script_dir, f"halts_{today}.csv")
    if user_input == 'y':
        todayshalts = getHalts()
        todayshalts.to_csv(file_path, index=False)
        # print("CSV file saved to:", os.path.abspath(file_path))
        print(todayshalts.to_string())

    else:
        todayshalts = pd.read_csv(file_path)
        # print(todayshalts.to_string())
        print("okay pookie!")
    return todayshalts

todayshalts = haltSaverChecker()
print(todayshalts.to_string())

    


# Process each halt event in the DataFrame
# results = []
# for idx, row in todayshalts.iterrows():
#     symbol = row['Issue Symbol']

#     halt_dt = row['HaltDateTime']
#     print("symbol:", symbol)
#     print("halt_dt:", halt_dt)

#     candle = getCandle(symbol, halt_dt)
#     if len(candle['Open']) == 0:
#         print("No data for this symbol")
#     else:
#         print('close: ' + str(candle["Close"].values[0].item()))
#         print('open: ' + str(candle["Open"].values[0].item()))
#     print('\n\n')

input("press anything to exit")
driver.quit()