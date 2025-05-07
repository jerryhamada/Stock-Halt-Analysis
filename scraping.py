from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time
import pandas as pd
from bs4 import BeautifulSoup
import yfinance as yf
from datetime import datetime, timedelta
import os
import boto3
import io
import requests

# Function to download one-minute interval data for a given symbol and halt time
def getCandle(symbol, dt):
    #returns the one minute candle rounded down

    # Floor the halt time to the minute
    halt_ts = pd.Timestamp(dt)
    candle_time = halt_ts.floor('min')
    print("candle_time:", candle_time)

    # Define the time window: from the start of the minute to one minute later
    start_dt = candle_time
    end_dt = candle_time + timedelta(minutes=1)

    # Download one-minute interval data for this window using yfinance
    data = yf.download(symbol, start=start_dt, end=end_dt, interval='1m')
    return data

#helper function to delete bad tickers
# used in haltSaverChecker function
def deleteBadTickers(halts):
    badTickeridx = []
    for idx, row in halts.iterrows():
        symbol = row['Ticker']
        halt_dt = row['Halt_dt']
        halt_dt = halt_dt.floor('min')
        candle = getCandle(symbol, halt_dt)
        if len(candle['Open']) == 0:
            # print("No data for this symbol: " + symbol)
            badTickeridx.append(idx)
    halts.drop(index=badTickeridx, inplace=True)
    # print(halts.to_string())
    return halts

# Function to calculate the halt direction and price
def haltDirandPrice(halts):
    print("\n\n started running haltDiandPrice function successfully")
    for idx, row in halts.iterrows():
        symbol = row['Ticker']
        halt_dt = row['Halt_dt']
        halt_ts = pd.Timestamp(halt_dt)
        halt_ts = halt_ts.floor('min')
        
        curCandle = getCandle(symbol, halt_ts)
        prev_ts = halt_ts - timedelta(minutes=1)
        prevCandle = getCandle(symbol, prev_ts)
        curLow = curCandle['Low'].iloc[0].item()
        if len(prevCandle['Open']) == 0:
            prevLow = curLow
        else:
            prevLow = prevCandle['Low'].iloc[0].item()
        halt_price = curCandle['Close'].iloc[0].item()
        low = min(curLow, prevLow)
        if halt_price > low:
            direction = 'UP'
        else:
            direction = 'DOWN'
        halts.loc[idx, 'Direction'] = direction
        halts.loc[idx, 'Halt Price'] = halt_price
    print("\n\nfinished running haltDiandPrice function successfully")
    print(halts.to_string())
    return halts

# def make_driver():
#     chrome_opts = Options()
#     chrome_opts.add_argument("--headless")                # HEADLESS MODE
#     chrome_opts.add_argument("--disable-gpu")             # recommended for Windows
#     chrome_opts.add_argument("--no-sandbox")              # required on many Linux hosts
#     chrome_opts.add_argument("--disable-dev-shm-usage")   # overcome limited /dev/shm

#     # **Point Chrome at the layer’s headless binary**
#     chrome_opts.binary_location = "/opt/headless-chrome/headless-chrome"
#     driver_path = "/opt/chromedriver/bin/chromedriver"
#     service = Service(driver_path)

#     # **Point WebDriver at the layer’s chromedriver**
#     driver = webdriver.Chrome(
#         service = service,
#         options=chrome_opts
#     )
#     return driver

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
    time.sleep(5)
    table = soup.find("table")


    # print("table:", table)
    if not table:
        raise RuntimeError("No table found in the HTML.")
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
    halts['Halt Duration'] = ((halts['Resume_dt'] - halts['Halt_dt']).dt.total_seconds() // 60).astype(int)
    halts.rename(columns={"Issue Symbol": "Ticker"}, inplace=True)
    halts = deleteBadTickers(halts)
    return halts

# Get the halts for today or not 
def haltSaverChecker():
    todayshalts = getHalts()
    print("hello world")
    print("type" + str(type(todayshalts)))
    print(todayshalts.to_string())
    return todayshalts

# function to do the resumption price and percent change analysis
def postHaltAnalysis(halts):
    # adds the columns resumption price and percent change to the halts dataframe
    print("\n\n started running postHaltAnalysis function successfully")
    badTickeridx = []
    for idx, row in halts.iterrows():
        symbol = row['Ticker']
        resume_dt = row['Resume_dt']
        resume_ts = pd.Timestamp(resume_dt)
        resume_ts = resume_ts.floor('min')
        
        curCandle = getCandle(symbol, resume_ts)
        if len(curCandle['Open']) == 0:
            # If no data is available, skip this row
            badTickeridx.append(idx)
            continue
        resume_price = curCandle['Open'].iloc[0].item()
        percent_change = ((resume_price - row['Halt Price']) / row['Halt Price']) * 100
        halts.loc[idx, 'Resumption Price'] = resume_price
        halts.loc[idx, 'Percent Change'] = percent_change
    halts.drop(index=badTickeridx, inplace=True)

    print("\n\n finished running postHaltAnalysis function successfully")

    return halts

# fucntion to upload the dataframe to s3
def upload_to_s3(df):
    BUCKET = "allhalts"

# 3) Serialize DataFrame to CSV in memory
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    csv_bytes = buffer.getvalue().encode("utf-8")

# 4) Construct a timestamped key
    today = datetime.today().strftime('%Y-%m-%d')
    key = f"raw/{today}/halts_test.csv"

    # 5) Upload to S3
    s3 = boto3.client("s3")
    s3.put_object(Bucket=BUCKET, Key=key, Body=csv_bytes)
    return key

    print(f"✅ Uploaded test CSV to s3://{BUCKET}/{key}")

def lambda_handler(event, context):
    """
    AWS Lambda entry point.
    Ignores `event`/`context` for now—just runs your main pipeline.
    """
    try:
        # 1) Run your main logic (which returns the S3 key)
        s3_key = main()

        # 2) Return a simple success payload
        return {
            "statusCode": 200,
            "body": {
                "message":     "Upload successful",
                "uploadedKey": s3_key
            }
        }

    except Exception as e:
        # Print the error so it shows up in CloudWatch Logs
        print("Error in lambda_handler:", e, flush=True)
        # Rethrow so Lambda marks the invocation as a failure
        raise

def main():
    todayshalts = pd.DataFrame()
    todayshalts = haltSaverChecker()
    todayshalts = haltDirandPrice(todayshalts)
    todayshalts = postHaltAnalysis(todayshalts)
    s3_key = upload_to_s3(todayshalts)
    return s3_key

result = lambda_handler({}, None)
print(result)
