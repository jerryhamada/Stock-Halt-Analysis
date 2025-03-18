from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
from bs4 import BeautifulSoup

# Initialize the Chrome WebDriver (ensure chromedriver is installed and in your PATH)
driver = webdriver.Chrome()

url = "https://nasdaqtrader.com/Trader.aspx?id=TradeHalts"
driver.get(url)

# Wait a fixed time (e.g., 5-10 seconds) to allow dynamic content to load
# time.sleep(5)  # Adjust this delay as needed
pd.set_option('display.width', 1000)

# Find the div containing the trading halts results
div_trade_halt = driver.find_element(By.ID, "divTradeHaltResults")
div_html = div_trade_halt.get_attribute("innerHTML")
# print("HTML content of 'divTradeHaltResults':")
# print(div_html)

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

print(halts.to_string())

input("Press Enter to close the browser...")
driver.quit()
