# stock-screener
Automates the process of fetching **real-time prices** and **quarterly financial data** (like EPS and sales) for **NSE-listed Indian stocks**, and updates it directly into a **Google Sheet**.

It’s built for retail traders, analysts, and anyone who wants live, centralized access to essential stock data.

---

## Features

-  **Real-Time Price Updates** via `yfinance`
-  **Quarterly Financials Scraped** from reliable sources (EPS, Sales, etc.)
-  **Auto-Updating Google Sheet** using Google Sheets API
-  **Indian Stock Focused** – Built for NSE tickers
-  **Modular & Customizable** – Easily adjust the tickers, data points, and format

---

##  Setup Instructions

1. **Clone this repo**  
   ```bash
   git clone https://github.com/aadit-n/stock-screener.git
   cd stock-screener
2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
3. **Setup Google Sheets API access**
   - Follow this guide to set up API access -> [Google Sheets API Quickstart](https://developers.google.com/workspace/sheets/api/guides/concepts)
   - Place your credentials.json file in the root directory.
   - Make sure the structure matches the credentials(template).json provided in the repo.
4. **Run the App**
   ```bash
   python stock_screener_stablev2.py

**NOTE**
1. This app is designed specifically for Indian stock tickers (e.g., RELIANCE.NS, INFY.NS). So make sure to add the '.NS' suffix to all tickers when adding tickers.
2. Make sure your Google Sheet is shared with the email listed in the credentials.json.
