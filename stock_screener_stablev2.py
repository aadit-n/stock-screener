import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import threading
import yfinance as yf
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, time, timedelta
import pytz
import time as t
import requests
from bs4 import BeautifulSoup
import numpy as np
import re
import logging
import json
import os
import random


logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("stock_screener.log"), 
                             logging.StreamHandler()])
logger = logging.getLogger(__name__)

class StockScreenerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Stock Screener")
        self.root.geometry("600x500")
        
        self.running = False
        self.thread = None
        self.eod_snapshot_done = False
        self.last_quarterly_pe_update = None
        self.quarterly_pe_data_cache = {}
        self.quarter_headers = []
        
        self.config_file = "stock_screener_config.json"
        if os.path.exists(self.config_file):
            with open(self.config_file, "r") as f:
                config = json.load(f)
                self.tickers = config.get("tickers", [])
        else:
            self.tickers = [
                "ASIANPAINT.NS", "AARTIIND.NS", "PIDILITIND.NS", "HINDZINC.NS",
                "BHARTIARTL.NS", "TATAMOTORS.NS", "HINDUNILVR.NS", "ITC.NS", 
                "RELIANCE.NS", "HDFCBANK.NS", "INFY.NS", "TCS.NS"
            ]
            self.save_config()
        
        self.create_widgets()
        
        self.sheet = None
        
    def create_widgets(self):
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        status_frame = ttk.LabelFrame(main_frame, text="Status", padding="10")
        status_frame.pack(fill=tk.X, pady=10)
        
        self.status_var = tk.StringVar(value="Ready")
        status_label = ttk.Label(status_frame, textvariable=self.status_var)
        status_label.pack(fill=tk.X)
        
        control_frame = ttk.Frame(main_frame, padding="10")
        control_frame.pack(fill=tk.X, pady=10)
        
        self.start_button = ttk.Button(control_frame, text="Start", command=self.toggle_service)
        self.start_button.pack(side=tk.LEFT, padx=5)
        
        ttk.Button(control_frame, text="Add Ticker", command=self.add_ticker).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Save Config", command=self.save_config).pack(side=tk.LEFT, padx=5)
        
        ticker_frame = ttk.LabelFrame(main_frame, text="Tracked Tickers", padding="10")
        ticker_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        scrollbar = ttk.Scrollbar(ticker_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.ticker_listbox = tk.Listbox(ticker_frame)
        self.ticker_listbox.pack(fill=tk.BOTH, expand=True)
        
        self.ticker_listbox.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.ticker_listbox.yview)
        
        self.context_menu = tk.Menu(self.ticker_listbox, tearoff=0)
        self.context_menu.add_command(label="Remove Ticker", command=self.remove_ticker)
        self.ticker_listbox.bind("<Button-3>", self.show_context_menu)
        
        self.update_ticker_listbox()
        
        log_frame = ttk.LabelFrame(main_frame, text="Log", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        log_scrollbar = ttk.Scrollbar(log_frame)
        log_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.log_text = tk.Text(log_frame, height=10, wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        self.log_text.config(yscrollcommand=log_scrollbar.set)
        log_scrollbar.config(command=self.log_text.yview)
        
        self.log_handler = TextHandler(self.log_text)
        self.log_handler.setLevel(logging.INFO)
        logger.addHandler(self.log_handler)
    
    def update_ticker_listbox(self):
        self.ticker_listbox.delete(0, tk.END)
        for ticker in self.tickers:
            self.ticker_listbox.insert(tk.END, ticker)
    
    def show_context_menu(self, event):
        try:
            self.ticker_listbox.selection_clear(0, tk.END)
            self.ticker_listbox.selection_set(self.ticker_listbox.nearest(event.y))
            self.context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.context_menu.grab_release()
    
    def remove_ticker(self):
        try:
            selected_index = self.ticker_listbox.curselection()[0]
            ticker = self.ticker_listbox.get(selected_index)
            self.tickers.remove(ticker)
            self.update_ticker_listbox()
            self.save_config()
            logger.info(f"Removed ticker: {ticker}")
        except Exception as e:
            logger.error(f"Error removing ticker: {e}")
    
    def add_ticker(self):
        ticker = simpledialog.askstring("Add Ticker", "Enter ticker symbol (e.g., INFY.NS):")
        if ticker:
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                if 'regularMarketPrice' not in info or info['regularMarketPrice'] is None:
                    messagebox.showerror("Invalid Ticker", f"Could not validate ticker: {ticker}")
                    return
                
                self.tickers.append(ticker)
                self.update_ticker_listbox()
                self.save_config()
                logger.info(f"Added ticker: {ticker}")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to add ticker: {e}")
    
    def save_config(self):
        try:
            config = {"tickers": self.tickers}
            with open(self.config_file, "w") as f:
                json.dump(config, f)
            logger.info("Configuration saved")
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
    
    def toggle_service(self):
        if not self.running:
            self.start_service()
        else:
            self.stop_service()
    
    def start_service(self):
        if self.running:
            return

        def start_threaded():
            try:
                self.setup_sheets()
                self.running = True
                self.thread = threading.Thread(target=self.run_service, daemon=True)
                self.thread.start()
                self.root.after(0, lambda: self.start_button.config(text="Stop"))
                self.root.after(0, lambda: self.status_var.set("Running"))
                logger.info("Service started")
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Error", f"Failed to start service: {e}"))
                logger.error(f"Failed to start service: {e}")

        threading.Thread(target=start_threaded, daemon=True).start()
    
    def stop_service(self):
        if not self.running:
            return
        
        self.running = False
        if self.thread:
            self.thread.join(timeout=1.0)
        
        self.start_button.config(text="Start")
        self.status_var.set("Stopped")
        logger.info("Service stopped")
    
    def setup_sheets(self):
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
            client = gspread.authorize(creds)
            spreadsheet = client.open("stock_screener")
            self.sheet = spreadsheet.sheet1
            logger.info("Connected to Google Sheets")
        except Exception as e:
            logger.error(f"Error connecting to Google Sheets: {e}")
            raise
    
    def run_service(self):
        logger.info("Running initial quarterly and PE data update...")
        self.update_quarterly_and_pe_data()
        
        self.update_sheet(force=True)
        
        last_update = datetime.now()
        
        while self.running:
            now = datetime.now()
            
            if (now - last_update).total_seconds() >= 300:  
                self.update_sheet()
                last_update = now
            
            india = pytz.timezone("Asia/Kolkata")
            india_now = datetime.now(india)
            
            if (india_now.hour == 13 and india_now.minute == 30 and 
                (self.last_quarterly_pe_update is None or 
                (india_now.date() > self.last_quarterly_pe_update.date()))):
                logger.info("Running scheduled quarterly data update")
                self.update_quarterly_and_pe_data()
            
            t.sleep(30)
    
    def is_market_open(self):
        india = pytz.timezone("Asia/Kolkata")
        now = datetime.now(india)
        market_start = time(9, 15)
        market_end = time(15, 30)
        return now.weekday() < 5 and market_start <= now.time() <= market_end
    
    def is_market_closed_exactly(self):
        india = pytz.timezone("Asia/Kolkata")
        now = datetime.now(india)
        return now.weekday() < 5 and now.time().hour == 15 and now.time().minute == 30
    
    def sanitize(self, value):
        if value is None:
            return 'N/A'
        try:
            if isinstance(value, (float, np.float64)) and (pd.isna(value) or not np.isfinite(value)):
                return 'N/A'
            if isinstance(value, (float, np.float64, np.int64, int)):
                return round(value, 2)
        except:
            pass
        
        if isinstance(value, str):
            try:
                cleaned_value = value.replace(',', '').strip()
                if cleaned_value.endswith(('B', 'b')):
                    return round(float(cleaned_value[:-1]) * 10**9, 2)
                elif cleaned_value.endswith(('M', 'm')):
                    return round(float(cleaned_value[:-1]) * 10**6, 2)
                elif cleaned_value.endswith(('K', 'k')):
                    return round(float(cleaned_value[:-1]) * 10**3, 2)
                
                return round(float(cleaned_value), 2)
            except:
                pass
        
        return value
    
    def clean_to_float(self, val):
        try:
            if val is None:
                return None
            return float(val.replace(',', '').replace('−', '-').replace('(', '-').replace(')', ''))
        except:
            return None
    
    def get_quarterly_data(self, symbol):
        max_retries = 3
        retry_delay = 5 
        for attempt in range(max_retries):
            try:
                symbol = symbol.replace(".NS", "")
                
                logger.info(f"Fetching Quarterly Data for {symbol}")
                
                urls = [
                    f"https://www.screener.in/company/{symbol}/consolidated/",
                    f"https://www.screener.in/company/{symbol}/"
                    
                ]
                
                sales_row = None
                eps_row = None
                net_profit_row = None
                
                session = requests.Session()
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1"
                }
                session.headers.update(headers)
                
                for url in urls:
                    try:
                        response = session.get(url, timeout=15)
                        if response.status_code != 200:
                            logger.warning(f"Failed to access {url}, status code: {response.status_code}")
                            continue
                        
                        soup = BeautifulSoup(response.text, 'html.parser')
                        section = soup.find('section', {'id': 'quarters'})
                        if not section:
                            logger.warning(f"No quarters section found at {url}")
                            continue
                        
                        table = section.find('table')
                        if not table:
                            logger.warning(f"No table found in quarters section at {url}")
                            continue
                        
                        tbody = table.find('tbody')
                        rows = tbody.find_all('tr') if tbody else table.find_all('tr')
                        
                        if not rows:
                            logger.warning(f"No rows found in table at {url}")
                            continue
                        
                        for row in rows:
                            cols = row.find_all('td')
                            if not cols:
                                continue
                            
                            if len(cols) > 0:
                                label = cols[0].text.strip().lower()
                                
                                if "sales" in label or "revenue" in label:
                                    sales_row = cols[1:]
                                    logger.info(f"Found sales row with {len(sales_row)} columns")
                                
                                if "eps" in label and "in rs" in label:
                                    eps_row = cols[1:]
                                    logger.info(f"Found EPS row with {len(eps_row)} columns")

                                if 'net' in label and 'profit' in label:
                                    net_profit_row = cols[1:]
                                    logger.info(f"Found net profit row with {len(net_profit_row)} columns")
                        
                        if sales_row:
                            break
                    
                    except Exception as url_error:
                        logger.error(f"Error processing URL {url}: {url_error}")
                
                if not sales_row:
                    logger.error(f"Sales/Revenue row missing for {symbol}")
                    return [], []
                
                if not eps_row:
                    logger.warning(f"EPS row missing for {symbol}, filling zeros...")
                    eps_row = [None] * len(sales_row)
                
                if not net_profit_row:
                    logger.warning(f"Net Profit row missing for {symbol}, filling zeros...")
                    net_profit_row = [None] * len(sales_row)
                
                sales = []
                for col in sales_row[:13]:  
                    try:
                        val = self.clean_to_float(col.text.strip()) if col else None
                        sales.append(val)
                    except Exception as e:
                        logger.warning(f"Error processing sales value: {e}")
                        sales.append(None)
                
                eps = []
                for col in eps_row[:13]:  
                    try:
                        val = self.clean_to_float(col.text.strip()) if col else 0.0
                        eps.append(val)
                    except Exception as e:
                        logger.warning(f"Error processing EPS value: {e}")
                        eps.append(0.0)

                net_profit = []
                for col in net_profit_row[:13]: 
                    try:
                        val = self.clean_to_float(col.text.strip()) if col else 0.0
                        net_profit.append(val)
                    except Exception as e:
                        logger.warning(f"Error processing net profit value: {e}")
                        net_profit.append(0.0)
                
                return eps, sales, net_profit
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed on attempt {attempt+1}/{max_retries}: {e}")
                if attempt < max_retries - 1:
                    sleep_time = retry_delay * (2 ** attempt)
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    t.sleep(sleep_time)
                else:
                    logger.error(f"Max retries reached for {symbol}")
                    return [], [], []
            
            except Exception as e:
                logger.error(f"Error scraping quarterly data for {symbol}: {e}")
                return [], [], []
        
    def get_quarterly_headers(self, symbol):
        """Extract quarterly headers from screener.in"""
        try:
            symbol = symbol.replace(".NS", "")
            logger.info(f"Fetching quarterly headers for {symbol}")
            
            urls = [
                f"https://www.screener.in/company/{symbol}/",
                f"https://www.screener.in/company/{symbol}/consolidated/"
            ]
            
            headers = []
            session = requests.Session()
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1"
            })
            
            for url in urls:
                try:
                    t.sleep(2 + random.random() * 3)
                    
                    response = session.get(url, timeout=15)
                    if response.status_code != 200:
                        logger.warning(f"Failed to access {url}, status code: {response.status_code}")
                        continue
                    
                    soup = BeautifulSoup(response.text, 'html.parser')
                    section = soup.find('section', {'id': 'quarters'})
                    if not section:
                        logger.warning(f"No quarters section found at {url}")
                        continue
                    
                    table = section.find('table')
                    if not table:
                        logger.warning(f"No table found in quarters section at {url}")
                        continue
                    
                    thead = table.find('thead')
                    if not thead:
                        logger.warning(f"No table header found in quarters section at {url}")
                        continue
                    
                    th_elements = thead.find_all('th')
                    if len(th_elements) <= 1:  
                        logger.warning(f"No quarter headers found in table at {url}")
                        continue
                    
                    headers = [th.text.strip() for th in th_elements[1:]]
                    logger.info(f"Found {len(headers)} quarter headers: {headers}")
                    
                    if headers:
                        break  
                        
                except Exception as url_error:
                    logger.error(f"Error processing URL {url}: {url_error}")
                    t.sleep(5)
            
            return headers
        
        except Exception as e:
            logger.error(f"Error extracting quarter headers for {symbol}: {e}")
            return []
        
    def create_full_headers(self, quarter_headers):
        """Create full headers set based on extracted quarter headers"""
        basic_headers = [
            "Ticker", "Sector", "CMP", "PE", "PB", "EPS", "TTM Sales",
            "52W High", "52W Low", "Dividend Yield",
            "YoY EPS Growth", "YoY Sales Growth"
        ]
        
        price_change_headers = [
            "1D %", "5D %", "1M %", "3M %", "6M %", "YTD %", "1Y %", "3Y %"
        ]
        
        eps_headers = [f"{quarter} EPS" for quarter in quarter_headers]
        
        sales_headers = [f"{quarter} Sales" for quarter in quarter_headers]
        
        profit_headers = [f"{quarter} Profit" for quarter in quarter_headers]
        
        return basic_headers + price_change_headers + eps_headers + sales_headers + profit_headers
    
    def get_price_changes(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            logger.info(f"Fetching price changes for {ticker}")
            
            hist = stock.history(period="3y")
            
            if hist.empty:
                return ['N/A'] * 8
            
            current_price = hist['Close'].iloc[-1]
                
            price_changes = []
            
            if len(hist) >= 2:
                one_day = ((current_price - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100
                price_changes.append(self.sanitize(one_day))
            else:
                price_changes.append('N/A')
                
            days_5_ago_idx = min(5, len(hist) - 1)
            if len(hist) > days_5_ago_idx:
                five_day_price = hist['Close'].iloc[-days_5_ago_idx-1]
                five_days = ((current_price - five_day_price) / five_day_price) * 100
                price_changes.append(self.sanitize(five_days))
            else:
                price_changes.append('N/A')
                
            month_ago_idx = min(21, len(hist) - 1)
            if len(hist) > month_ago_idx:
                month_price = hist['Close'].iloc[-month_ago_idx-1]
                one_month = ((current_price - month_price) / month_price) * 100
                price_changes.append(self.sanitize(one_month))
            else:
                price_changes.append('N/A')
                
            three_month_idx = min(63, len(hist) - 1)
            if len(hist) > three_month_idx:
                three_month_price = hist['Close'].iloc[-three_month_idx-1]
                three_months = ((current_price - three_month_price) / three_month_price) * 100
                price_changes.append(self.sanitize(three_months))
            else:
                price_changes.append('N/A')
                
            six_month_idx = min(126, len(hist) - 1)
            if len(hist) > six_month_idx:
                six_month_price = hist['Close'].iloc[-six_month_idx-1]
                six_months = ((current_price - six_month_price) / six_month_price) * 100
                price_changes.append(self.sanitize(six_months))
            else:
                price_changes.append('N/A')
                
            india = pytz.timezone("Asia/Kolkata")
            now = datetime.now(india)
            ytd_start = datetime(now.year, 1, 1).strftime('%Y-%m-%d')
            ytd_data = hist[hist.index >= ytd_start]
            if not ytd_data.empty:
                ytd_change = ((current_price - ytd_data['Close'].iloc[0]) / ytd_data['Close'].iloc[0]) * 100
                price_changes.append(self.sanitize(ytd_change))
            else:
                price_changes.append('N/A')
                
            one_year_idx = min(252, len(hist) - 1)
            if len(hist) > one_year_idx:
                one_year_price = hist['Close'].iloc[-one_year_idx-1]
                one_year = ((current_price - one_year_price) / one_year_price) * 100
                price_changes.append(self.sanitize(one_year))
            else:
                price_changes.append('N/A')
                
            if len(hist) > 5:
                oldest_price = hist['Close'].iloc[0]
                three_years = ((current_price - oldest_price) / oldest_price) * 100
                price_changes.append(self.sanitize(three_years))
            else:
                price_changes.append('N/A')
                
            return price_changes
            
        except Exception as e:
            logging.error(f"Error calculating price changes for {ticker}: {e}")
            return ['N/A'] * 8
    
    def get_financial_data(self, ticker):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            basic_financials = [
                ticker,
                self.sanitize(info.get('sector', 'N/A')),
                self.sanitize(info.get('currentPrice', 'N/A')),
                self.sanitize(info.get('trailingPE', 'N/A')),
                self.sanitize(info.get('priceToBook', 'N/A')),
                self.sanitize(info.get('trailingEps', 'N/A')),
                self.sanitize(info.get('totalRevenue', 'N/A'))/10**7,
                self.sanitize(info.get('fiftyTwoWeekHigh', 'N/A')),
                self.sanitize(info.get('fiftyTwoWeekLow', 'N/A')),
                self.sanitize(info.get('dividendYield', 'N/A')),
                self.sanitize(info.get('earningsGrowth', 'N/A')),
                self.sanitize(info.get('revenueGrowth', 'N/A'))
            ]
            
            price_changes = self.get_price_changes(ticker)
            
            if ticker in self.quarterly_pe_data_cache:
                cached_data = self.quarterly_pe_data_cache[ticker]
                eps_data = cached_data.get('eps_data', [])
                sales_data = cached_data.get('sales_data', [])
                net_profit_data = cached_data.get('net_profit_data', [])
                
            else:
                eps_data = ['N/A'] * 13
                sales_data = ['N/A'] * 13
                net_profit_data = ['N/A'] * 13
            
            return basic_financials + price_changes + eps_data + sales_data + net_profit_data
        
        except Exception as e:
            logging.error(f"Error fetching data for {ticker}: {e}")
            return [ticker] + ['N/A'] * 44
    
    def update_quarterly_and_pe_data(self):
        india = pytz.timezone("Asia/Kolkata")
        now = datetime.now(india)
        logger.info(f"Running quarterly and PE data update at {now}")
        
        self.quarter_headers = []
        
        for i, ticker in enumerate(self.tickers):
            try:
                if i == 0 or not self.quarter_headers:
                    self.quarter_headers = self.get_quarterly_headers(ticker)
                    if not self.quarter_headers:
                        logger.warning("Failed to get quarter headers, using default")
                        self.quarter_headers = [
                            "Q4/21-22", "Q1/22-23", "Q2/22-23", "Q3/22-23", "Q4/22-23",
                            "Q1/23-24", "Q2/23-24", "Q3/23-24", "Q4/23-24",
                            "Q1/24-25", "Q2/24-25", "Q3/24-25", "Q4/24-25"
                        ]
                
                if i > 0:
                    t.sleep(1 + random.random() * 2)
                    
                eps_data, sales_data, net_profit_data = self.get_quarterly_data(ticker)
                
                self.quarterly_pe_data_cache[ticker] = {
                    'eps_data': eps_data,
                    'sales_data': sales_data,
                    'net_profit_data': net_profit_data,
                }
                
                logger.info(f"Updated quarterly and PE data for {ticker}")
                
                self.root.after(0, lambda: self.status_var.set(f"Updated: {ticker}"))
                
            except Exception as e:
                logger.error(f"Error updating quarterly and PE data for {ticker}: {e}")
        
        self.last_quarterly_pe_update = now
        logger.info("Quarterly and PE data update completed")
    
    def update_sheet(self, force=False):
        if not self.sheet:
            logger.error("Google Sheet not available")
            return
        
        india = pytz.timezone("Asia/Kolkata")
        now = datetime.now(india)
        
        market_is_open = self.is_market_open()
        market_closed_now = self.is_market_closed_exactly()
        
        if market_is_open:
            self.eod_snapshot_done = False
            logging.info(f"Market is open. Live update.")
        elif market_closed_now and not self.eod_snapshot_done:
            logging.info(f"Market just closed. EOD snapshot.")
            self.eod_snapshot_done = True
        elif not market_is_open and force:
            logging.info(f"Market closed. Doing EOD update as fallback.")
        else:
            logging.info(f"Skipping update.")
            return
        
        data = []
        for ticker in self.tickers:
            row = self.get_financial_data(ticker)
            logging.info(f"Fetched data for {ticker}")
            self.root.after(0, lambda t=ticker: self.status_var.set(f"Updating: {t}"))
            data.append(row)
        
            if not hasattr(self, 'quarter_headers') or not self.quarter_headers:
                self.quarter_headers = self.get_quarterly_headers(self.tickers[0]) if self.tickers else []
            
            if not self.quarter_headers:
                self.quarter_headers = [
                    "Q4/21-22", "Q1/22-23", "Q2/22-23", "Q3/22-23", "Q4/22-23",
                    "Q1/23-24", "Q2/23-24", "Q3/23-24", "Q4/23-24",
                    "Q1/24-25", "Q2/24-25", "Q3/24-25", "Q4/24-25"
                ]
        
        headers = self.create_full_headers(self.quarter_headers)
        
        percentage_columns_indices = {
            8: True,   
            9: True,  
            10: True, 
            

            11: True, 
            12: True, 
            13: True,  
            14: True, 
            15: True,  
            16: True,  
            17: True,  
            18: True,
            19: True, 
        }
        
        processed_data = []
        for row in data:
            processed_row = list(row) 
            for idx, value in enumerate(processed_row):
                if idx in percentage_columns_indices and value != 'N/A':
                    try:
                        processed_row[idx] = float(value) / 100.0
                    except (ValueError, TypeError):
                        pass
            processed_data.append(processed_row)
        
        start_col = 'C'
        end_col_idx = 2 + len(headers) - 1
        end_col = ""
        
        while end_col_idx >= 0:
            end_col = chr(65 + (end_col_idx % 26)) + end_col
            end_col_idx = end_col_idx // 26 - 1
        
        end_row = 2 + len(processed_data)
        range_str = f"{start_col}2:{end_col}{end_row}"
        
        logging.info(f"Updating sheet range: {range_str}")
        
        self.sheet.update(values=[headers] + processed_data, range_name=range_str)
        
        percentage_columns = {
            2: '0',
            3: '0.0',
            4:'0.0',
            5: '0',
            6: '0',
            7: '0',
            8: "0",  
            9: "0.0%", 
            10: "0.0%",
            11: "0.0%",
            12: "0%", 
            13: "0%",
            14: "0%", 
            15: "0%",
            16: "0%", 
            17: "0%",
            18: "0%", 
            19: "0%"
        }
        
        try:
            for col_idx, format_pattern in percentage_columns.items():
                col_letter = ""
                temp_idx = col_idx + 2  
                while temp_idx >= 0:
                    col_letter = chr(65 + (temp_idx % 26)) + col_letter
                    temp_idx = temp_idx // 26 - 1
                
                format_range = f"{col_letter}3:{col_letter}{end_row}"
                
                self.sheet.format(format_range, {"numberFormat": {"type": "PERCENT", "pattern": format_pattern}})
                
            logging.info("Applied percentage formatting to relevant columns")
        except Exception as format_error:
            logging.warning(f"Failed to apply formatting: {format_error}")
        
        logging.info(f"Sheet updated with {len(processed_data)} stocks.")
        self.root.after(0, lambda: self.status_var.set(f"Sheet updated: {now.strftime('%H:%M:%S')}"))


class TextHandler(logging.Handler):
    def __init__(self, text_widget):
        logging.Handler.__init__(self)
        self.text_widget = text_widget
    
    def emit(self, record):
        msg = self.format(record)
        def append():
            self.text_widget.configure(state='normal')
            self.text_widget.insert(tk.END, msg + '\n')
            self.text_widget.configure(state='disabled')
            self.text_widget.yview(tk.END)
        self.text_widget.after(0, append)


if __name__ == "__main__":
    root = tk.Tk()
    root.title("Stock Screener")
    
    try:
        root.iconbitmap("stock_icon.ico")
    except:
        pass
    
    app = StockScreenerApp(root)
    
    root.mainloop()