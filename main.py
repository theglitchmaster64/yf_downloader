#!/bin/python3

import yfinance as yf
import datetime
import sys, os
import pandas as pd


class DataObj:
    def __init__(self,symbol='NVDA'):
        try:
            self.sym = symbol
            self.ticker =yf.Ticker(self.sym)
            now = datetime.datetime.now()
            self.now = str(now.year)+'-'+str(now.month)+'-'+str(now.day)
            self.expiration_dates = self.ticker.options
            self.stock_data = self.ticker.history(period='1wk',interval='1m')
            self.stock_data.index = self.stock_data.index.tz_localize(None)
            self.option_data = {}
            print(self)
            for date in self.expiration_dates:
                sys.stderr.write('downloading {} option data for {}\n'.format(self.sym,date))
                self.option_data[date] = self.ticker.option_chain(date=date)
                self.option_data[date].calls.lastTradeDate = self.option_data[date].calls.lastTradeDate.dt.tz_convert('America/New_York')
                self.option_data[date].puts.lastTradeDate = self.option_data[date].puts.lastTradeDate.dt.tz_convert('America/New_York')
                self.option_data[date].calls.lastTradeDate = self.option_data[date].calls.lastTradeDate.dt.tz_localize(None).round('T')
                self.option_data[date].puts.lastTradeDate = self.option_data[date].puts.lastTradeDate.dt.tz_localize(None).round('T')
                #expand calls
                self.option_data[date].calls['stock_open'] = 0
                self.option_data[date].calls['stock_high'] = 0
                self.option_data[date].calls['stock_low'] = 0
                self.option_data[date].calls['stock_close'] = 0
                self.option_data[date].calls['stock_volume'] = 0
                #expand puts
                self.option_data[date].puts['stock_open'] = 0
                self.option_data[date].puts['stock_high'] = 0
                self.option_data[date].puts['stock_low'] = 0
                self.option_data[date].puts['stock_close'] = 0
                self.option_data[date].puts['stock_volume'] = 0

        except Exception as e:
            sys.stderr.write('failed it init DataObj for {}\nskipping...'.format(self.sym))


    def save_csv(self):
        try:
            pwd = os.getcwd()
            try:
                os.mkdir(self.sym)
            except FileExistsError:
                pass
            f1 = (pwd+'/{}/{}_{}_STOCK_DATA.csv').format(self.sym,self.now,self.sym)
            self.stock_data.to_csv(f1)
            for date in self.expiration_dates:
                f2 = (pwd+'/{}/{}_EXP-{}_{}_OPTION_DATA_CALL.csv').format(self.sym,self.now,date,self.sym)
                f3 = (pwd+'/{}/{}_EXP-{}_{}_OPTION_DATA_PUT.csv').format(self.sym,self.now,date,self.sym)
                self.option_data[date].calls.to_csv(f2)
                self.option_data[date].puts.to_csv(f3)
        except Exception as e:
            sys.stderr.write('failed to save CSV file(s) for {}\nexception:\n{}\n\n'.format(self.sym,e))

    def save_excel(self):
        try:
            pwd = os.getcwd()
            try:
                os.mkdir(self.sym)
            except FileExistsError:
                pass
            f1 = pwd + '/{}/{}_{}_STOCK_DATA.xlsx'.format(self.sym,self.sym,self.now)
            f2 = pwd + '/{}/{}_{}_OPTION_DATA_CALL.xlsx'.format(self.sym,self.sym,self.now)
            f3 = pwd + '/{}/{}_{}_OPTION_DATA_PUT.xlsx'.format(self.sym,self.sym,self.now)
            w1 = pd.ExcelWriter(f1,engine='xlsxwriter')
            w2 = pd.ExcelWriter(f2,engine='xlsxwriter')
            w3 = pd.ExcelWriter(f3,engine='xlsxwriter')
            self.stock_data.to_excel(w1)
            for date in self.expiration_dates:
                self.option_data[date].calls.to_excel(w2,sheet_name='EXP-{}'.format(date))
                self.option_data[date].puts.to_excel(w3,sheet_name='EXP-{}'.format(date))
            w1.save()
            sys.stderr.write('saving {} ...\n'.format(f1))
            w2.save()
            sys.stderr.write('saving {} ...\n'.format(f2))
            w3.save()
            sys.stderr.write('saving {} ...\n'.format(f3))
        except Exception as e:
            sys.stderr.write('failed to save XLSX file(s) for {}\nexception:\n{}\n\n'.format(self.sym,e))



    def __repr__(self):
        return '\nSYM={}\nDATE={}\nEXP_DATES={}\n'.format(self.sym,self.now,self.expiration_dates)


    def _return_matching_stock_row(self,option_row):
        sys.stderr.write('matching...\n')
        return self.stock_data[self.stock_data.index == option_row]

    def match(self):
        #match calls
        for date in self.expiration_dates:
            for i in range(len(self.option_data[date].calls['lastTradeDate'])):
                match = self._return_matching_stock_row(self.option_data[date].calls['lastTradeDate'][i])
                if match.empty:
                    self.option_data[date].calls.iloc[i,14] = None
                    self.option_data[date].calls.iloc[i,15] = None
                    self.option_data[date].calls.iloc[i,16] = None
                    self.option_data[date].calls.iloc[i,17] = None
                    self.option_data[date].calls.iloc[i,18] = None
                else:
                    self.option_data[date].calls.iloc[i,14] = float(match.Open)
                    self.option_data[date].calls.iloc[i,15] = float(match.High)
                    self.option_data[date].calls.iloc[i,16] = float(match.Low)
                    self.option_data[date].calls.iloc[i,17] = float(match.Close)
                    self.option_data[date].calls.iloc[i,18] = float(match.Volume)
        #match puts
        for date in self.expiration_dates:
            for i in range(len(self.option_data[date].puts['lastTradeDate'])):
                match = self._return_matching_stock_row(self.option_data[date].puts['lastTradeDate'][i])
                if match.empty:
                    self.option_data[date].puts.iloc[i,14] = None
                    self.option_data[date].puts.iloc[i,15] = None
                    self.option_data[date].puts.iloc[i,16] = None
                    self.option_data[date].puts.iloc[i,17] = None
                    self.option_data[date].puts.iloc[i,18] = None
                else:
                    self.option_data[date].puts.iloc[i,14] = float(match.Open)
                    self.option_data[date].puts.iloc[i,15] = float(match.High)
                    self.option_data[date].puts.iloc[i,16] = float(match.Low)
                    self.option_data[date].puts.iloc[i,17] = float(match.Close)
                    self.option_data[date].puts.iloc[i,18] = float(match.Volume)

    def sortall(self):
        for date in self.expiration_dates:
            self.option_data[date].calls.sort_values(by='lastTradeDate',inplace=True)
            self.option_data[date].puts.sort_values(by='lastTradeDate',inplace=True)


def test():
    TEST_SET = ['TSLA','AAPL','MSFT','META','NVDA','XOM','KO','PEP','MRK','DIS','MCD','QCOM','NKE','INTC','NFLX','BA','LMT','RTX','GOOGL','GOOG','CVX','V','MA']
    for SYM in TEST_SET:
        obj = DataObj(SYM)
        obj.match()
        obj.save_excel()


def test2():
    TEST_SET = ['TSLA','AAPL','MSFT','META','NVDA','XOM','KO','PEP','MRK','DIS','MCD','QCOM','NKE','INTC','NFLX','BA','LMT','RTX','GOOGL','GOOG','CVX','V','MA']
    for SYM in TEST_SET:
        DataObj(SYM).save_excel()

def test3():
    obj = DataObj('TSLA')
    obj.match()
    obj.save_excel()

if __name__=='__main__':
    print('ok')
    test()










