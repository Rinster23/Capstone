import pandas as pd
import os
import tarfile
import copy
import numpy as np

INPUT_FOLDER_PATH = '../Data/Origin'
OUT_FOLDER_PATH = '../Data/'
DATA_PATH = '../Data/Process'


def unzip_files(input_folder_path, out_folder_path):
    if not os.path.exists(out_folder_path):
        os.makedirs(out_folder_path)
    for file_name in os.listdir(input_folder_path):
        if file_name == '.DS_Store':
            continue
        input_file_path = input_folder_path + '/' + file_name
        out_file_path = out_folder_path
        file = tarfile.open(input_file_path)
        names = [name for name in file.getnames() if 'bars' in name]
        file.extractall(path=out_file_path, members=[file.getmember(name) for name in names])
        file.close()
    os.rename(out_folder_path+'/out', out_folder_path+'/Process')

def fetch_stocks(data_path=DATA_PATH):
    stocks = os.listdir(data_path)
    if '.DS_Store' in stocks:
        stocks.remove('.DS_Store')
    return stocks

def fetch_trade_dates(stock='AAPL', data_path=DATA_PATH):
    return sorted([date.split('.')[-1] for date in os.listdir(data_path+'/'+stock)])

def fetch_times(freq:str):
    if freq not in ['1', '5', '10', '15', '30', 'd']:
        raise ValueError('Freq must be either 1 or 5 or 10 or 15 or 30 or d')
    elif freq == 'd':
        return ['093000', '160000']
    else:
        times = ['090{}00'.format(minute) if minute<10 else '09{}00'.format(minute) for minute in range(30, 60, int(freq))]
        for hour in [str(i) for i in range(10, 16)]:
            times += ['{}0{}00'.format(hour, minute) if minute<10 else '{}{}00'.format(hour, minute) for minute in range(0, 60, int(freq))]
        times += ['160000']
        return times

def fetch_data_stock(stock:str, bgn_date:str, end_date:str, freq:str, cols=None, data_path=DATA_PATH):
    stock = stock.upper()
    # if stock not in fetch_stocks(data_path):
    #     raise Exception('There is not such one stock: {}'.format(stock))
    data_path = data_path + '/{}'.format(stock)
    data = pd.DataFrame()    
    for file in sorted([i for i in os.listdir(data_path) if i.split('.')[-1]>=bgn_date and i.split('.')[-1]<=end_date]):
        file_path = data_path + '/' + file
        date = file.split('.')[-1]
        df = pd.read_csv(file_path, sep=' ')
        df.time = df.time.apply(lambda x: ''.join(x.split(':')))
        times = fetch_times(freq)
        df_freq = df[df.time.isin(times[1:])].copy().reset_index(drop=True)
        df_freq.loc[:,df_freq.columns[1:]] = np.nan
        if freq != '1':
            for idx, time in enumerate(times[1:]):
                tmpdf = df[(df.time > times[idx]) & (df.time <= time)]
                for col in ['trade_count', 'trade_volume', 'hid_vol', 'buy_vol', 'sell_vol', 'unsided_vol']:
                    df_freq.loc[idx, col] = np.nansum(tmpdf[col])
                for col in ['bid_price', 'ask_price', 'bid_size', 'ask_size','trade_first','trade_last',]:
                    df_freq.loc[idx, col] = tmpdf[col].values[0]
                df_freq.loc[idx, 'trade_high'] = np.nanmax(tmpdf.trade_high.values)
                df_freq.loc[idx,'trade_low'] = np.nanmin(tmpdf.trade_low.values)
                df_freq.loc[idx,'vwap'] = np.nansum(tmpdf.vwap.values*tmpdf.trade_volume.values)/np.nansum(tmpdf.trade_volume.values)                                                                              
                df_freq.loc[idx, 'time'] = pd.to_datetime(date + df_freq.loc[idx, 'time'])
            data = pd.concat([data, df_freq])
    data = data.sort_values(by='time', ascending=True).reset_index(drop=True)
    return data

def fetch_data(stocks:list, bgn_date:str, end_date:str, freq:str, cols=None, data_path=DATA_PATH):
    data = {}
    for stock in stocks:
        df = fetch_data_stock(stock, bgn_date, end_date, freq, data_path)
        if cols == None:
            columns = df.columns
        else:
            columns = ['time'] + cols if 'time' not in cols else cols
            for col in columns:
                if col not in df.columns:
                    cols.remove(col)
        data[stock.upper()] = df[columns]
    return data



if __name__ == '__main__':
    pass
