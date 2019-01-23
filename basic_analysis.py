import pandas as pd
import datetime as dt
import numpy as np
import sys

abs_min = -sys.maxsize - 1

file_loc = "C:\\Developer\\stock-notes\\data\\reliance_24m_15Jan_2019.csv"


def cleanup(x):
    return str(x).replace(" ", "_")


def get_date(date_str):
    record_date = dt.datetime.strptime(date_str, "%d-%b-%Y").date()
    if record_date.month < 10:
        return str(record_date.year) + str(record_date.month)
    else:
        return str(record_date.year) + '0' + str(record_date.month)


def append_returns(df: pd.DataFrame):
    prev_val = abs_min
    for row in df.itertuples():
        if prev_val == abs_min:
            df.at[row.Index, 'return'] = 0
        else:
            df.at[row.Index, 'return'] = (row.Close_Price - prev_val)*100/prev_val
        prev_val = row.Close_Price


def process_file(filename):
    data = pd.read_csv(filename)
    cols = data.columns.values
    new_cols = list(map(cleanup, cols))
    data.columns = new_cols
    data['record_date'] = data['Date'].apply(lambda x: get_date(x))
    data.sort_values(['record_date'])
    append_returns(data)
    data['abs_return'] = abs(data['return'])
    data_long_only = data[data['return'] > 0]
    data_grouped = data.groupby('record_date')
    data_grouped_stats = data_grouped['abs_return'].agg([np.sum])
    final_data = data_grouped_stats.describe().transpose()
    return final_data

result = process_file(file_loc)
print(type(result))
print(result)
