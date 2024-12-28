from os import access
import xlwings as xw
from connect_to_dhan import Connection
from dhanhq import dhanhq
import numpy as np
import pandas as pd
import yaml
import time


""" configuring the workbook and getting the security info
"""
# Load Excel file
excel_file = 'TradeHistory.xlsx'
workbook = xw.Book(excel_file)
TradeHistorySheet = workbook.sheets['TradeHistory']

def connect_to_dhan():
    config_file_path = r".\\config\\config.yaml" #This has the API key and client id
    with open(config_file_path,'r') as config:
        api_config = yaml.safe_load(config)
    clinet_id = api_config.get('api_config')[1]
    access_token = api_config.get('api_config')[2]

    # Establish connection to Dhan
    try:
        DhanConnector = Connection(clinet_id, access_token)
        ConnectionObject = DhanConnector.connect_dhan()
        dhan = ConnectionObject['conn']
    except Exception as e :
        raise ConnectionError(f"Can't connect {e}")  

    return({"connection":dhan,
            "client_id":clinet_id,
            "access_token":access_token})  


def main():
    connections__= connect_to_dhan() #returned as dictionary but accessed like a list
    dhan = connections__['connection']
    TradeHistorySheet.clear()
    start_date = '2023-01-01'
    end_date = '2024-12-27'
    page_count = 0
    trade_history = []
    while dhan.get_trade_history(from_date=start_date,to_date=end_date,page_number=page_count)['data']:
        trade_history = trade_history + dhan.get_trade_history(from_date=start_date,to_date=end_date,page_number=page_count)['data']
        time.sleep(1)
        print(f"Imported page {page_count} successfully!")
        page_count = page_count+ 1
    pd_trade_history = pd.DataFrame(trade_history)
    TradeHistorySheet.range("A1").options(index=False).value = pd_trade_history
    


if __name__ == "__main__":
    main()
    print("Trade History Generated Sucessfully")
