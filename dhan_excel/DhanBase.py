from os import access
import xlwings as xw
from connect_to_dhan import Connection
from dhanhq import dhanhq
import numpy as np
import pandas as pd
from dhanhq import marketfeed
import yaml
import asyncio


def option_age(x):  # This function will classify the options strike if it belongs to Current series, Next series or Far next.
    if x == 1.0:
        return 'C'  # Current series
    elif x == 2.0:
        return 'N'  # Next Series
    else:
        return 'F'  # Far next series

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


def configure_the_workbook():
    """ configuring the workbook and getting the security info
    """
    # Load Excel file
    excel_file = 'Dhan_orders.xlsx'
    workbook = xw.Book(excel_file)
    config_sheet = workbook.sheets['Config']
    home_sheet = workbook.sheets['Home']
    underlying_sheet = workbook.sheets['Underlying'] 
    options_sheet = workbook.sheets['Options']
    crude_options_chain_sheet = workbook.sheets['CRUDE']
    
    dhan,client_id, access_token = connect_to_dhan()
    
    # Load the latest keys and scripts metadata automatically
    security_list = dhan.fetch_security_list("compact")
    
    #--------------------------------------------------------------------------------------------
    # Filter for only NIFTY and BANKNIFTY OPTIONS and MCX
    NSE = security_list[(
        (security_list['SEM_EXM_EXCH_ID'] == 'NSE') & 
        (security_list['SEM_INSTRUMENT_NAME'] == 'OPTIDX') & 
        ((security_list['SEM_TRADING_SYMBOL'].str.startswith('BANKNIFTY')) | 
         (security_list['SEM_TRADING_SYMBOL'].str.startswith('NIFTY')))
        &
        (security_list['SEM_TRADING_SYMBOL'].str.startswith('NIFTYNXT50') == False)
    )]    
    MCX = security_list[(security_list['SEM_EXM_EXCH_ID'] == 'MCX') & 
                        (security_list['SEM_INSTRUMENT_NAME'] == 'OPTFUT') & 
                        (security_list['SM_SYMBOL_NAME'] == 'CRUDEOIL')]

    filtered_df = pd.concat([NSE, MCX])
    filtered_df = filtered_df.copy()  # We are copying as a new instance to avoid slice warning    

    # Below logic will help us to classify the Series type
    filtered_df['SEM_EXPIRY_DATE_CUSTOM'] = np.nan
    filtered_df['SEM_EXPIRY_DATE_CUSTOM'] = pd.to_datetime(filtered_df.SEM_EXPIRY_DATE)
    filtered_df['SEM_UNDERLYING'] = [x[0] for x in filtered_df.SEM_CUSTOM_SYMBOL.str.split(' ')]
    filtered_df['Rank'] = filtered_df.groupby('SEM_UNDERLYING')['SEM_EXPIRY_DATE_CUSTOM'].rank(method='dense', ascending=True)
    filtered_df['Series'] = filtered_df['Rank'].apply(option_age)

    # For MCX we are restricting to current month contract only due to liquidity
    filtered_df = filtered_df[((filtered_df['SEM_EXM_EXCH_ID'] == 'MCX') & (filtered_df['Series'] == 'C')) | 
                              (filtered_df.Series != 'F')]  # We are removing the Farnext series due to better memory management

    # Overwrite the sheet called "Options" in Dhan_orders.xlsx
    options_sheet.clear()  # Clear existing content
    options_sheet.range('A1').options(index=False).value = filtered_df[['SEM_SMST_SECURITY_ID', 'SEM_LOT_UNITS', 'SEM_CUSTOM_SYMBOL', 'SEM_STRIKE_PRICE', 'SEM_OPTION_TYPE', 'SEM_UNDERLYING', 'Series']]    
    #--------------------------------------------------------------------------------------------
    # index
    #------------------------------------
    mcx_fut =  security_list[(security_list['SEM_EXM_EXCH_ID']=='MCX') & 
                             (security_list['SM_SYMBOL_NAME']=='CRUDEOIL') & 
                             (security_list['SEM_INSTRUMENT_NAME']=='FUTCOM')]
    index = security_list[(security_list['SEM_INSTRUMENT_NAME'] == 'INDEX') & (
        (security_list['SEM_TRADING_SYMBOL']=='BANKNIFTY') |
        (security_list['SEM_TRADING_SYMBOL']=='NIFTY')) & (security_list['SEM_SEGMENT']=='I')]
    filtered_df_index = pd.concat([mcx_fut,index])
    filtered_df_index = filtered_df_index.copy()
    filtered_df_index['SEM_TRADING_SYMBOL']= [x[0] for x in filtered_df_index.SEM_TRADING_SYMBOL.str.split('-')]

    filtered_df_index['Rank'] = filtered_df_index.groupby('SEM_EXM_EXCH_ID')['SEM_EXPIRY_DATE'].rank(method='dense', ascending=True)
    filtered_df_index = filtered_df_index[(filtered_df_index['Rank']==1.0) | (np.isnan(filtered_df_index['Rank']))]
    # Overwrite the sheet called "Options" in Dhan_orders.xlsx
    underlying_sheet = workbook.sheets['Underlying']    
    underlying_sheet.clear()
    underlying_sheet.range('A1').options(index=False).value = filtered_df_index[['SEM_SMST_SECURITY_ID','SEM_TRADING_SYMBOL']]    
    return({
        "excel_file": excel_file,
        "workbook" : workbook,
        "config_sheet" : config_sheet,
        "home_sheet" : home_sheet,
        "underlying_sheet" : underlying_sheet,
        "options_sheet" : options_sheet,
        "crude_options_chain_sheet":crude_options_chain_sheet,
        "client_id":client_id,
        "access_token" : access_token
    })

crude_instruments = []

    
def subscription_management(underlying_sheet,crude_options_chain_sheet):
    CRUDE_CE_CURRENT = crude_options_chain_sheet.range('D3:D23').value
    index_instruments = [
    (marketfeed.MCX, f"{str(int(underlying_sheet.range('A2').value))}", marketfeed.Ticker),   # Crudeoil
    (marketfeed.IDX, f"{str(int(underlying_sheet.range('A3').value))}", marketfeed.Ticker),      # NIFTY
    (marketfeed.IDX, f"{str(int(underlying_sheet.range('A4').value))}", marketfeed.Ticker)       # BANKNIFTY
    ]
    new_crude_instruments = [(marketfeed.MCX,f"{str(int(x))}",marketfeed.Ticker) for x in CRUDE_CE_CURRENT]

    instruments = index_instruments + crude_instruments
    return(instruments)



   
def run_feed(home_sheet,clientid,accesstoken,crude_options_sheet,crude_keys,instruments):    
    # WebSocket for real-time LTP updates
    #cid = "1100381471"
    #ac_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzMzOTIyMTM4LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDM4MTQ3MSJ9.byjw_4xYApTsQEc7s6FGNaEcSTBE6NSsWE3fCsBz3a0D2gtP0uYqdB3WG9EIKVaBaDP-JDLvM48_hXaIBbwKCQ"
    version = "v2"
    try:
        data = marketfeed.DhanFeed(clientid, accesstoken, instruments, version)
        while True:
            data.run_forever()
            response = data.get_data()
            if response['security_id'] == 435823 and response['type'] == 'Ticker Data':
                home_sheet.range('B2').value = response['LTP']  # Update Crudeoil LTP in A2
            if response['security_id'] == int(crude_keys[0]) and response['type'] == 'Ticker Data':
                crude_options_sheet.range('E2').value = response['LTP']  # Update Crudeoil LTP in A2
            if response['security_id'] == int(crude_keys[10]) and response['type'] == 'Ticker Data':
                crude_options_sheet.range('E13').value = response['LTP']  # Update Crudeoil LTP in A2                
                print(response)
            #print(response)
    except Exception as e:
        print(e)
    finally:
        # Close Connection if it was opened
        try:
            data.disconnect()
        except:
            pass

def main():
    trade_workbook = configure_the_workbook()
    instruments = subscription_management(trade_workbook['underlying_sheet'],trade_workbook['crude_options_chain_sheet'])
    crude_keys = [x[1] for x in instruments[3:]]
    print(instruments)
    while True:
        run_feed(trade_workbook['home_sheet'],trade_workbook['client_id'],trade_workbook['access_token'],trade_workbook['crude_options_chain_sheet'],crude_keys,instruments)

if __name__ == "__main__":
    main()

