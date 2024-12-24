from os import access
import xlwings as xw
from connect_to_dhan import Connection
from dhanhq import dhanhq
import numpy as np
import pandas as pd
from dhanhq import marketfeed
import yaml
import time



""" configuring the workbook and getting the security info
"""
# Load Excel file
excel_file = 'DhanTrading.xlsx'
workbook = xw.Book(excel_file)
TradeSheet = workbook.sheets['Trade']
OptionsLookUp = workbook.sheets['OptionsLookUp'] 
IndexLookup = workbook.sheets['IndexLookup']


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



def initial_sheet_config():
    connections__= connect_to_dhan() #returned as dictionary but accessed like a list
    dhan = connections__['connection']
    client_id = connections__['client_id']
    access_token = connections__['access_token']
    # Load the latest keys and scripts metadata automatically
    security_list = dhan.fetch_security_list("compact")
    #--------------------------------------------------------------------------------------------
    # index
    #--------------------------------------------------------------------------------------------
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
    IndexLookup.clear()
    IndexLookup.range('A1').options(index=False).value = filtered_df_index[['SEM_TRADING_SYMBOL','SEM_SMST_SECURITY_ID']]
    #--------------------------------------------------------------------------------------------
    # Filter for only NIFTY and BANKNIFTY OPTIONS and MCX
    #--------------------------------------------------------------------------------------------
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
    filtered_df = filtered_df.copy()  

    # Below logic will help us to classify the Series type {C - Current, N - Next and F - Future}
    filtered_df['SEM_EXPIRY_DATE_CUSTOM'] = np.nan
    filtered_df['SEM_EXPIRY_DATE_CUSTOM'] = pd.to_datetime(filtered_df.SEM_EXPIRY_DATE)
    filtered_df['SEM_UNDERLYING'] = [x[0] for x in filtered_df.SEM_CUSTOM_SYMBOL.str.split(' ')]
    filtered_df['Rank'] = filtered_df.groupby('SEM_UNDERLYING')['SEM_EXPIRY_DATE_CUSTOM'].rank(method='dense', ascending=True)
    filtered_df['Series'] = filtered_df['Rank'].apply(option_age)

    filtered_df = filtered_df[filtered_df['Series'] == 'C'].copy(deep=True)
    filtered_df['SEM_STRIKE_PRICE'] = filtered_df['SEM_STRIKE_PRICE'].astype(int)
    filtered_df = filtered_df[['SEM_SMST_SECURITY_ID','SEM_UNDERLYING','SEM_OPTION_TYPE','SEM_STRIKE_PRICE']].copy(deep=True)
    filtered_df['Series'] = filtered_df['SEM_UNDERLYING']+"_"+filtered_df['SEM_OPTION_TYPE']+"_"+filtered_df['SEM_STRIKE_PRICE'].astype(str)
    OptionsLookUp.clear()
    OptionsLookUp.range('A1').options(index=False).value = filtered_df[['Series','SEM_SMST_SECURITY_ID']]
    return({
        "connection":dhan,
        "client_id":client_id,
        "access_token" : access_token
    })



def refresh_instruments(change_instrument, dhan):
  
    if TradeSheet.range("INSTRUMENT").value == "CRUDEOIL":
        market_feed = "MCX_COMM"
        index_key = str(int(TradeSheet.range("IndexKey").value))
    else:
        market_feed = "IDX_I"
        index_key = str(int(TradeSheet.range("IndexKey").value))
    lp_dict =  dhan.ohlc_data(securities={market_feed:[int(index_key)]})       
    lp = lp_dict['data']['data'][market_feed][index_key]['last_price']
    TradeSheet.range("INDEX_LTP").value =  lp
    if change_instrument:
        time.sleep(2)
    atm_key = str(int(TradeSheet.range("ATM_KEY").value))
    itm_1_key = str(int(TradeSheet.range("ITM_ONE_KEY").value))
    itm_2_key = str(int(TradeSheet.range("ITM_TWO_KEY").value))
    itm_3_key = str(int(TradeSheet.range("ITM_THREE_KEY").value))
    range_mapping = {
        index_key : 'INDEX_LTP',
        atm_key : 'ATM_LTP',
        itm_1_key : 'ITM_ONE_LTP',
        itm_2_key : 'ITM_TWO_LTP',
        itm_3_key : 'ITM_THREE_LTP'
    }
    return(range_mapping)


def prepare_instruments(instrument_keys):
    instruments = []
    if TradeSheet.range("INSTRUMENT").value == "CRUDEOIL":
        market_feed = marketfeed.MCX
    else:
        market_feed = marketfeed.NSE_FNO

    index_key = [(market_feed,instrument_keys[0],marketfeed.Ticker)]
    options_key = [(market_feed,x,marketfeed.Ticker) for x in instrument_keys[1:] ]
    
    instruments = index_key + options_key
    return(instruments)



def run_feed(clientid,accesstoken,dhan):
    version = "v2"
    try:
        instruments_all = refresh_instruments(True,dhan)  # Update instruments by calling subscription_management
        instruments = list(instruments_all.keys())
        
        prepared_instruments = prepare_instruments(instruments)
        security_to_cell = instruments_all
        data = marketfeed.DhanFeed(clientid, accesstoken, prepared_instruments, version)
        STRIKE_CHECK = TradeSheet.range('ATM_KEY').value
        INDEX_KEY =  TradeSheet.range('IndexKey').value
        while True:
            #Trade Flag
            refresh = TradeSheet.range("Refresh").value
            if refresh:  
                TradeSheet.range("Refresh").value = False
                refresh_start_time = time.time()
                instruments_all = refresh_instruments(False,dhan)  # Update instruments by calling subscription_management
                instruments = list(instruments_all.keys())
                prepared_instruments = prepare_instruments(instruments)
                security_to_cell = instruments_all
                data.disconnect()          
                data = marketfeed.DhanFeed(clientid, accesstoken, prepared_instruments, version)
                refresh_end_time = time.time()
                #pd.DataFrame({'Time':[f"{(refresh_end_time - refresh_start_time)*1000} milliseconds for refresh"]}).to_csv(r"D:\AlgoTrading\Books\Sucessful Algorithmic Trading\SucessfulAlgoTrading\dhan_excel\update.csv")
                print("Subscription updated.")

            if ( STRIKE_CHECK != TradeSheet.range('ATM_KEY').value ):
                refresh_start_time = time.time()
                instruments_all = refresh_instruments(False,dhan)  # Update instruments by calling subscription_management
                instruments = list(instruments_all.keys())
                prepared_instruments = prepare_instruments(instruments)
                security_to_cell = instruments_all
                data.disconnect()          
                data = marketfeed.DhanFeed(clientid, accesstoken, prepared_instruments, version)
                refresh_end_time = time.time()
                #pd.DataFrame({'Time':[f"{(refresh_end_time - refresh_start_time)*1000} milliseconds for refresh"]}).to_csv(r"D:\AlgoTrading\Books\Sucessful Algorithmic Trading\SucessfulAlgoTrading\dhan_excel\update.csv")
                STRIKE_CHECK = TradeSheet.range('ATM_KEY').value
                print("Subscription updated based on Strike Change.")           

            if (INDEX_KEY != TradeSheet.range('IndexKey').value):
                refresh_start_time = time.time()
                instruments_all = refresh_instruments(True,dhan)  # Update instruments by calling subscription_management
                instruments = list(instruments_all.keys())
                prepared_instruments = prepare_instruments(instruments)
                security_to_cell = instruments_all
                data.disconnect()          
                data = marketfeed.DhanFeed(clientid, accesstoken, prepared_instruments, version)
                refresh_end_time = time.time()
                #pd.DataFrame({'Time':[f"{(refresh_end_time - refresh_start_time)*1000} milliseconds for refresh"]}).to_csv(r"D:\AlgoTrading\Books\Sucessful Algorithmic Trading\SucessfulAlgoTrading\dhan_excel\update.csv")
                INDEX_KEY =  TradeSheet.range('IndexKey').value
                print("Subscription updated based on Strike Change.") 
            
            data.run_forever()
            response = data.get_data()
           

            if response['security_id'] == int(instruments[0]) and response['type'] == 'Ticker Data':
               TradeSheet.range("INDEX_LTP").value = response['LTP']  # Update index ltp
           
           
            if response['type'] == 'Ticker Data' and response['security_id'] != int(instruments[0]) :
                security_id = str(response['security_id'])
                cell_ref = security_to_cell[security_id]
                TradeSheet.range(cell_ref).value = response['LTP']
            print(response)


                                                               
    except Exception as e:
         print(e)
    finally:
        # Close Connection if it was opened
        try:
            data.disconnect()
        except:
            pass
    pass


def main():
    set_up_worksheet = initial_sheet_config()
    print("Connected to Dhan Data!")
    dhan = set_up_worksheet['connection']
    cid = set_up_worksheet['client_id']
    atoken = set_up_worksheet['access_token']
    while True:
        run_feed(cid,atoken,dhan)

if __name__ == "__main__":
    main()

