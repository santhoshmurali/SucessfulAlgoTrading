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
excel_file = 'Dhan_orders.xlsx'
workbook = xw.Book(excel_file)
config_sheet = workbook.sheets['Config']
#home_sheet = workbook.sheets['Home']
index_sheet = workbook.sheets['Index'] 
options_sheet = workbook.sheets['Options']
crude_options_chain_sheet = workbook.sheets['CRUDE']
nifty_options_chain_sheet = workbook.sheets['NIFTY']
banknifty_options_chain_sheet = workbook.sheets['BANKNIFTY']



CRUDE_AVAILABLE_CAPITAL = crude_options_chain_sheet.range('G1').value
NIFTY_AVAILABLE_CAPITAL = nifty_options_chain_sheet.range('G1').value
BANKNIFTY_AVAILABLE_CAPITAL = banknifty_options_chain_sheet.range('G1').value




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
    connections__= connect_to_dhan() #returned as dictionary but accessed like a list
    
    
    dhan = connections__['connection']
    client_id = connections__['client_id']
    access_token = connections__['access_token']
    # Load the latest keys and scripts metadata automatically
    
    security_list = dhan.fetch_security_list("compact")
    
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

    # For MCX we are restricting to current month contract only due to liquidity
    filtered_df = filtered_df[((filtered_df['SEM_EXM_EXCH_ID'] == 'MCX') & (filtered_df['Series'] == 'C')) | 
                              (filtered_df.Series != 'F')]  # We are removing the Farnext series due to better memory management

    # Overwrite the sheet called "Options" in Dhan_orders.xlsx
    options_sheet.clear()  # Clear existing content
    options_sheet.range('A1').options(index=False).value = filtered_df[['SEM_SMST_SECURITY_ID', 'SEM_LOT_UNITS', 'SEM_CUSTOM_SYMBOL', 'SEM_STRIKE_PRICE', 'SEM_OPTION_TYPE', 'SEM_UNDERLYING', 'Series']]    
    
    
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
    

       
    index_sheet.clear()
    index_sheet.range('A1').options(index=False).value = filtered_df_index[['SEM_SMST_SECURITY_ID','SEM_TRADING_SYMBOL']]    
    return({
        "client_id":client_id,
        "access_token" : access_token
    })



    
def subscription_management():
    instruments = []
    CRUDE_CE_CURRENT = crude_options_chain_sheet.range('D6:D16').value
    CRUDE_PE_CURRENT = crude_options_chain_sheet.range('G6:G16').value
    NIFTY_CE_CURRENT = nifty_options_chain_sheet.range('D6:D16').value
    NIFTY_PE_CURRENT = nifty_options_chain_sheet.range('G6:G16').value
    BANKNIFTY_CE_CURRENT = banknifty_options_chain_sheet.range('D6:D16').value
    BANKNIFTY_PE_CURRENT = banknifty_options_chain_sheet.range('G6:G16').value
    index_instruments = [
    (marketfeed.MCX, f"{str(int(index_sheet.range('A2').value))}", marketfeed.Ticker),   # Crudeoil
    (marketfeed.IDX, f"{str(int(index_sheet.range('A3').value))}", marketfeed.Ticker),      # NIFTY
    (marketfeed.IDX, f"{str(int(index_sheet.range('A4').value))}", marketfeed.Ticker)       # BANKNIFTY
    ]
    new_crude_instruments_ce = [(marketfeed.MCX,f"{str(int(x))}",marketfeed.Ticker) for x in CRUDE_CE_CURRENT]
    new_crude_instruments_pe = [(marketfeed.MCX,f"{str(int(x))}",marketfeed.Ticker) for x in CRUDE_PE_CURRENT]
    new_nifty_instruments_ce = [(marketfeed.NSE_FNO,f"{str(int(x))}",marketfeed.Ticker) for x in NIFTY_CE_CURRENT]
    new_nifty_instruments_pe = [(marketfeed.NSE_FNO,f"{str(int(x))}",marketfeed.Ticker) for x in NIFTY_PE_CURRENT]
    new_banknifty_instruments_ce = [(marketfeed.NSE_FNO,f"{str(int(x))}",marketfeed.Ticker) for x in BANKNIFTY_CE_CURRENT]
    new_banknifty_instruments_pe = [(marketfeed.NSE_FNO,f"{str(int(x))}",marketfeed.Ticker) for x in BANKNIFTY_PE_CURRENT]

    instruments = index_instruments + new_crude_instruments_ce + new_crude_instruments_pe + new_nifty_instruments_ce + new_nifty_instruments_pe + new_banknifty_instruments_ce + new_banknifty_instruments_pe
    return(instruments)


def convert_to_dict(instruments):
    crude_keys_ce = [x[1] for x in instruments[3:14]]
    crude_keys_pe = [x[1] for x in instruments[14:25]]
    nifty_keys_ce = [x[1] for x in instruments[25:36]]
    nifty_keys_pe = [x[1] for x in instruments[36:47]]
    banknifty_keys_ce = [x[1] for x in instruments[47:58]]
    banknifty_keys_pe = [x[1] for x in instruments[58:69]]
    ce_col = "E"
    pe_col = "H"
    crude_to_cell_map_ce = {
        int(crude_keys_ce[i]): f'{ce_col}{i + 6}' for i in range(len(crude_keys_ce))
    }
    crude_to_cell_map_pe = {
        int(crude_keys_pe[i]): f'{pe_col}{i + 6}' for i in range(len(crude_keys_pe))
    }
    crude_to_cell = crude_to_cell_map_ce | crude_to_cell_map_pe
    nifty_to_cell_map_ce = {
        int(nifty_keys_ce[i]): f'{ce_col}{i + 6}' for i in range(len(nifty_keys_ce))
    }
    nifty_to_cell_map_pe = {
        int(nifty_keys_pe[i]): f'{pe_col}{i + 6}' for i in range(len(nifty_keys_pe))
    }
    nifty_to_cell = nifty_to_cell_map_ce | nifty_to_cell_map_pe
    nifty_to_cell_map_ce = {
        int(banknifty_keys_ce[i]): f'{ce_col}{i + 6}' for i in range(len(banknifty_keys_ce))
    }
    nifty_to_cell_map_pe = {
        int(banknifty_keys_pe[i]): f'{pe_col}{i + 6}' for i in range(len(banknifty_keys_pe))
    }
    banknifty_to_cell = nifty_to_cell_map_ce | nifty_to_cell_map_pe   
    security_to_cell = {
        'crude_to_cell' : crude_to_cell,
        'nifty_to_cell' : nifty_to_cell,
        'banknifty_to_cell' : banknifty_to_cell,
        'crude_len' : len(crude_to_cell)/2,
        'nifty_len' : len(nifty_to_cell)/2,
        'banknifty_len' : len(banknifty_to_cell)/2,

    } 
    return(security_to_cell)

   
def run_feed(clientid,accesstoken,instruments,dhan,CORRELATION_ID):    
    # WebSocket for real-time LTP updates
    #cid = "1100381471"
    #ac_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzMzOTIyMTM4LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDM4MTQ3MSJ9.byjw_4xYApTsQEc7s6FGNaEcSTBE6NSsWE3fCsBz3a0D2gtP0uYqdB3WG9EIKVaBaDP-JDLvM48_hXaIBbwKCQ"
    version = "v2"
    last_subscription_time = time.time()
    ce_col_o = "J"
    ce_col_activated = "K"
    pe_col_o = "L"
    
    try:
        instruments = subscription_management()  # Update instruments by calling subscription_management
        crude_key = instruments[0][1]
        nifty_key = instruments[1][1]
        banknifty_key = instruments[2][1]
        security_to_cell = convert_to_dict(instruments)
        data = marketfeed.DhanFeed(clientid, accesstoken, instruments, version)
        NIFTY_STRIKE_CHECK = nifty_options_chain_sheet.range('G6')
        BANKNIFTY_STRIKE_CHECK = banknifty_options_chain_sheet.range('G6')
        CRUDE_STRIKE_CHECK = crude_options_chain_sheet.range('G6')
        while True:
            #Trade Flag
            refresh = config_sheet.range('B1').value
            CRUDE_TRADE = config_sheet.range('B3').value
            NIFTY_TRADE = config_sheet.range('B4').value
            BANKNIFTY_TRADE = config_sheet.range('B5').value
            if refresh:  
                config_sheet.range('B1').value = False
                refresh_start_time = time.time()
                instruments = subscription_management()  # Update instruments by calling subscription_management
                crude_key = instruments[0][1]
                nifty_key = instruments[1][1]
                banknifty_key = instruments[2][1]
                security_to_cell = convert_to_dict(instruments)
                data.disconnect()          
                data = marketfeed.DhanFeed(clientid, accesstoken, instruments, version)
                refresh_end_time = time.time()
                pd.DataFrame({'Time':[f"{(refresh_end_time - refresh_start_time)*1000} milliseconds for refresh"]}).to_csv(r"D:\AlgoTrading\Books\Sucessful Algorithmic Trading\SucessfulAlgoTrading\dhan_excel\update.csv")
                print("Subscription updated.")

            if ((NIFTY_TRADE and NIFTY_STRIKE_CHECK != nifty_options_chain_sheet.range('G6')) or (BANKNIFTY_TRADE and BANKNIFTY_STRIKE_CHECK != banknifty_options_chain_sheet.range('G6')) or (CRUDE_TRADE and CRUDE_STRIKE_CHECK != crude_options_chain_sheet.range('G6') )):
                refresh_start_time = time.time()
                instruments = subscription_management()  # Update instruments by calling subscription_management
                crude_key = instruments[0][1]
                nifty_key = instruments[1][1]
                banknifty_key = instruments[2][1]
                security_to_cell = convert_to_dict(instruments)
                data.disconnect()          
                data = marketfeed.DhanFeed(clientid, accesstoken, instruments, version)
                refresh_end_time = time.time()
                pd.DataFrame({'Time':[f"{(refresh_end_time - refresh_start_time)*1000} milliseconds for refresh"]}).to_csv(r"D:\AlgoTrading\Books\Sucessful Algorithmic Trading\SucessfulAlgoTrading\dhan_excel\update.csv")
                NIFTY_STRIKE_CHECK = nifty_options_chain_sheet.range('G6')
                BANKNIFTY_STRIKE_CHECK = banknifty_options_chain_sheet.range('G6')
                CRUDE_STRIKE_CHECK = crude_options_chain_sheet.range('G6')
                print("Subscription updated based on Strike Change.")                
            
            
            data.run_forever()
            response = data.get_data()
            if response['security_id'] == int(crude_key) and response['type'] == 'Ticker Data' and CRUDE_TRADE:
                crude_options_chain_sheet.range('C2').value = response['LTP']  # Update Crudeoil LTP in C2
            if response['security_id'] == int(nifty_key) and response['type'] == 'Ticker Data' and NIFTY_TRADE:
                nifty_options_chain_sheet.range('C2').value = response['LTP']  # Update NIFTY LTP in AC2                
            if response['security_id'] == int(banknifty_key) and response['type'] == 'Ticker Data' and BANKNIFTY_TRADE:
                banknifty_options_chain_sheet.range('C2').value = response['LTP']  # Update BANKNIFTY LTP in AC2                  
            
            # Process the response using the dictionary # Wrting the LTP of Options in respective Cell address
            if response['type'] == 'Ticker Data':
                security_id = int(response['security_id'])
                if security_id in security_to_cell['crude_to_cell'] and CRUDE_TRADE:
                    cell_reference = security_to_cell['crude_to_cell'][security_id]
                    crude_options_chain_sheet.range(cell_reference).value = response['LTP']
                elif security_id in security_to_cell['nifty_to_cell'] and NIFTY_TRADE:
                    cell_reference = security_to_cell['nifty_to_cell'][security_id]
                    nifty_options_chain_sheet.range(cell_reference).value = response['LTP']     
                elif security_id in security_to_cell['banknifty_to_cell'] and BANKNIFTY_TRADE:
                    cell_reference = security_to_cell['banknifty_to_cell'][security_id]
                    banknifty_options_chain_sheet.range(cell_reference).value = response['LTP']    
                                                               
            print(response)
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
    instruments = subscription_management()
    client_id = trade_workbook['client_id']
    access_token = trade_workbook['access_token']
    CORRELATIONID = 'test order sell'
    connections__= connect_to_dhan() #returned as dictionary but accessed like a list
    dhan = connections__['connection']
    while True:
        run_feed(client_id, access_token, instruments,dhan,CORRELATIONID)


if __name__ == "__main__":
    main()

