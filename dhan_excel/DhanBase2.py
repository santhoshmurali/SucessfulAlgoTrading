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
    print(dhan)
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



   
def run_feed(clientid,accesstoken,instruments):    
    # WebSocket for real-time LTP updates
    #cid = "1100381471"
    #ac_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzMzOTIyMTM4LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDM4MTQ3MSJ9.byjw_4xYApTsQEc7s6FGNaEcSTBE6NSsWE3fCsBz3a0D2gtP0uYqdB3WG9EIKVaBaDP-JDLvM48_hXaIBbwKCQ"
    version = "v2"
    last_subscription_time = time.time()
    try:
        crude_key = instruments[0][1]
        nifty_key = instruments[1][1]
        banknifty_key = instruments[2][1]
        crude_keys_ce = [x[1] for x in instruments[3:14]]
        crude_keys_pe = [x[1] for x in instruments[14:25]]
        nifty_keys_ce = [x[1] for x in instruments[25:36]]
        nifty_keys_pe = [x[1] for x in instruments[36:47]]
        banknifty_keys_ce = [x[1] for x in instruments[47:58]]
        banknifty_keys_pe = [x[1] for x in instruments[58:69]]
        data = marketfeed.DhanFeed(clientid, accesstoken, instruments, version)
        while True:
            # Check if 5 minutes have passed
            current_time = time.time()
            if current_time - last_subscription_time >= 120:  # 300 seconds = 5 minutes
                instruments = subscription_management()  # Update instruments by calling subscription_management
                crude_key = instruments[0][1]
                nifty_key = instruments[1][1]
                banknifty_key = instruments[2][1]
                crude_keys_ce = [x[1] for x in instruments[3:14]]
                crude_keys_pe = [x[1] for x in instruments[14:25]]
                nifty_keys_ce = [x[1] for x in instruments[25:36]]
                nifty_keys_pe = [x[1] for x in instruments[36:47]]
                banknifty_keys_ce = [x[1] for x in instruments[47:58]]
                banknifty_keys_pe = [x[1] for x in instruments[58:69]]                
                data = marketfeed.DhanFeed(clientid, accesstoken, instruments, version)
                pd.DataFrame({'Time':[time.time()]}).to_csv(r"D:\AlgoTrading\Books\Sucessful Algorithmic Trading\SucessfulAlgoTrading\dhan_excel\update.csv")
                print("Subscription updated.")
                last_subscription_time = current_time  # Reset the timer

            data.run_forever()
            response = data.get_data()
            if response['security_id'] == int(crude_key) and response['type'] == 'Ticker Data':
                crude_options_chain_sheet.range('C2').value = response['LTP']  # Update Crudeoil LTP in C2
            if response['security_id'] == int(nifty_key) and response['type'] == 'Ticker Data':
                nifty_options_chain_sheet.range('C2').value = response['LTP']  # Update NIFTY LTP in AC2                
            if response['security_id'] == int(banknifty_key) and response['type'] == 'Ticker Data':
                banknifty_options_chain_sheet.range('C2').value = response['LTP']  # Update BANKNIFTY LTP in AC2                  
            ### Crude CE
            if response['security_id'] == int(crude_keys_ce[0]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('E6').value = response['LTP']  
            if response['security_id'] == int(crude_keys_ce[1]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('E7').value = response['LTP']  
            if response['security_id'] == int(crude_keys_ce[2]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('E8').value = response['LTP']  
            if response['security_id'] == int(crude_keys_ce[3]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('E9').value = response['LTP']  
            if response['security_id'] == int(crude_keys_ce[4]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('E10').value = response['LTP']  
            if response['security_id'] == int(crude_keys_ce[5]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('E11').value = response['LTP']                                                                                  
            if response['security_id'] == int(crude_keys_ce[6]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('E12').value = response['LTP']  
            if response['security_id'] == int(crude_keys_ce[7]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('E13').value = response['LTP']  
            if response['security_id'] == int(crude_keys_ce[8]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('E14').value = response['LTP']  
            if response['security_id'] == int(crude_keys_ce[9]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('E15').value = response['LTP']  
            if response['security_id'] == int(crude_keys_ce[10]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('E16').value = response['LTP']  
            ### Crude PE
            if response['security_id'] == int(crude_keys_pe[0]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('H6').value = response['LTP']  
            if response['security_id'] == int(crude_keys_pe[1]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('H7').value = response['LTP']  
            if response['security_id'] == int(crude_keys_pe[2]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('H8').value = response['LTP']  
            if response['security_id'] == int(crude_keys_pe[3]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('H9').value = response['LTP']  
            if response['security_id'] == int(crude_keys_pe[4]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('H10').value = response['LTP']  
            if response['security_id'] == int(crude_keys_pe[5]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('H11').value = response['LTP']                                                                                  
            if response['security_id'] == int(crude_keys_pe[6]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('H12').value = response['LTP']  
            if response['security_id'] == int(crude_keys_pe[7]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('H13').value = response['LTP']  
            if response['security_id'] == int(crude_keys_pe[8]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('H14').value = response['LTP']  
            if response['security_id'] == int(crude_keys_pe[9]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('H15').value = response['LTP']  
            if response['security_id'] == int(crude_keys_pe[10]) and response['type'] == 'Ticker Data': #Options Pricing
                crude_options_chain_sheet.range('H16').value = response['LTP']     
            ### NIFTY CE
            if response['security_id'] == int(nifty_keys_ce[0]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('E6').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_ce[1]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('E7').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_ce[2]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('E8').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_ce[3]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('E9').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_ce[4]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('E10').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_ce[5]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('E11').value = response['LTP']                                                                                  
            if response['security_id'] == int(nifty_keys_ce[6]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('E12').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_ce[7]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('E13').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_ce[8]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('E14').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_ce[9]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('E15').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_ce[10]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('E16').value = response['LTP']  
            ### NIFTY PE
            if response['security_id'] == int(nifty_keys_pe[0]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('H6').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_pe[1]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('H7').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_pe[2]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('H8').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_pe[3]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('H9').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_pe[4]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('H10').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_pe[5]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('H11').value = response['LTP']                                                                                  
            if response['security_id'] == int(nifty_keys_pe[6]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('H12').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_pe[7]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('H13').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_pe[8]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('H14').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_pe[9]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('H15').value = response['LTP']  
            if response['security_id'] == int(nifty_keys_pe[10]) and response['type'] == 'Ticker Data': #Options Pricing
                nifty_options_chain_sheet.range('H16').value = response['LTP']     
            
            ### BANKNIFTY CE
            if response['security_id'] == int(banknifty_keys_ce[0]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('E6').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_ce[1]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('E7').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_ce[2]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('E8').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_ce[3]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('E9').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_ce[4]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('E10').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_ce[5]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('E11').value = response['LTP']                                                                                  
            if response['security_id'] == int(banknifty_keys_ce[6]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('E12').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_ce[7]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('E13').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_ce[8]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('E14').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_ce[9]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('E15').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_ce[10]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('E16').value = response['LTP']  
            ### BANKNIFTY PE
            if response['security_id'] == int(banknifty_keys_pe[0]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('H6').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_pe[1]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('H7').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_pe[2]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('H8').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_pe[3]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('H9').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_pe[4]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('H10').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_pe[5]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('H11').value = response['LTP']                                                                                  
            if response['security_id'] == int(banknifty_keys_pe[6]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('H12').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_pe[7]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('H13').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_pe[8]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('H14').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_pe[9]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('H15').value = response['LTP']  
            if response['security_id'] == int(banknifty_keys_pe[10]) and response['type'] == 'Ticker Data': #Options Pricing
                banknifty_options_chain_sheet.range('H16').value = response['LTP']  

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
   
    print(instruments)
    while True:
        run_feed(trade_workbook['client_id'],trade_workbook['access_token'],instruments)

if __name__ == "__main__":
    main()

