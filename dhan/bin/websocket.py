from dhanhq import dhanhq
import yaml
import pandas  as pd
import numpy as np
from dhanhq import marketfeed

# Authentication
CONFIG_FILE_PATH = r"..\\configs\\config.yaml" #This has the API key and client id
with open(CONFIG_FILE_PATH,'r') as config:
    api_config = yaml.safe_load(config)
APPLICAITON_ID = api_config.get('api_config')[0]
CLIENT_ID = api_config.get('api_config')[1]
ACCESS_TOKEN = api_config.get('api_config')[2]
EXP_HOURS = api_config.get('api_config')[3]
dhan = dhanhq(CLIENT_ID,ACCESS_TOKEN)

# Getting other variables
OTHER_CONFIG_FILE_PATH = r"..\\configs\\other_config.yaml" # This we will configure all other variables
with open(OTHER_CONFIG_FILE_PATH,'r') as other_configs:
    gen_config = yaml.safe_load(other_configs)

strike_size = gen_config.get('strike_size')
underlying = gen_config.get('index_file_metadata')
options = gen_config.get('options_file_metadata')

underlying = pd.read_excel('underlying.xlsx')
options = pd.read_excel('Options.xlsx')

UNDERLYING_INDEX = "NIFTY"

def get_otms_itma(SPOT):
    INDEX_CE_ITM = {"ITM" :
    {
        x : SPOT - ((SPOT%strike_size[UNDERLYING_INDEX]) + (x*strike_size[UNDERLYING_INDEX])) for x in range(10)
    }
    }
    INDEX_PE_ITM =  {"ITM" :
    {
        x : SPOT - ((SPOT%strike_size[UNDERLYING_INDEX]) - ((x+1)*strike_size[UNDERLYING_INDEX])) for x in range(10)
    }
    }
    INDEX_ATM = SPOT - (SPOT%strike_size[UNDERLYING_INDEX])
    return({"CE" : INDEX_CE_ITM, "PE" : INDEX_PE_ITM, "ATM":INDEX_ATM})

def get_index_ltp(dhanO, INDEX):
    INDEX_ID = str(underlying.loc[(underlying.SEM_TRADING_SYMBOL == INDEX),['SEM_SMST_SECURITY_ID']].squeeze())
    INDEX_LTP = dhanO.ohlc_data(securities = {"IDX_I":[int(INDEX_ID)]})['data']['data']['IDX_I'][f'{INDEX_ID}']['last_price']
    STRIKE_PRICES = get_otms_itma(INDEX_LTP)
    return {"INDEX": INDEX, "SPOT": INDEX_LTP, "STRIKES":STRIKE_PRICES}

INDEX_LTP_STRIKE = get_index_ltp(dhan,UNDERLYING_INDEX)

INDEX_ID = underlying.loc[(underlying['SEM_TRADING_SYMBOL']==INDEX_LTP_STRIKE['INDEX']),'SEM_SMST_SECURITY_ID'].squeeze()

instruments = [(marketfeed.NSE, "1333", marketfeed.Ticker),   # Ticker - Ticker Data
    (marketfeed.NSE, "1333", marketfeed.Quote),     # Quote - Quote Data
    (marketfeed.NSE, "1333", marketfeed.Full),      # Full - Full Packet
    (marketfeed.NSE, "11915", marketfeed.Ticker),
    (marketfeed.NSE, "11915", marketfeed.Full)]

version = "v2"          # Mention Version and set to latest version 'v2'
# In case subscription_type is left as blank, by default Ticker mode will be subscribed.

try:
    data = marketfeed.DhanFeed(CLIENT_ID, ACCESS_TOKEN, instruments, version)
    while True:
        data.run_forever()
        response = data.get_data()
        print(response)

except Exception as e:
    print(e)