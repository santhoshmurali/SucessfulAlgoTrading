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

#Trade Flag
CRUDE_TRADE = config_sheet.range('B3').value
NIFTY_TRADE = config_sheet.range('B4').value
BANKNIFTY_TRADE = config_sheet.range('B5').value

ce_input_prop = "I"
pe_input_prop = "L"
ce_col_o = "J" #Order Quantity for CE
ce_col_activated = "K" #If Order is activated for CE
pe_col_o = "M" #Order Quantity for PE   
pe_col_activated = "N" #If Order is activated for PE
CE_LTP = "E"
PE_LTP = "H"
CE_SCRIPT_KEY = "D"
PE_SCRIPT_KEY = "G"
COL_ORDER_ID = "O"
ORDER_STATUS = "P"
QTY = "Q"
AVG_PRICE = "R"
PROFIT_ORDER_NUMBER = "T"
PROFT_TGT = "U"	
PROFIT_ORDER_STATUS = "V"
PROFIT_ORDER_AVG_PRICE = "W"
SL_ORDER_NUMBER = "X"
SL_TGT = "Y"
SL_ORDER_STATUS = "Z"
SL_ORDER_AVG_PRICE = "AA"
LTP_ = "AB"
TRIGGER_PROFIT_PRICE = "AC"
TRIGGER_SL_PRICE = "AD"



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

#Order Placement Functions
def place_buy_order(dhan,script_key, order_qty, trade_price,segment,correlation_id):
    print(script_key)
    print(int(order_qty))
    print(np.round(float(trade_price),1))
    print(segment)
    SLICE_ORDER_BUY = dhan.place_order(security_id=str(int(script_key)), exchange_segment=segment, transaction_type=dhanhq.BUY, quantity=str(int(order_qty)),
                           order_type=dhanhq.LIMIT, product_type=dhanhq.INTRA, price=trade_price, trigger_price= trade_price-0.1, disclosed_quantity=0,
                           after_market_order=False, validity='DAY', amo_time='OPEN',
                           bo_profit_value=None, bo_stop_loss_Value=None, tag=correlation_id)
    print(SLICE_ORDER_BUY)
    return(SLICE_ORDER_BUY)


#Place Profit Order
def place_profit_order(dhan,script_key, order_qty, trade_price,segment,correlation_id):
    print(str(int(script_key)))
    print(int(order_qty))
    print(np.round(float(trade_price),1))
    print(segment)

    SLICE_ORDER_SELL_PROFIT = dhan.place_order(security_id=str(int(script_key)), exchange_segment=segment, transaction_type=dhanhq.SELL, quantity=int(order_qty),
                           order_type=dhanhq.LIMIT, product_type=dhanhq.INTRA, price=np.round(float(trade_price),1), trigger_price=0.0, disclosed_quantity=0, validity='DAY',amo_time='OPEN',
                            tag=correlation_id)
    print(SLICE_ORDER_SELL_PROFIT)
    return(SLICE_ORDER_SELL_PROFIT)

#Place SL Order
def place_sl_order(dhan,script_key, order_qty, trade_price,segment,correlation_id):
    print(str(int(script_key)))
    print(int(order_qty))
    print(trade_price)
    print(segment)
    SLICE_ORDER_SELL_LOSS = dhan.place_order(security_id=str(int(script_key)), exchange_segment=segment, transaction_type=dhanhq.SELL, quantity=int(order_qty),
                           order_type=dhanhq.SL, product_type=dhanhq.INTRA, price=np.round(float(trade_price)-0.2,1), trigger_price=np.round(float(trade_price),1), disclosed_quantity=0, 
                           validity='DAY', amo_time='OPEN',tag=correlation_id)
    print(SLICE_ORDER_SELL_LOSS)
    return(SLICE_ORDER_SELL_LOSS)

def place_cancel_order(dhan,orderid):
    CANCEL_ORDER = dhan.cancel_order(orderid)
    return(CANCEL_ORDER)


#Order Management
def get_order_details(dhan):
    # Buy Order Object
    CRUDE_BUY_ORDER_CE = [None for k in range(11)]
    CRUDE_BUY_ORDER_PE = [None for k in range(11)]
    NIFTY_BUY_ORDER_CE = [None for k in range(11)]
    NIFTY_BUY_ORDER_PE = [None for k in range(11)]
    BANKNIFTY_BUY_ORDER_CE = [None for k in range(11)]
    BANKNIFTY_BUY_ORDER_PE = [None for k in range(11)]

    # Sell Profit Object
    CRUDE_PROFIT_ORDER_CE = [None for k in range(11)]
    CRUDE_PROFIT_ORDER_PE = [None for k in range(11)]
    NIFTY_PROFIT_ORDER_CE = [None for k in range(11)]
    NIFTY_PROFIT_ORDER_PE = [None for k in range(11)]
    BANKNIFTY_PROFIT_ORDER_CE = [None for k in range(11)]
    BANKNIFTY_PROFIT_ORDER_PE = [None for k in range(11)]    

    # Sell Loss Object
    CRUDE_LOSS_ORDER_CE = [None for k in range(11)]
    CRUDE_LOSS_ORDER_PE = [None for k in range(11)]
    NIFTY_LOSS_ORDER_CE = [None for k in range(11)]
    NIFTY_LOSS_ORDER_PE = [None for k in range(11)]
    BANKNIFTY_LOSS_ORDER_CE = [None for k in range(11)]
    BANKNIFTY_LOSS_ORDER_PE = [None for k in range(11)]    

    

    while True:
        if CRUDE_TRADE :
            for strikes in range(11):
                # Crude CE 
                # Stage 1 : Check if the User initatd any orders, if yes. Place the Buy order and do not execute this if again. This is controlled using Activated flag in Excel mapped using  ce_col_activated
                if (crude_options_chain_sheet.range(f'{ce_col_o}{strikes+6}').value and crude_options_chain_sheet.range(f'{ce_col_activated}{strikes+6}').value == False): 
                    Quantity = crude_options_chain_sheet.range(f'{ce_col_o}{strikes+6}').value
                    Script_Key = crude_options_chain_sheet.range(f'{CE_SCRIPT_KEY}{strikes+6}').value
                    LTP = crude_options_chain_sheet.range(f'{CE_LTP}{strikes+6}').value
                    buy_order  = place_buy_order(dhan,Script_Key,Quantity,LTP-0.1,dhanhq.MCX,"SPARK_CRUDE_CALL_BUY")
                    print(buy_order)
                    CRUDE_BUY_ORDER_CE[strikes] = buy_order

                    crude_options_chain_sheet.range(f'{ce_col_activated}{strikes+6}').value = True
                    crude_options_chain_sheet.range(f'{COL_ORDER_ID}{strikes+6}').value = None
                    crude_options_chain_sheet.range(f'{ORDER_STATUS}{strikes+6}').value = None
                    crude_options_chain_sheet.range(f'{AVG_PRICE}{strikes+6}').value = None
                    crude_options_chain_sheet.range(f'{PROFIT_ORDER_NUMBER}{strikes+6}').value = None
                    crude_options_chain_sheet.range(f'{PROFIT_ORDER_STATUS}{strikes+6}').value = None
                    crude_options_chain_sheet.range(f'{PROFIT_ORDER_AVG_PRICE}{strikes+6}').value = None
                    crude_options_chain_sheet.range(f'{SL_ORDER_NUMBER}{strikes+6}').value = None
                    crude_options_chain_sheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value = None
                    crude_options_chain_sheet.range(f'{SL_ORDER_AVG_PRICE}{strikes+6}').value = None


                    
                    
                
                # Stage 2 : If the order is placed successfully, we have to start capturing the status of the order as the initial status and leave this condition.
                if (crude_options_chain_sheet.range(f'{ce_col_o}{strikes+6}').value and crude_options_chain_sheet.range(f'{ce_col_activated}{strikes+6}').value == True and CRUDE_BUY_ORDER_CE[strikes]
                    and not(crude_options_chain_sheet.range(f'{COL_ORDER_ID}{strikes+6}').value)):
                    
                    ord_id = CRUDE_BUY_ORDER_CE[strikes]['data']['orderId']
                    order_details = dhan.get_order_by_id(ord_id)['data'][0]
                    crude_options_chain_sheet.range(f'{COL_ORDER_ID}{strikes+6}').value  = order_details['orderId']
                    crude_options_chain_sheet.range(f'{ORDER_STATUS}{strikes+6}').value  = order_details['orderStatus']

                # Stage 3 : Following condition will update respective cells in the Excel until the Status is changed to "TRADED"
                if (crude_options_chain_sheet.range(f'{ce_col_o}{strikes+6}').value and crude_options_chain_sheet.range(f'{ce_col_activated}{strikes+6}').value == True and 
                    crude_options_chain_sheet.range(f'{COL_ORDER_ID}{strikes+6}').value and crude_options_chain_sheet.range(f'{ORDER_STATUS}{strikes+6}').value != "TRADED"):
                    ord_id = CRUDE_BUY_ORDER_CE[strikes]['data']['orderId']
                    order_details = dhan.get_order_by_id(ord_id)['data'][0]
                    crude_options_chain_sheet.range(f'{COL_ORDER_ID}{strikes+6}').value  = order_details['orderId']
                    crude_options_chain_sheet.range(f'{ORDER_STATUS}{strikes+6}').value  = order_details['orderStatus']    
                
                # Stage 4 : This is where all drama happens. Once the status is updated to "TRADED", we begin Exit Phase.
                if (crude_options_chain_sheet.range(f'{ce_col_o}{strikes+6}').value and crude_options_chain_sheet.range(f'{ce_col_activated}{strikes+6}').value == True and 
                    crude_options_chain_sheet.range(f'{COL_ORDER_ID}{strikes+6}') and crude_options_chain_sheet.range(f'{ORDER_STATUS}{strikes+6}').value == "TRADED"): 
                    Script_Key = crude_options_chain_sheet.range(f'{CE_SCRIPT_KEY}{strikes+6}').value
                    avg_executed_price =  order_details['averageTradedPrice']
                    crude_options_chain_sheet.range(f'{AVG_PRICE}{strikes+6}').value = avg_executed_price
                    ltp_ = crude_options_chain_sheet.range(f'{LTP_}{strikes+6}').value
                    trigger_profit_ = crude_options_chain_sheet.range(f'{TRIGGER_PROFIT_PRICE}{strikes+6}').value
                    trigger_sl_ = crude_options_chain_sheet.range(f'{TRIGGER_SL_PRICE}{strikes+6}').value
                    profit_price_ = crude_options_chain_sheet.range(f'{PROFT_TGT}{strikes+6}').value
                    sl_price_ = crude_options_chain_sheet.range(f'{SL_TGT}{strikes+6}').value
                    Quantity = crude_options_chain_sheet.range(f'{ce_col_o}{strikes+6}').value

                    
                    if ltp_>=trigger_profit_ and not(CRUDE_PROFIT_ORDER_CE[strikes]):
                         CRUDE_PROFIT_ORDER_CE[strikes] = place_profit_order(dhan,Script_Key,Quantity,profit_price_,dhanhq.MCX,"SPARK_CRUDE_CALL_BOOK_PROFIT")
 
                    if ltp_<=trigger_sl_ and not(CRUDE_LOSS_ORDER_CE[strikes]):
                         CRUDE_LOSS_ORDER_CE[strikes] = place_sl_order(dhan,Script_Key,Quantity,sl_price_,dhanhq.MCX,"SPARK_CRUDE_CALL_BOOK_LOSS")

                    if(CRUDE_PROFIT_ORDER_CE[strikes] and crude_options_chain_sheet.range(f'{PROFIT_ORDER_STATUS}{strikes+6}').value != 'TRADED' and CRUDE_PROFIT_ORDER_CE[strikes]['status']=='success'):
                        ord_id_p = CRUDE_PROFIT_ORDER_CE[strikes]['data']['orderId']
                        order_details_p = dhan.get_order_by_id(ord_id_p)['data'][0]
                        crude_options_chain_sheet.range(f'{PROFIT_ORDER_NUMBER}{strikes+6}').value = ord_id_p
                        crude_options_chain_sheet.range(f'{PROFIT_ORDER_STATUS}{strikes+6}').value = order_details_p['orderStatus']
                        if CRUDE_LOSS_ORDER_CE[strikes]:
                            ord_id_l = CRUDE_LOSS_ORDER_CE[strikes]['data']['orderId']
                            CANCEL_ORD = place_cancel_order(ord_id_l)
                            if (CANCEL_ORD['status']=='success'):
                                CRUDE_LOSS_ORDER_CE[strikes] = None
                                crude_options_chain_sheet.range(f'{SL_ORDER_NUMBER}{strikes+6}').value = None
                                crude_options_chain_sheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value = None

                    if(CRUDE_LOSS_ORDER_CE[strikes] and crude_options_chain_sheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value != 'TRADED' and CRUDE_LOSS_ORDER_CE[strikes]['status']=='success'):
                        ord_id_l = CRUDE_LOSS_ORDER_CE[strikes]['data']['orderId']
                        order_details_l = dhan.get_order_by_id(ord_id_l)['data'][0]
                        crude_options_chain_sheet.range(f'{SL_ORDER_NUMBER}{strikes+6}').value = ord_id_l
                        crude_options_chain_sheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value = order_details_l['orderStatus']
                        if CRUDE_PROFIT_ORDER_CE[strikes]:
                            ord_id_p = CRUDE_PROFIT_ORDER_CE[strikes]['data']['orderId']
                            CANCEL_ORD = place_cancel_order(ord_id_p)
                            if (CANCEL_ORD['status']=='success'):
                                CRUDE_PROFIT_ORDER_CE[strikes] = None
                                crude_options_chain_sheet.range(f'{PROFIT_ORDER_NUMBER}{strikes+6}').value = None
                                crude_options_chain_sheet.range(f'{PROFIT_ORDER_STATUS}{strikes+6}').value = None  

                    if(CRUDE_PROFIT_ORDER_CE[strikes] and crude_options_chain_sheet.range(f'{PROFIT_ORDER_STATUS}{strikes+6}').value == 'TRADED'):
                        crude_options_chain_sheet.range(f'{PROFIT_ORDER_AVG_PRICE}{strikes+6}').value = order_details_p['averageTradedPrice']
                        CRUDE_PROFIT_ORDER_CE[strikes] = None
                        crude_options_chain_sheet.range(f'{ce_input_prop}{strikes+6}').value = None
                        crude_options_chain_sheet.range(f'{ce_col_activated}{strikes+6}').value = False
                        if CRUDE_LOSS_ORDER_CE[strikes]:
                            ord_id_l = CRUDE_LOSS_ORDER_CE[strikes]['data']['orderId']
                            CANCEL_ORD = place_cancel_order(ord_id_l)
                            if (CANCEL_ORD['status']=='success'):
                                CRUDE_LOSS_ORDER_CE[strikes] = None
                                crude_options_chain_sheet.range(f'{SL_ORDER_NUMBER}{strikes+6}').value = None
                                crude_options_chain_sheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value = None
                    
                    if(CRUDE_LOSS_ORDER_CE[strikes] and crude_options_chain_sheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value == 'TRADED'):
                        crude_options_chain_sheet.range(f'{SL_ORDER_AVG_PRICE}{strikes+6}').value = order_details_p['averageTradedPrice']
                        CRUDE_LOSS_ORDER_CE[strikes] = None
                        crude_options_chain_sheet.range(f'{ce_input_prop}{strikes+6}').value = None
                        crude_options_chain_sheet.range(f'{ce_col_activated}{strikes+6}').value = False                        
                        if CRUDE_PROFIT_ORDER_CE[strikes]:
                            ord_id_l = CRUDE_PROFIT_ORDER_CE[strikes]['data']['orderId']
                            CANCEL_ORD = place_cancel_order(ord_id_l)
                            if (CANCEL_ORD['status']=='success'):
                                CRUDE_PROFIT_ORDER_CE[strikes] = None
                                crude_options_chain_sheet.range(f'{PROFIT_ORDER_NUMBER}{strikes+6}').value = None
                                crude_options_chain_sheet.range(f'{PROFIT_ORDER_STATUS}{strikes+6}').value = None   

                elif (not(crude_options_chain_sheet.range(f'{ce_col_o}{strikes+6}').value)):
                    crude_options_chain_sheet.range(f'{ce_col_activated}{strikes+6}').value = False
                
                
                # Crude PE
                if (crude_options_chain_sheet.range(f'{pe_col_o}{strikes+6}').value and crude_options_chain_sheet.range(f'{pe_col_activated}{strikes+6}').value == False):
                    # print(crude_options_chain_sheet.range(f'{pe_col_o}{strikes+6}').value)
                    crude_options_chain_sheet.range(f'{pe_col_activated}{strikes+6}').value = True
                elif (not(crude_options_chain_sheet.range(f'{pe_col_o}{strikes+6}').value)):
                    crude_options_chain_sheet.range(f'{pe_col_activated}{strikes+6}').value = False




        if NIFTY_TRADE :
            for strikes in range(11):
                # NIFTY CE
                if (nifty_options_chain_sheet.range(f'{ce_col_o}{strikes+6}').value and nifty_options_chain_sheet.range(f'{ce_col_activated}{strikes+6}').value == False):
                    print(nifty_options_chain_sheet.range(f'{ce_col_o}{strikes+6}').value)
                    Quantity = nifty_options_chain_sheet.range(f'{ce_col_o}{strikes+6}').value
                    Script_Key = nifty_options_chain_sheet.range(f'{CE_SCRIPT_KEY}{strikes+6}').value
                    LTP = nifty_options_chain_sheet.range(f'{CE_LTP}{strikes+6}').value
                    buy_order  = place_buy_order(dhan,Script_Key,Quantity,LTP-0.1,"SPARK_NIFTY_CALL_BUY")
                    nifty_options_chain_sheet.range(f'{ce_col_activated}{strikes+6}').value = True

                elif (not(nifty_options_chain_sheet.range(f'{ce_col_o}{strikes+6}').value)):
                    nifty_options_chain_sheet.range(f'{ce_col_activated}{strikes+6}').value = False
                
                
                # NIFTY PE
                if (nifty_options_chain_sheet.range(f'{pe_col_o}{strikes+6}').value and nifty_options_chain_sheet.range(f'{pe_col_activated}{strikes+6}').value == False):
                    # print(nifty_options_chain_sheet.range(f'{pe_col_o}{strikes+6}').value)
                    Quantity = nifty_options_chain_sheet.range(f'{pe_col_o}{strikes+6}').value
                    Script_Key = nifty_options_chain_sheet.range(f'{PE_SCRIPT_KEY}{strikes+6}').value
                    LTP = nifty_options_chain_sheet.range(f'{PE_LTP}{strikes+6}').value
                    buy_order  = place_buy_order(dhan,Script_Key,Quantity,LTP-0.1,"SPARK_NIFTY_PUT_BUY")                    
                    nifty_options_chain_sheet.range(f'{pe_col_activated}{strikes+6}').value = True
                elif (nifty_options_chain_sheet.range(f'{pe_col_o}{strikes+6}').value and nifty_options_chain_sheet.range(f'{pe_col_activated}{strikes+6}').value == True and buy_order['status']=='success'):
                    nifty_options_chain_sheet.range(f'{COL_ORDER_ID}{strikes+6}').value = buy_order['data'][0]['orderId']
                    order_details = dhan.get_order_by_id(buy_order['data'][0]['orderId'])['data'][0]
                    ##### All order details 
                    nifty_options_chain_sheet.range(f'{ORDER_STATUS}{strikes+6}').value = order_details['orderStatus']
                    nifty_options_chain_sheet.range(f'{AVG_PRICE}{strikes+6}').value = order_details['averageTradedPrice']

                    print(buy_order)

                elif (not(nifty_options_chain_sheet.range(f'{pe_col_o}{strikes+6}').value)):
                    nifty_options_chain_sheet.range(f'{pe_col_activated}{strikes+6}').value = False   
        if BANKNIFTY_TRADE :
            for strikes in range(11):
                # NIFTY CE
                if (BANKNIFTY_AVAILABLE_CAPITAL.range(f'{ce_col_o}{strikes+6}').value and BANKNIFTY_AVAILABLE_CAPITAL.range(f'{ce_col_activated}{strikes+6}').value == False):
                    # print(BANKNIFTY_AVAILABLE_CAPITAL.range(f'{ce_col_o}{strikes+6}').value)
                    BANKNIFTY_AVAILABLE_CAPITAL.range(f'{ce_col_activated}{strikes+6}').value = True
                elif (not(BANKNIFTY_AVAILABLE_CAPITAL.range(f'{ce_col_o}{strikes+6}').value)):
                    BANKNIFTY_AVAILABLE_CAPITAL.range(f'{ce_col_activated}{strikes+6}').value = False
                # NIFTY PE
                if (BANKNIFTY_AVAILABLE_CAPITAL.range(f'{pe_col_o}{strikes+6}').value and BANKNIFTY_AVAILABLE_CAPITAL.range(f'{pe_col_activated}{strikes+6}').value == False):
                    # print(BANKNIFTY_AVAILABLE_CAPITAL.range(f'{pe_col_o}{strikes+6}').value)
                    BANKNIFTY_AVAILABLE_CAPITAL.range(f'{pe_col_activated}{strikes+6}').value = True
                elif (not(BANKNIFTY_AVAILABLE_CAPITAL.range(f'{pe_col_o}{strikes+6}').value)):
                    BANKNIFTY_AVAILABLE_CAPITAL.range(f'{pe_col_activated}{strikes+6}').value = False                                                   
            

def main():
    connections__= connect_to_dhan() #returned as dictionary but accessed like a list
    dhan = connections__['connection']
    # print(instruments)
    get_order_details(dhan)


if __name__ == "__main__":
    main()
