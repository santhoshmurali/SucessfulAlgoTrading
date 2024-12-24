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
excel_file = 'DhanTrading.xlsx'
workbook = xw.Book(excel_file)
TradeSheet = workbook.sheets['Trade']





INPUT_PROP = "I"
QUANTITY = "J" #Order Quantity for CE
ORD_ACTIVATED = "K" #If Order is activated for CE
LTP_ = "H"
SCRIPT_KEY = "G"
BUY_ORDER_ID = "M"
BUY_ORDER_STATUS = "N"
BUY_PRICE = "O"
PROFIT_ORDER_ID = "P"
PROFIT_ORDER_STATUS = "Q"
PROFIT_PRICE = "R"
PROFIT_TGT = "S"
SL_ORDER_ID = "T"
SL_ORDER_STATUS = "U"
SL_PRICE = "V"
SL_TGT = "W"
PROFIT_TRIGGER_PRICE = "X"
SL_TRIGGER_PRICE = "Y"




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
def place_buy_order(dhan,script_key, order_qty, trade_price,correlation_id):
    if TradeSheet.range("INSTRUMENT").value == "CRUDEOIL":
        segment = dhanhq.MCX
    else:
        segment = dhanhq.NSE_FNO
    SLICE_ORDER_BUY = dhan.place_order(security_id=str(int(script_key)), exchange_segment=segment, transaction_type=dhanhq.BUY, quantity=str(int(order_qty)),
                           order_type=dhanhq.LIMIT, product_type=dhanhq.INTRA, price=trade_price, trigger_price= trade_price-0.1, disclosed_quantity=0,
                           after_market_order=False, validity='DAY', amo_time='OPEN',
                           bo_profit_value=None, bo_stop_loss_Value=None, tag="ALGOENTRY") 
    print(SLICE_ORDER_BUY)
    return(SLICE_ORDER_BUY)


#Place Profit Order
def place_profit_order(dhan,script_key, order_qty, trade_price,correlation_id):
    if TradeSheet.range("INSTRUMENT").value == "CRUDEOIL":
        segment = dhanhq.MCX
    else:
        segment = dhanhq.NSE_FNO    
    SLICE_ORDER_SELL_PROFIT = dhan.place_order(security_id=str(int(script_key)), exchange_segment=segment, transaction_type=dhanhq.SELL, quantity=int(order_qty),
                           order_type=dhanhq.LIMIT, product_type=dhanhq.INTRA, price=np.round(float(trade_price),1),disclosed_quantity=0, validity='DAY',amo_time='OPEN',
                            tag="ALGOPFT")
    print(SLICE_ORDER_SELL_PROFIT)
    return(SLICE_ORDER_SELL_PROFIT)

#Place SL Order
def place_sl_order(dhan,script_key, order_qty, trade_price,correlation_id):
    if TradeSheet.range("INSTRUMENT").value == "CRUDEOIL":
        segment = dhanhq.MCX
    else:
        segment = dhanhq.NSE_FNO
    SLICE_ORDER_SELL_LOSS = dhan.place_order(security_id=str(int(script_key)), exchange_segment=segment, transaction_type=dhanhq.SELL, quantity=int(order_qty),
                           order_type=dhanhq.SL, product_type=dhanhq.INTRA, price=np.round(float(trade_price),1), trigger_price=np.round(float(trade_price),1)+0.2, disclosed_quantity=0, 
                           validity='DAY', amo_time='OPEN',tag="ALGOLOSS")
    print(SLICE_ORDER_SELL_LOSS)
    return(SLICE_ORDER_SELL_LOSS)

def place_cancel_order(dhan,orderid):
    CANCEL_ORDER = dhan.cancel_order(orderid)
    return(CANCEL_ORDER)


#Order Management
def get_order_details(dhan):
    # Buy Order Object
    BUY_ORDER = [None for k in range(4)]

    # Sell Profit Object
    PROFIT_ORDER = [None for k in range(4)]

    # Sell Loss Object
    LOSS_ORDER = [None for k in range(4)]

    TRADE_COMPLETE = False  
    SWING_SWITCH = False

    TRADED_QUANTITY = None
    

    while True:
        for strikes in range(4):

            # # NIFTY CE 
            # Stage 1 : Check if the User initatd any orders, if yes. Place the Buy order and do not execute this if again. This is controlled using Activated flag in Excel mapped using  ce_col_activated
            if (TradeSheet.range(f'{QUANTITY}{strikes+6}').value and TradeSheet.range(f'{ORD_ACTIVATED}{strikes+6}').value == False and TRADE_COMPLETE == False): 
                Quantity = TradeSheet.range(f'{QUANTITY}{strikes+6}').value
                TRADED_QUANTITY = Quantity
                Script_Key = TradeSheet.range(f'{SCRIPT_KEY}{strikes+6}').value
                ltp__ = TradeSheet.range(f'{LTP_}{strikes+6}').value
                buy_order  = place_buy_order(dhan,Script_Key,Quantity,ltp__-0.1,"ALGO_ORDER")
                print(buy_order)
                BUY_ORDER[strikes] = buy_order
                TradeSheet.range(f'{ORD_ACTIVATED}{strikes+6}').value = True
                TradeSheet.range(f'{BUY_ORDER_ID}{strikes+6}').value = None
                TradeSheet.range(f'{BUY_ORDER_STATUS}{strikes+6}').value = None
                TradeSheet.range(f'{BUY_PRICE}{strikes+6}').value = None
                TradeSheet.range(f'{PROFIT_ORDER_ID}{strikes+6}').value = None
                TradeSheet.range(f'{PROFIT_ORDER_STATUS}{strikes+6}').value = None
                TradeSheet.range(f'{PROFIT_PRICE}{strikes+6}').value = None
                TradeSheet.range(f'{SL_ORDER_ID}{strikes+6}').value = None
                TradeSheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value = None
                TradeSheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value = None
                TradeSheet.range(f'{SL_PRICE}{strikes+6}').value = None
                
            
            # Stage 2 : If the order is placed successfully, we have to start capturing the status of the order as the initial status and leave this condition.
            if (TradeSheet.range(f'{QUANTITY}{strikes+6}').value and TradeSheet.range(f'{ORD_ACTIVATED}{strikes+6}').value == True and BUY_ORDER[strikes]
                and not(TradeSheet.range(f'{BUY_ORDER_ID}{strikes+6}').value)):
                
                ord_id = BUY_ORDER[strikes]['data']['orderId']
                order_details = dhan.get_order_by_id(ord_id)['data'][0]
                TradeSheet.range(f'{BUY_ORDER_ID}{strikes+6}').value  = order_details['orderId']
                TradeSheet.range(f'{BUY_ORDER_STATUS}{strikes+6}').value  = order_details['orderStatus']

            # Stage 3 : Following condition will update respective cells in the Excel until the Status is changed to "TRADED"
            if (TradeSheet.range(f'{QUANTITY}{strikes+6}').value and TradeSheet.range(f'{ORD_ACTIVATED}{strikes+6}').value == True and 
                TradeSheet.range(f'{BUY_ORDER_ID}{strikes+6}').value and TradeSheet.range(f'{BUY_ORDER_STATUS}{strikes+6}').value != "TRADED"):
                ord_id = BUY_ORDER[strikes]['data']['orderId']
                order_details = dhan.get_order_by_id(ord_id)['data'][0]
                TradeSheet.range(f'{BUY_ORDER_ID}{strikes+6}').value  = order_details['orderId']
                TradeSheet.range(f'{BUY_ORDER_STATUS}{strikes+6}').value  = order_details['orderStatus']    
            
            # Stage 4 : This is where all drama happens. Once the status is updated to "TRADED", we begin Exit Phase.
            if (TradeSheet.range(f'{QUANTITY}{strikes+6}').value and TradeSheet.range(f'{ORD_ACTIVATED}{strikes+6}').value == True and 
                TradeSheet.range(f'{BUY_ORDER_ID}{strikes+6}') and TradeSheet.range(f'{BUY_ORDER_STATUS}{strikes+6}').value == "TRADED"): 
                Script_Key = TradeSheet.range(f'{SCRIPT_KEY}{strikes+6}').value
                avg_executed_price =  order_details['price']
                TradeSheet.range(f'{BUY_PRICE}{strikes+6}').value = avg_executed_price
                ltp_ = TradeSheet.range(f'{LTP_}{strikes+6}').value
                trigger_profit_ = TradeSheet.range(f'{PROFIT_TRIGGER_PRICE}{strikes+6}').value
                trigger_sl_ = TradeSheet.range(f'{SL_TRIGGER_PRICE}{strikes+6}').value
                profit_price_ = TradeSheet.range(f'{PROFIT_TGT}{strikes+6}').value
                sl_price_ = TradeSheet.range(f'{SL_TGT}{strikes+6}').value
                Quantity = TradeSheet.range(f'{QUANTITY}{strikes+6}').value

      
                # Logic for handling the ordes when price is swining between Tgt pl and tgt sl
                # If LTP moves above trigger profit we turn the switch On and place the profit order                    
                if ltp_>=trigger_profit_ and not(PROFIT_ORDER[strikes]) and not(SWING_SWITCH):
                        PROFIT_ORDER[strikes] = place_profit_order(dhan,Script_Key,TRADED_QUANTITY,profit_price_,"")
                        if (LOSS_ORDER[strikes]):
                            CANCEL_ORD = place_cancel_order(dhan,str(LOSS_ORDER[strikes]['data']['orderId']))
                            LOSS_ORDER[strikes] = None
                            TradeSheet.range(f'{SL_ORDER_ID}{strikes+6}').value = CANCEL_ORD['data']['orderId']
                            TradeSheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value = CANCEL_ORD['status']
                        SWING_SWITCH = True
                # We Monitor the order till it is not TRADED and SWITCH is ON
                if (PROFIT_ORDER[strikes]  and PROFIT_ORDER[strikes]['status']=='success' and SWING_SWITCH):
                    ord_id_p = PROFIT_ORDER[strikes]['data']['orderId']
                    order_details_p = dhan.get_order_by_id(ord_id_p)['data'][0]
                    TradeSheet.range(f'{SL_ORDER_ID}{strikes+6}').value = None
                    TradeSheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value = None                   
                    TradeSheet.range(f'{PROFIT_ORDER_ID}{strikes+6}').value = ord_id_p
                    TradeSheet.range(f'{PROFIT_ORDER_STATUS}{strikes+6}').value = order_details_p['orderStatus']
                    print(f'Profit Order Status {order_details_p['orderStatus']}')
                  # Profit Order is succes    
                if(PROFIT_ORDER[strikes] and TradeSheet.range(f'{PROFIT_ORDER_STATUS}{strikes+6}').value == 'TRADED' and SWING_SWITCH):
                    SWING_SWITCH = False
                    PROFIT_ORDER[strikes] = None
                    TRADE_COMPLETE = True

                # If LTP moves below trigger SL we turn the switch On and place the loss order
                if ltp_<=trigger_sl_ and not(LOSS_ORDER[strikes]) and not(SWING_SWITCH):
                        LOSS_ORDER[strikes] = place_sl_order(dhan,Script_Key,TRADED_QUANTITY,sl_price_,"")
                        if (PROFIT_ORDER[strikes]):
                            CANCEL_ORD = place_cancel_order(dhan,str(PROFIT_ORDER[strikes]['data']['orderId']))
                            PROFIT_ORDER[strikes] = None
                            TradeSheet.range(f'{PROFIT_ORDER_ID}{strikes+6}').value =  CANCEL_ORD['data']['orderId']
                            TradeSheet.range(f'{PROFIT_ORDER_STATUS}{strikes+6}').value = CANCEL_ORD['status']                        
                        SWING_SWITCH = True           
                # We Monitor the order till it is not TRADED and SWITCH is ON
                if(LOSS_ORDER[strikes] and LOSS_ORDER[strikes]['status']=='success' and SWING_SWITCH):
                    ord_id_l = LOSS_ORDER[strikes]['data']['orderId']
                    order_details_l = dhan.get_order_by_id(ord_id_l)['data'][0]
                    TradeSheet.range(f'{PROFIT_ORDER_ID}{strikes+6}').value = None
                    TradeSheet.range(f'{PROFIT_ORDER_STATUS}{strikes+6}').value = None                  
                    TradeSheet.range(f'{SL_ORDER_ID}{strikes+6}').value = ord_id_l
                    TradeSheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value = order_details_l['orderStatus']
                    print(f'SL Order Status {order_details_l['orderStatus']}')
                # Loss Order is succes    
                if(LOSS_ORDER[strikes] and TradeSheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value == 'TRADED' and SWING_SWITCH ):
                    SWING_SWITCH = False
                    LOSS_ORDER[strikes] = None
                    TRADE_COMPLETE = True                                          
                
                
                if TRADE_COMPLETE:
                    TradeSheet.range(f'{INPUT_PROP}{strikes+6}').value = None
                    TradeSheet.range(f'{ORD_ACTIVATED}{strikes+6}').value = False                    
                    TradeSheet.range(f'{BUY_ORDER_ID}{strikes+6}').value = None
                    TradeSheet.range(f'{BUY_ORDER_STATUS}{strikes+6}').value = None
                    TradeSheet.range(f'{BUY_PRICE}{strikes+6}').value = None
                    TradeSheet.range(f'{PROFIT_ORDER_ID}{strikes+6}').value = None
                    TradeSheet.range(f'{PROFIT_ORDER_STATUS}{strikes+6}').value = None
                    TradeSheet.range(f'{PROFIT_PRICE}{strikes+6}').value = None
                    TradeSheet.range(f'{SL_ORDER_ID}{strikes+6}').value = None
                    TradeSheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value = None
                    TradeSheet.range(f'{SL_ORDER_STATUS}{strikes+6}').value = None
                    TradeSheet.range(f'{SL_PRICE}{strikes+6}').value = None 
                    TRADED_QUANTITY = None
                    TRADE_COMPLETE = False

            # elif (not(TradeSheet.range(f'{QUANTITY}{strikes+6}').value)):
                # TradeSheet.range(f'{ce_col_activated}{strikes+6}').value = False
            

            

def main():
    connections__= connect_to_dhan() #returned as dictionary but accessed like a list
    dhan = connections__['connection']
    # print(instruments)
    print("Order MGMT started!")
    get_order_details(dhan)


if __name__ == "__main__":
    main()
