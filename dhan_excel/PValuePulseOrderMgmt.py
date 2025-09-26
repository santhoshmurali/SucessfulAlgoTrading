import os
from os import access
from sqlalchemy import null
import xlwings as xw
from connect_to_dhan import Connection
from dhanhq import dhanhq
import numpy as np
import pandas as pd
import yaml
import time
from dotenv import load_dotenv
load_dotenv()

""" configuring the workbook and getting the security info
"""
# Load Excel file
excel_file = os.getenv('TradingSystemnNew','PValue_Pulse.xlsx')
workbook = xw.Book(excel_file)
TradeSheet = workbook.sheets['Trade']
OrderMgmt = workbook.sheets['TRADEMGMT']
OrdersSheet = workbook.sheets['Orders']





def connect_to_dhan():
    client_id =  os.getenv('DHAN_CLIENT_ID')
    access_token = os.getenv('DHAN_ACCESS_TOKEN')

    # Establish connection to Dhan
    try:
        DhanConnector = Connection(client_id, access_token)
        ConnectionObject = DhanConnector.connect_dhan()
        dhan = ConnectionObject['conn']
    except Exception as e :
        raise ConnectionError(f"Can't connect {e}")  

    return({"connection":dhan,
            "client_id":client_id,
            "access_token":access_token})    

#Order Placement Functions
def place_buy_order(dhan,script_key, order_qty, trade_price,correlation_id):
    if TradeSheet.range("INSTRUMENT").value == "CRUDEOIL" or TradeSheet.range("INSTRUMENT").value == "CRUDEOILM":
        segment = dhanhq.MCX
    else:
        segment = dhanhq.NSE_FNO
    #* Place a slice order buy with trigger price
    SLICE_ORD_QTY = order_qty['SLICE_ORDER_QTY']
    NON_SLICE_ORD_QTY = order_qty['NON_SLICE_ORDER_QTY']
    if SLICE_ORD_QTY > 0:
        SLICE_ORDER_BUY = dhan.place_slice_order(security_id=str(int(script_key)),      # The ID of the security to trade.
                                        exchange_segment=segment,                    # The exchange segment (e.g., NSE, BSE).
                                        transaction_type=dhanhq.BUY,                 # The type of transaction (BUY/SELL).
                                        quantity=str(int(SLICE_ORD_QTY)),                # The quantity of the order.
                                        order_type=dhanhq.LIMIT,                     # The type of order (LIMIT/MARKET/SL).
                                        product_type=dhanhq.INTRA,                   # The product type (CNC, MIS, etc.).
                                        price=trade_price,                           # The price of the order.
                                        trigger_price= trade_price-0.1,              # The trigger price for the order.
                                        disclosed_quantity=0,                        # The disclosed quantity for the order.
                                        after_market_order=False,                    # Flag for after market order.
                                        validity='DAY',                              # The validity of the order (DAY, IOC, etc.).
                                        amo_time='OPEN',                             # The time for AMO orders.
                                        bo_profit_value=None,                        # The profit value for BO orders.
                                        bo_stop_loss_Value=None,                     # The stop loss value for BO orders.
                                        tag="ALGOENTRY"                              #  Optional correlation ID for tracking.
                                        )     
    if NON_SLICE_ORD_QTY > 0:
        NON_SLICE_ORDER_BUY = dhan.place_order(security_id=str(int(script_key)),            # The ID of the security to trade.
                                        exchange_segment=segment,                    # The exchange segment (e.g., NSE, BSE).
                                        transaction_type=dhanhq.BUY,                 # The type of transaction (BUY/SELL).
                                        quantity=str(int(NON_SLICE_ORD_QTY)),                # The quantity of the order.
                                        order_type=dhanhq.LIMIT,                     # The type of order (LIMIT/MARKET/SL).
                                        product_type=dhanhq.INTRA,                   # The product type (CNC, MIS, etc.).
                                        price=trade_price,                           # The price of the order.
                                        disclosed_quantity=0,                        # The disclosed quantity for the order.
                                        after_market_order=False,                    # Flag for after market order.
                                        validity='DAY',                              # The validity of the order (DAY, IOC, etc.).
                                        amo_time='OPEN',                             # The time for AMO orders.
                                        bo_profit_value=None,                        # The profit value for BO orders.
                                        bo_stop_loss_Value=None,                     # The stop loss value for BO orders.
                                        tag="ALGOENTRY"                              #  Optional correlation ID for tracking.
                                        )
 
    return({
        "NON_SLICE_ORDER": NON_SLICE_ORDER_BUY if NON_SLICE_ORD_QTY > 0 else None,
        "SLICE_ORDER": SLICE_ORDER_BUY if SLICE_ORD_QTY > 0 else None,
    })


#Place Profit Order
def place_profit_order(dhan,script_key, order_qty, trade_price,correlation_id):
    if TradeSheet.range("INSTRUMENT").value == "CRUDEOIL"  or TradeSheet.range("INSTRUMENT").value == "CRUDEOILM":
        segment = dhanhq.MCX
    else:
        segment = dhanhq.NSE_FNO    
    #* Place a slice order buy with trigger price
    SLICE_ORDER_SELL_PROFIT = dhan.place_slice_order(
                                    security_id=str(int(script_key)),               # The ID of the security to trade.
                                    exchange_segment=segment,                       # The exchange segment (e.g., NSE, BSE).
                                    transaction_type=dhanhq.SELL,                   # The type of transaction (BUY/SELL).
                                    quantity=int(order_qty),                        # The quantity of the order.
                                    order_type=dhanhq.LIMIT,                        # The type of order (LIMIT/MARKET/SL).
                                    product_type=dhanhq.INTRA,                      # The product type (CNC, MIS, etc.).
                                    price=float(trade_price),                       # The price of the order.
                                    disclosed_quantity=0,                           # The disclosed quantity for the order. 
                                    validity='DAY',                                 # The validity of the order (DAY, IOC, etc.).
                                    amo_time='OPEN',                                # The time for AMO orders.
                                    tag="ALGOPFT"                                   # Optional correlation ID for tracking.
                                    )
    print(SLICE_ORDER_SELL_PROFIT)
    return(SLICE_ORDER_SELL_PROFIT)

#Place SL Order
def place_sl_order(dhan,script_key, order_qty, trade_price,correlation_id):
    if TradeSheet.range("INSTRUMENT").value == "CRUDEOIL"  or TradeSheet.range("INSTRUMENT").value == "CRUDEOILM":
        segment = dhanhq.MCX
    else:
        segment = dhanhq.NSE_FNO
    #* Place a slice order buy with trigger price
    SLICE_ORDER_SELL_LOSS = dhan.place_slice_order(
                                security_id=str(int(script_key)),                   # The ID of the security to trade. 
                                exchange_segment=segment,                           # The exchange segment (e.g., NSE, BSE).
                                transaction_type=dhanhq.SELL,                       # The type of transaction (BUY/SELL).
                                quantity=int(order_qty),                            # The quantity of the order.
                                order_type=dhanhq.SL,                               # The type of order (LIMIT/MARKET/SL).
                                product_type=dhanhq.INTRA,                          # The product type (CNC, MIS, etc.).
                                price=float(trade_price),                           # The price of the order.
                                trigger_price=float(trade_price)+0.2,               # The trigger price for the order.
                                disclosed_quantity=0,                               # The disclosed quantity for the order.
                                validity='DAY',                                     # The validity of the order (DAY, IOC, etc.).
                                amo_time='OPEN',                                    # The time for AMO orders.
                                tag="ALGOLOSS"                                      # Optional correlation ID for tracking.
                                )

    print(SLICE_ORDER_SELL_LOSS)
    return(SLICE_ORDER_SELL_LOSS)

def place_cancel_order(dhan,orderid):
    CANCEL_ORDER = dhan.cancel_order(orderid)
    return(CANCEL_ORDER)

def rest_flags_values(strikes, entry_type):
    pass                      



#Order Management
def get_order_details(dhan):
    # Global Variables
    OrderMgmt.range('INITIATE').value = "STOP"
    OrderMgmt.range('LMT_PRICE').value = None
    BUY_ORDER_STATUS = None
    BUY_ORDER_PRICE = None
    while True:
        # Stage 1 : Check if the initiate flag is set to start and LMT price is set in the excel sheet and if prce is above LMT price, initiate a buy order
        if (OrderMgmt.range('INITIATE').value == "START" and OrderMgmt.range('LMT_PRICE').value > 0 and OrderMgmt.range('LMT_PRICE').value != None) :
            Script_Key = OrderMgmt.range('SYMBOL_KEY').value
            Quantity = {
                'SLICE_ORDER_QTY': OrderMgmt.range('SLICE_ORDER_QTY').value,
                'NON_SLICE_ORDER_QTY': OrderMgmt.range('NON_SLICE_ORDER_QTY').value,
            }
            if OrderMgmt.range('LTP').value > OrderMgmt.range('LMT_PRICE').value and BUY_ORDER_STATUS == None:
                # ONCE PRCE MOVES ABLVE LMT PRICE CHANGE STATUS TO CREATED AND UPDATE ORDER PRICE AS LIMIT PRICE
                BUY_ORDER_STATUS = "CREATED" 
                BUY_ORDER_PRICE =OrderMgmt.range('LMT_PRICE').value
                buy_order = place_buy_order(dhan,Script_Key,Quantity,BUY_ORDER_PRICE,"ALGO_ORDER")

        
        # Stage 2 : If the order is placed successfully, we have to start capturing the status of the order as the initial status and leave this condition.
        if (BUY_ORDER_STATUS == "CREATED" and OrderMgmt.range('TOTAL_ORDERED_QTY').value == 0 and buy_order):
            BUY_ORDER_STATUS = "PLACED"
            print(buy_order)




            

def main():
    connections__= connect_to_dhan() #returned as dictionary but accessed like a list
    dhan = connections__['connection']
    # print(instruments)
    print("Order MGMT started!")
    get_order_details(dhan)


if __name__ == "__main__":
    main()
