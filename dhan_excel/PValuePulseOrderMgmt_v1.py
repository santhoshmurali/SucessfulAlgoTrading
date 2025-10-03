import os, sys
from os import access
from tkinter import N
from sqlalchemy import Column, null
import xlwings as xw
from dhanhq import dhanhq, DhanContext
import numpy as np
import pandas as pd
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


def connect_to_dhan(sandbox=False):
    """
    Updated to use DhanContext properly
    DhanContext now handles all credential management
    """
    if sandbox:
        client_id =  os.getenv('SB_DHAN_CLIENT_ID')
        access_token = os.getenv('SB_DHAN_ACCESS_TOKEN')
        dhan_context = DhanContext(client_id, access_token, use_sandbox=True)
    else:
        client_id =  os.getenv('DHAN_CLIENT_ID')
        access_token = os.getenv('DHAN_ACCESS_TOKEN')
        dhan_context = DhanContext(client_id, access_token, use_sandbox=False)
    
    # Establish connection to Dhan
    try:
        dhan = dhanhq(dhan_context)
    except Exception as e :
        raise ConnectionError(f"Can't connect {e}")  

    return({"connection":dhan,
            "client_id":client_id,
            "access_token":access_token,
            "dhan_context": dhan_context  }) #  Include context for future use}  

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
                                        tag= correlation_id+"_S"                             #  Optional correlation ID for tracking.
                                        )     
    if NON_SLICE_ORD_QTY > 0:
        print(f"Non slice order QTY = {NON_SLICE_ORD_QTY}")
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
                                        tag=correlation_id+"_N"                       #  Optional correlation ID for tracking.
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
                                    tag=correlation_id                                   # Optional correlation ID for tracking.
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
                                tag=correlation_id                                      # Optional correlation ID for tracking.
                                )

    print(SLICE_ORDER_SELL_LOSS)
    return(SLICE_ORDER_SELL_LOSS)

def place_cancel_order(dhan,orderid):
    CANCEL_ORDER = dhan.cancel_order(orderid)
    return(CANCEL_ORDER)


def extract_order_ids(orders: dict) -> list:
    order_ids = []
    for section in orders.values():
        data = section.get('data')
        if not data:
            continue
        if isinstance(data, dict):
            # Single order dict
            oid = data.get('orderId')
            if oid is not None:
                order_ids.append(oid)
        elif isinstance(data, list):
            # List of order dicts
            for item in data:
                if isinstance(item, dict):
                    oid = item.get('orderId')
                    if oid is not None:
                        order_ids.append(oid)
    return order_ids



#Order Management
def get_order_details(dhan):
    # Global Variables
    OrderMgmt.range('INITIATE').value = "STOP"
    OrderMgmt.range('TRADE_STATUS').value = "OPEN"
    OrderMgmt.range('LMT_PRICE').value = None
    BUY_ORDER_STATUS = None
    BUY_ORDER_PRICE = None
    # CREATE A LIST TO STORE ALL BUY ORDERS THAT ARE PLACED
    BUY_ORDER_LIST = []
    # CREATE A LIST TO STORE ALL SELL ORDERS THAT ARE PLACED
    SELL_ORDER_LIST = []
    while True:
        # Stage 1 : Check if the initiate flag is set to start and LMT price is set in the excel sheet and if prce is above LMT price, initiate a buy order
        if (OrderMgmt.range('INITIATE').value == "TRADE" and OrderMgmt.range('LMT_PRICE').value > 0 and OrderMgmt.range('LMT_PRICE').value != None) :
            Script_Key = OrderMgmt.range('SYMBOL_KEY').value
            Quantity = {
                'SLICE_ORDER_QTY': OrderMgmt.range('SLICE_ORDER_QTY').value,
                'NON_SLICE_ORDER_QTY': OrderMgmt.range('NON_SLICE_ORDER_QTY').value,
            }
            correlation_id = "ALGO_ORDER_BUY"
            if OrderMgmt.range('LTP').value > OrderMgmt.range('LMT_PRICE').value and BUY_ORDER_STATUS == None:
                # ONCE PRCE MOVES ABLVE LMT PRICE CHANGE STATUS TO CREATED AND UPDATE ORDER PRICE AS LIMIT PRICE
                BUY_ORDER_STATUS = "CREATED" 
                BUY_ORDER_PRICE =OrderMgmt.range('LMT_PRICE').value
                buy_order = place_buy_order(dhan,Script_Key,Quantity,BUY_ORDER_PRICE,correlation_id)

        
        # Stage 2 : If the order is placed successfully, we have to start capturing the status of the order as the initial status and leave this condition.
        if (BUY_ORDER_STATUS == "CREATED" and  buy_order):
            BUY_ORDER_STATUS = "PLACED"
            #buy_order = {'NON_SLICE_ORDER': {'status': 'success', 'remarks': '', 'data': {'orderId': '322250930708302', 'orderStatus': 'TRANSIT'}}, 'SLICE_ORDER': {'status': 'success', 'remarks': '', 'data': [{'orderId': '102250930508102', 'orderStatus': 'TRANSIT'}, {'orderId': '42250930554402', 'orderStatus': 'TRANSIT'}, {'orderId': '332250930573002', 'orderStatus': 'TRANSIT'}]}}
            # Lets store the all the order id from buy_order and corresponsinf status to a list. for those orders whose status is success
            buy_orders = extract_order_ids(buy_order)
            print(buy_orders)
            OrderMgmt.range("D8").options(index=False).value = pd.DataFrame(buy_orders,columns=['BUY_ORDER_ID'])

            
            







            

def main():
    """
    if sys.argv[0]==dev then connect to sanbox api else connect to production api
    """

    # Validate command line arguments
    if len(sys.argv) < 2:
        print("❌ Usage: python PValuePulseOrderMgmt_v[x] [dev|prod]")
        sys.exit(1)
    
    environment = sys.argv[1].lower()
    if environment == 'dev':
        print("🔧 Connecting to SANDBOX environment...")
        connections = connect_to_dhan(sandbox=True)  # ✅ Fixed: was 'sanbox'
    elif environment == 'prod':
        print("🚀 Connecting to PRODUCTION environment...")
        connections = connect_to_dhan(sandbox=False)
    else:
        print(f"❌ Invalid environment: {environment}. Use 'dev' or 'prod'")
        sys.exit(1)
         
    dhan = connections['connection']

    print(f"✅ Connected to {environment.upper()} environment")
    print(f"📊 Order Management started!")
        
    # print(instruments)
    print(f"{sys.argv[1]} - Order MGMT started!")

    try:
        get_order_details(dhan)
    except KeyboardInterrupt:
        print("\n⚠️ Order management stopped by user")
    except Exception as e:
        print(f"❌ Fatal error in order management: {e}")
        raise

if __name__ == "__main__":
    main()
