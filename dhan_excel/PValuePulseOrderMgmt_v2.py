import os, sys
from os import access
from tkinter import N
import xlwings as xw
from dhanhq import dhanhq, DhanContext
import numpy as np
import pandas as pd
import datetime

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
FREEZE_QTY_MAP = os.getenv('freeze_qty_order')


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


def slice_the_order(total_qty: int) -> dict:
    """
    Slice order quantity based on freeze quantity limits.
    
    Args:
        total_qty: Total quantity to be sliced
        freeze_qty_map: Dictionary mapping index names to freeze quantities
        order_mgmt_sheet: Excel sheet object containing order management data
        
    Returns:
        dict with 'slice_qty' and 'non_slice_qty'
    """
    # Get index name (first 5 characters)
    index_name_full = OrderMgmt.range('INDEX_NAME').value
    INDEX_NAME = index_name_full[:5]  # Python slice instead of left()
    
    # Get freeze quantity for this index
    FREEZE_QTY = FREEZE_QTY_MAP[INDEX_NAME]
    
    # Calculate slices
    if total_qty <= FREEZE_QTY:  # Added missing colon
        slice_qty = 0
        non_slice_qty = total_qty
    else:
        total_slices = total_qty // FREEZE_QTY  # Floor division
        slice_qty = total_slices * FREEZE_QTY
        non_slice_qty = total_qty % FREEZE_QTY  # Modulo operator
    
    return {
        "slice_qty": slice_qty,
        "non_slice_qty": non_slice_qty
    }



# new function
def reset_sheet():
    OrderMgmt.range('INITIATE').value = "STOP"
    OrderMgmt.range('TRADE_STATUS').value = "START"
    OrderMgmt.range('LMT_PRICE').value = None
    OrderMgmt.range('AVG_BUY_PRICE').value = 0.0
    OrderMgmt.range('BUY_QTY').value = 0
    OrderMgmt.range('MESSAGE').value = ""

#-----------------------------------------------------------------
# Main Mahoc Function
#----------------------------------------------------------------
#python code
def process_order_details(order_details,qty:bool=False):
    """
    Process order details and return pending orders, total quantities, and average price.
    
    Args:
        order_details (list): List of order dictionaries
        
    Returns:
        dict: {
            'pending_order_ids': list of order IDs with PENDING status,
            'total_quantity': sum of filled quantities (positive for BUY, negative for SELL),
            'average_price': weighted average price of TRADED orders
        }
    """
    pending_order_ids = []
    total_filled_qty = 0
    sumproduct = 0  # Sum of (filledQty * averageTradedPrice)
    total_traded_qty = 0  # Sum of filledQty for TRADED orders
    transaction_type = None
    
    for order in order_details:
        order_status = order.get('orderStatus')
        order_id = order.get('orderId')
        filled_qty = order.get('filledQty', 0)
        avg_price = order.get('averageTradedPrice', 0)
        trans_type = order.get('transactionType')
        
        # Set transaction type from first order (assuming all have same type)
        if transaction_type is None:
            transaction_type = trans_type
        
        # Collect pending order IDs
        if order_status == 'PENDING' or order_status == 'TRANSIT':
            pending_order_ids.append(order_id)

        
        # Process TRADED orders
        if order_status == 'TRADED':
            # Calculate sumproduct for average price
            sumproduct += filled_qty * avg_price
            total_traded_qty += filled_qty
    

    # Calculate average price
    if total_traded_qty > 0:
        average_price = sumproduct / total_traded_qty
    else:
        average_price = 0.0
    
    if qty:
        return total_filled_qty
    else:
        return {
            'pending_order_ids': pending_order_ids,
            'average_price': average_price
        }



def get_order_status(dhan, correlation_id: str):
    order_details = dhan.get_order_by_correlationID(correlation_id)['data']
    processed = process_order_details(order_details)
    PENDING_ORDERS = processed['pending_order_ids']
    AVG_PRICE = processed['average_price']
    return PENDING_ORDERS,  AVG_PRICE

def get_updated_open_trades(dhan,correlation_id_buy, correlation_id_sell_profit, correlation_id_sell_loss, correlation_id_sell_be):
    if correlation_id_buy != "":
        order_details_buy =  dhan.get_order_by_correlationID(correlation_id_buy)['data']
        processed_order_buy = process_order_details(order_details_buy,True)
    else:
        processed_order_buy = 0

    if correlation_id_sell_profit != "":
        order_details_sell_profit =  dhan.get_order_by_correlationID(correlation_id_sell_profit)['data']
        processed_sell_profit = process_order_details(order_details_sell_profit,True)
    else:
        processed_sell_profit = 0

    if correlation_id_sell_loss != "":
        order_details_sell_loss =  dhan.get_order_by_correlationID(correlation_id_sell_loss)['data']
        processed_sell_loss = process_order_details(order_details_sell_loss,True)
    else:
        processed_sell_loss = 0

    if correlation_id_sell_be != "":
        order_details_sell_be =  dhan.get_order_by_correlationID(correlation_id_sell_be)['data']
        processed_sell_be = process_order_details(order_details_sell_be,True)
    else:
        processed_sell_be = 0

    return processed_order_buy - (processed_sell_profit+processed_sell_loss+processed_sell_be)



#-----------------------------------------------------------------------------
# place buy order

def place_buy_order(dhan, script_key: str, qty: int, trade_price: float,correlation_id):

    ORDER_QUANTITY = slice_the_order(qty)
    execution_time = datetime.now().strftime("%H%M%S")
    buy_tag = correlation_id+"_"+execution_time
    
    SLICE_ORD_QTY = ORDER_QUANTITY['slice_qty']
    NON_SLICE_ORD_QTY = ORDER_QUANTITY['non_slce_qty']
    if SLICE_ORD_QTY > 0:
        print(f"Slice order QTY = {SLICE_ORD_QTY}")
        SLICE_ORDER_BUY = dhan.place_slice_order(security_id=str(int(script_key)),    # The ID of the security to trade.
                                        exchange_segment=dhanhq.NSE_FNO,             # The exchange segment (e.g., NSE, BSE).
                                        transaction_type=dhanhq.BUY,                 # The type of transaction (BUY/SELL).
                                        quantity=str(int(SLICE_ORD_QTY)),            # The quantity of the order.
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
                                        tag= buy_tag                                 #  correlation ID for tracking.
                                        )     
    if NON_SLICE_ORD_QTY > 0:
        print(f"Non slice order QTY = {NON_SLICE_ORD_QTY}")
        NON_SLICE_ORDER_BUY = dhan.place_order(security_id=str(int(script_key)),            # The ID of the security to trade.
                                        exchange_segment=dhanhq.NSE_FNO,                    # The exchange segment (e.g., NSE, BSE).
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
                                        tag= buy_tag                                 # correlation ID for tracking.
                                        )
 
    return(buy_tag,execution_time)

def place_sell_order(dhan,script_key, order_qty, trade_price, execution_time,correlation_id):
    
    ORDER_QUANTITY = slice_the_order(order_qty)
    
    sell_tag = correlation_id+"_"+execution_time

    SLICE_ORD_QTY = ORDER_QUANTITY['slice_qty']
    NON_SLICE_ORD_QTY = ORDER_QUANTITY['non_slce_qty']
    SELL_PARAMS = {
        'EXIT_SELL_PROFIT': {
            'order_type': dhanhq.LIMIT,
            'trigger_price': float(trade_price)-0.1
        },
        'EXIT_SELL_LOSS': {
            'order_type': dhanhq.SL,
            'trigger_price': float(trade_price)+0.2
        },        
    }
    if SLICE_ORD_QTY > 0:
        print(f"Slice order QTY = {SLICE_ORD_QTY}")    
        SLICE_ORDER_SELL = dhan.place_slice_order(
                                        security_id=str(int(script_key)),                       # The ID of the security to trade.
                                        exchange_segment=dhanhq.NSE_FNO,                       # The exchange segment (e.g., NSE, BSE).
                                        transaction_type=dhanhq.SELL,                           # The type of transaction (BUY/SELL).
                                        quantity=str(int(SLICE_ORD_QTY)),                        # The quantity of the order.
                                        order_type=SELL_PARAMS[correlation_id]['order_type'],   # The type of order (LIMIT/MARKET/SL).
                                        product_type=dhanhq.INTRA,                              # The product type (CNC, MIS, etc.).
                                        price=float(trade_price), 
                                        trigger_price=SELL_PARAMS[correlation_id]['trigger_price'],                      # The price of the order.             
                                        disclosed_quantity=0,                                   # The disclosed quantity for the order. 
                                        validity='DAY',                                 # The validity of the order (DAY, IOC, etc.).
                                        amo_time='OPEN',                                # The time for AMO orders.
                                        tag=sell_tag                                   # Optional correlation ID for tracking.
                                        )
    if NON_SLICE_ORD_QTY > 0:
        print(f"Non slice order QTY = {NON_SLICE_ORD_QTY}")                                            
        NON_SLICE_ORDER_SELL = dhan.place_slice_order(
                                        security_id=str(int(script_key)),                       # The ID of the security to trade.
                                        exchange_segment=dhanhq.NSE_FNO,                          # The exchange segment (e.g., NSE, BSE).
                                        transaction_type=dhanhq.SELL,                             # The type of transaction (BUY/SELL).
                                        quantity=str(int(SLICE_ORD_QTY)),                        # The quantity of the order.
                                        order_type=SELL_PARAMS[correlation_id]['order_type'],                        # The type of order (LIMIT/MARKET/SL).
                                        product_type=dhanhq.INTRA,                              # The product type (CNC, MIS, etc.).
                                        price=float(trade_price), 
                                        trigger_price=SELL_PARAMS[correlation_id]['trigger_price'],                    # The price of the order.
                                        disclosed_quantity=0,                           # The disclosed quantity for the order. 
                                        validity='DAY',                                 # The validity of the order (DAY, IOC, etc.).
                                        amo_time='OPEN',                                # The time for AMO orders.
                                        tag=sell_tag                                   # Optional correlation ID for tracking.
                                        )
    return(sell_tag,execution_time)

def cancel_order(dhan,PENDING_ORDERS: list[str]):
    for order_id in PENDING_ORDERS:
        dhan.cancel_order(order_id)
    

#Order Management
def start_placing_orders(dhan):
    # Global Variables
    reset_sheet()
    PENDING_BUY_ORDERS = []
    PENDING_SELL_ORDERS = []

    OPEN_QTY= 0
    TRADED_QTY = 0
    AVG_PRICE = 0.0

    BUY_CORR_ID = "BUY_ENTER"
    SELL_PROFIT_CORR_ID = "SELL_PROFIT"
    SELL_LOSS_CORR_ID = "SELL_LOSS"
    SELL_BE_CORR_ID = "SELL_BE"

    BUY_TAG = ""
    SELL_TAG = ""
    SELL_PROFIT_TAG = ""
    SELL_LOSS_TAG = ""
    SELL_BE_TAG = ""

    EXEC_TIME = ""

    # Execution loop begins
    while True:
        # There are some variables that needs to be caputed in realtime from thw OrderMgmt sheet, those assignments are made below
        LIMIT_PRICE = OrderMgmt.range('LMT_PRICE').value
        LTP = OrderMgmt.range('LTP').value
        SYMBOL_KEY = OrderMgmt.range('SYMBOL_KEY').value
        TRIGGER_PROFIT = OrderMgmt.range('TRIGGER_PROFIT').value
        TRIGGER_SL = OrderMgmt.range('TRIGGER_SL').value
        PROFIT_TARGET = OrderMgmt.range('PROFIT_TARGET').value
        SL_TARGET =  OrderMgmt.range('SL_TARGET').value

        # Initiating a new trade
        # We have to check if there is no open postions already then proceed
        if OrderMgmt.range('INITIATE').value == "TRADE" and LIMIT_PRICE > 0.0 and OrderMgmt.range('TRADE_STATUS').value == "START" and LTP > LIMIT_PRICE:
            postions = len(dhan.get_positions()['data'])
            QTY = OrderMgmt.range('ORDER_PLACED_QTY').value
            if postions > 0:
                OrderMgmt.range('MESSAGE').value = "ERROR : CLOSE THE OPEN POSITIONS AND PLACE NEW ORDER!"
                reset_sheet()
            else:
                OrderMgmt.range('MESSAGE').value = "NO OPEN POSITONS! PROCEEDING..."
                BUY_TAG,EXEC_TIME = place_buy_order(dhan,SYMBOL_KEY,QTY,LIMIT_PRICE,BUY_CORR_ID)

                OrderMgmt.range('TRADE_STATUS').value = "PROCESSING"
                PENDING_BUY_ORDERS,AVG_PRICE = get_order_status(dhan, BUY_TAG)
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY
        
        # Now the order is placed, we have to track the pending order and update the quantity and avg price till it reaches Profit trigger or SL trigger
        if OrderMgmt.range('TRADE_STATUS').value == "PROCESSING" and len(PENDING_BUY_ORDERS) != 0 and (LTP <= TRIGGER_PROFIT or LTP >= TRIGGER_SL):
                PENDING_BUY_ORDERS,AVG_PRICE = get_order_status(dhan, BUY_TAG)
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY


        # when LTP crosses TRIGGER_PROFIT or TRIGGER_SL
        OrderMgmt.range('TRADE_STATUS').value = "PROCESS-EXIT-PROFIT" if LTP > TRIGGER_PROFIT else "PROCESS-EXIT-LOSS" if LTP < TRIGGER_SL else OrderMgmt.range('TRADE_STATUS').value
        

        # Processing for the trade when LTP crosses TRIGGER_PROFIT
        if OrderMgmt.range('TRADE_STATUS').value == "PROCESS-EXIT-PROFIT":
            if len(PENDING_BUY_ORDERS) != 0:
                OrderMgmt.range('MESSAGE').value = "Canceling pending 'Buy Orders' before placing Book Profit 'Sell Orders'"
                cancel_order(dhan,PENDING_BUY_ORDERS)
                PENDING_BUY_ORDERS,AVG_PRICE = get_order_status(dhan, BUY_TAG)
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE

            if len(PENDING_SELL_ORDERS) != 0:
                OrderMgmt.range('MESSAGE').value = "Canceling pending 'Sell Orders' before placing Book Profit 'Sell Orders'"
                cancel_order(PENDING_SELL_ORDERS)
                PENDING_SELL_ORDERS,AVG_PRICE = get_order_status(dhan, SELL_TAG)
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)           
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE

            if  len(PENDING_SELL_ORDERS) == 0 and len(PENDING_BUY_ORDERS) == 0:
                OrderMgmt.range('MESSAGE').value = "Placing Profit Orders"
                SELL_TAG, EXEC_TIME = place_sell_order(SYMBOL_KEY, OPEN_QTY, PROFIT_TARGET,EXEC_TIME, correlation_id=SELL_PROFIT_CORR_ID)
                OrderMgmt.range('TRADE_STATUS').value = "EXIT-PROFIT"
                SELL_PROFIT_TAG = SELL_TAG
                PENDING_SELL_ORDERS,AVG_PRICE = get_order_status(dhan, SELL_TAG)
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE  

        # Monitoring the profit orders while price is in profit zone
        if OrderMgmt.range('TRADE_STATUS').value == "EXIT-PROFIT":
            if len(PENDING_SELL_ORDERS) != 0:
                OrderMgmt.range('MESSAGE').value = "Exiting in profit, while price is in profit zone"
                PENDING_SELL_ORDERS,AVG_PRICE = get_order_status(dhan, SELL_TAG)
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE                
            else:
                print(f"No pending orders, TRADE COMPLETED SUCCESSFULLY")
                reset_sheet()


        # Processing for the trade when LTP crosses TRIGGER_SL
        if OrderMgmt.range('TRADE_STATUS').value == "PROCESS-EXIT-LOSS":
            if len(PENDING_BUY_ORDERS) != 0:
                OrderMgmt.range('MESSAGE').value = "Canceling pending 'Buy Orders' before placing Loss 'Sell Orders'"
                cancel_order(dhan,PENDING_BUY_ORDERS)
                PENDING_BUY_ORDERS,AVG_PRICE = get_order_status(dhan, BUY_TAG)
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE

            if len(PENDING_SELL_ORDERS) != 0:
                OrderMgmt.range('MESSAGE').value = "Canceling pending 'Sell Orders' before placing Loss 'Sell Orders'"
                cancel_order(PENDING_SELL_ORDERS)
                PENDING_SELL_ORDERS,AVG_PRICE = get_order_status(dhan, SELL_TAG)
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)           
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE

            if len(PENDING_SELL_ORDERS) == 0 and len(PENDING_BUY_ORDERS) == 0:
                OrderMgmt.range('MESSAGE').value = "Placing Loss Orders"
                SELL_TAG, EXEC_TIME = place_sell_order(SYMBOL_KEY, OPEN_QTY, SL_TARGET ,EXEC_TIME, correlation_id=SELL_LOSS_CORR_ID)
                OrderMgmt.range('TRADE_STATUS').value = "EXIT-LOSS"
                SELL_LOSS_TAG = SELL_TAG
                PENDING_SELL_ORDERS,AVG_PRICE = get_order_status(dhan, SELL_TAG)
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE  

        # Monitoring the profit orders while price is in profit zone
        if OrderMgmt.range('TRADE_STATUS').value == "EXIT-LOSS":
            if len(PENDING_SELL_ORDERS) != 0:
                OrderMgmt.range('MESSAGE').value = "Exiting in Loss, while price is in Loss zone"
                PENDING_SELL_ORDERS,AVG_PRICE = get_order_status(dhan, SELL_TAG)
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE                
            else:
                print(f"No pending orders, TRADE COMPLETED SUCCESSFULLY")
                reset_sheet()

        
        # Managing the Breakeven Exits
        # When Price moves above TRIGGER_PROFIT or PROFIT_TARGET, then exit at break even
        # If price move beow TRIGGER_SL or SL_TARGET and comes back to avg price, we shold not axit at breakevenas the chances are there to make profit.
        # these logics were covered here.
        if OrderMgmt.range('TRADE_STATUS').value == "EXIT-LOSS"  and (OrderMgmt.range('TRADE_STATUS').value == "PROCESS-EXIT-PROFIT" or OrderMgmt.range('TRADE_STATUS').value == "EXIT-PROFIT") and OrderMgmt.range('INITIATE').value == "TRADE" and OrderMgmt.range('AVG_BUY_PRICE').value>0.0 and LTP < OrderMgmt.range('AVG_BUY_PRICE').value:
            OrderMgmt.range('TRADE_STATUS').value = "PROCESS-EXIT-BE"

        if OrderMgmt.range('TRADE_STATUS').value == "PROCESS-EXIT-BE":
            if len(PENDING_BUY_ORDERS) != 0:
                OrderMgmt.range('MESSAGE').value = "Canceling pending 'Buy Orders' before placing Breakeven Loss 'Sell Orders'"
                cancel_order(dhan,PENDING_BUY_ORDERS)
                PENDING_BUY_ORDERS,AVG_PRICE = get_order_status(dhan, BUY_TAG)
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE

            if len(PENDING_SELL_ORDERS) != 0:
                OrderMgmt.range('MESSAGE').value = "Canceling pending 'Sell Orders' before placing Breakeven Loss 'Sell Orders'"
                cancel_order(PENDING_SELL_ORDERS)
                PENDING_SELL_ORDERS,AVG_PRICE = get_order_status(dhan, SELL_TAG)
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)           
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE            

            if len(PENDING_SELL_ORDERS) == 0 and len(PENDING_BUY_ORDERS) == 0:
                OrderMgmt.range('MESSAGE').value = "Placing Breakeven Loss Orders"
                AVG_PRICE = OrderMgmt.range('AVG_BUY_PRICE').value
                SELL_TAG, EXEC_TIME = place_sell_order(SYMBOL_KEY, OPEN_QTY, AVG_PRICE ,EXEC_TIME, correlation_id=SELL_BE_CORR_ID)
                OrderMgmt.range('TRADE_STATUS').value = "EXIT-BE"
                SELL_BE_TAG = SELL_TAG
                PENDING_SELL_ORDERS,AVG_PRICE = get_order_status(dhan, SELL_TAG)
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE

        # Monitoring the Exit Breakeven orders and exiting gradually
        if OrderMgmt.range('TRADE_STATUS').value == "EXIT-BE":
            if len(PENDING_SELL_ORDERS) != 0:
                OrderMgmt.range('MESSAGE').value = "Exiting in Loss, while price is in Breakeven zone"
                PENDING_SELL_ORDERS,AVG_PRICE = get_order_status(dhan, SELL_TAG)
                OPEN_QTY = get_updated_open_trades(dhan,BUY_TAG, SELL_PROFIT_TAG, SELL_LOSS_TAG, SELL_BE_TAG)
                OrderMgmt.range('BUY_QTY').value = OPEN_QTY
                OrderMgmt.range('AVG_BUY_PRICE').value = AVG_PRICE                
            else:
                print(f"No pending orders, TRADE COMPLETED SUCCESSFULLY")
                reset_sheet()   


              

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
        start_placing_orders(dhan)
    except KeyboardInterrupt:
        print("\n⚠️ Order management stopped by user")
    except Exception as e:
        print(f"❌ Fatal error in order management: {e}")
        raise

if __name__ == "__main__":
    main()
