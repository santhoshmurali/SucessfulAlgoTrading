import os
import sys
import ast
import xlwings as xw
from dhanhq import dhanhq, DhanContext
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# CONFIGURATION & GLOBAL SETUP
# ============================================================================

# Load Excel workbook
excel_file = os.getenv('TradingSystemnNew', 'PValue_Pulse.xlsx')
workbook = xw.Book(excel_file)
TradeSheet = workbook.sheets['Trade']
OrderMgmt = workbook.sheets['TRADEMGMT']
OrdersSheet = workbook.sheets['Orders']

# Load freeze quantity mapping from environment
def load_freeze_qty_map() -> Dict[str, int]:
    """Load freeze quantity mapping from environment variables."""
    try:
        freeze_qty_str = os.getenv('freeze_qty_order', '{}')
        return ast.literal_eval(freeze_qty_str)
    except (ValueError, SyntaxError) as e:
        print(f"Warning: Could not parse freeze_qty_order: {e}")
        return {}

FREEZE_QTY_MAP = load_freeze_qty_map()

# ============================================================================
# CONNECTION MANAGEMENT
# ============================================================================

def connect_to_dhan(sandbox: bool = False) -> Dict:
    """
    Establish connection to Dhan API.
    
    Args:
        sandbox: If True, connects to sandbox environment
        
    Returns:
        Dictionary containing connection objects and credentials
    """
    # Get credentials based on environment
    if sandbox:
        client_id = os.getenv('SB_DHAN_CLIENT_ID')
        access_token = os.getenv('SB_DHAN_ACCESS_TOKEN')
    else:
        client_id = os.getenv('DHAN_CLIENT_ID')
        access_token = os.getenv('DHAN_ACCESS_TOKEN')
    
    # Create Dhan context and connection
    try:
        dhan_context = DhanContext(client_id, access_token, use_sandbox=sandbox)
        dhan = dhanhq(dhan_context)
    except Exception as e:
        raise ConnectionError(f"Cannot connect to Dhan API: {e}")
    
    return {
        "connection": dhan,
        "client_id": client_id,
        "access_token": access_token,
        "dhan_context": dhan_context
    }

# ============================================================================
# ORDER SLICING
# ============================================================================

def slice_the_order(total_qty: int) -> Dict[str, int]:
    """
    Slice order quantity based on freeze quantity limits.
    
    Args:
        total_qty: Total quantity to be sliced
        
    Returns:
        Dictionary with 'slice_qty' (quantity needing slicing) and 
        'non_slice_qty' (remaining quantity)
    """
    # Get index name from Excel sheet
    index_name_full = OrderMgmt.range('INDEX_NAME').value
    index_name = index_name_full[:5] if index_name_full else ""
    
    # Get freeze quantity for this index
    freeze_qty = FREEZE_QTY_MAP.get(index_name, 0)
    
    if freeze_qty == 0:
        raise ValueError(f"Freeze quantity not found for index: {index_name}")
    
    # Calculate slices
    if total_qty <= freeze_qty:
        return {"slice_qty": 0, "non_slice_qty": total_qty}
    else:
        num_slices = total_qty // freeze_qty
        slice_qty = num_slices * freeze_qty
        non_slice_qty = total_qty % freeze_qty
        return {"slice_qty": slice_qty, "non_slice_qty": non_slice_qty}

# ============================================================================
# ORDER PROCESSING
# ============================================================================

def process_order_details(order_details: List[Dict], qty_only: bool = False) -> Dict:
    """
    Process order details to extract pending orders and average price.
    
    Args:
        order_details: List of order dictionaries from Dhan API
        qty_only: If True, returns only total filled quantity
        
    Returns:
        Dictionary with pending_order_ids and average_price, or just quantity
    """
    pending_order_ids = []
    sumproduct = 0.0
    total_traded_qty = 0
    
    for order in order_details:
        order_status = order.get('orderStatus')
        order_id = order.get('orderId')
        filled_qty = order.get('filledQty', 0)
        avg_price = order.get('averageTradedPrice', 0.0)
        
        # Collect pending/transit orders
        if order_status in ['PENDING', 'TRANSIT']:
            pending_order_ids.append(order_id)
        
        # Calculate weighted average for traded orders
        if order_status == 'TRADED':
            sumproduct += filled_qty * avg_price
            total_traded_qty += filled_qty
    
    # Calculate average price
    avg_price = sumproduct / total_traded_qty if total_traded_qty > 0 else 0.0
    
    if qty_only:
        return total_traded_qty
    
    return {
        'pending_order_ids': pending_order_ids,
        'average_price': avg_price
    }

def get_order_status(dhan, correlation_id: str) -> Tuple[List[str], float]:
    """
    Get order status by correlation ID.
    
    Returns:
        Tuple of (pending_order_ids, average_price)
    """
    order_details = dhan.get_order_by_correlationID(correlation_id)['data']
    processed = process_order_details(order_details)
    return processed['pending_order_ids'], processed['average_price']

def get_net_open_quantity(dhan, buy_tag: str, sell_profit_tag: str, 
                           sell_loss_tag: str, sell_be_tag: str) -> int:
    """
    Calculate net open quantity across all order types.
    
    Returns:
        Net quantity (buy - sell)
    """
    def get_qty(tag: str) -> int:
        if not tag:
            return 0
        order_details = dhan.get_order_by_correlationID(tag)['data']
        return process_order_details(order_details, qty_only=True)
    
    buy_qty = get_qty(buy_tag)
    sell_profit_qty = get_qty(sell_profit_tag)
    sell_loss_qty = get_qty(sell_loss_tag)
    sell_be_qty = get_qty(sell_be_tag)
    
    return buy_qty - (sell_profit_qty + sell_loss_qty + sell_be_qty)

# ============================================================================
# ORDER PLACEMENT
# ============================================================================

def place_buy_order(dhan, script_key: str, qty: int, trade_price: float, 
                    correlation_id: str) -> Tuple[str, str]:
    """
    Place buy order with automatic slicing if needed.
    
    Returns:
        Tuple of (buy_tag, execution_time)
    """
    order_slices = slice_the_order(qty)
    execution_time = datetime.now().strftime("%H%M%S")
    buy_tag = f"{correlation_id}_{execution_time}"
    
    # Common order parameters
    base_params = {
        "security_id": str(int(script_key)),
        "exchange_segment": dhanhq.NSE_FNO,
        "transaction_type": dhanhq.BUY,
        "order_type": dhanhq.LIMIT,
        "product_type": dhanhq.INTRA,
        "price": trade_price,
        "disclosed_quantity": 0,
        "after_market_order": False,
        "validity": 'DAY',
        "amo_time": 'OPEN',
        "bo_profit_value": None,
        "bo_stop_loss_Value": None,
        "tag": buy_tag
    }
    
    # Place slice order if needed
    if order_slices['slice_qty'] > 0:
        print(f"Placing slice order: {order_slices['slice_qty']} qty")
        dhan.place_slice_order(
            **base_params,
            quantity=str(int(order_slices['slice_qty'])),
            trigger_price=trade_price - 0.1
        )
    
    # Place non-slice order
    if order_slices['non_slice_qty'] > 0:
        print(f"Placing non-slice order: {order_slices['non_slice_qty']} qty")
        dhan.place_order(
            **{k: v for k, v in base_params.items() if k != 'trigger_price'},
            quantity=str(int(order_slices['non_slice_qty']))
        )
    
    return buy_tag, execution_time

def place_sell_order(dhan, script_key: str, order_qty: int, trade_price: float, 
                     execution_time: str, correlation_id: str) -> Tuple[str, str]:
    """
    Place sell order with automatic slicing if needed.
    
    Returns:
        Tuple of (sell_tag, execution_time)
    """
    order_slices = slice_the_order(order_qty)
    sell_tag = f"{correlation_id}_{execution_time}"
    
    # Sell parameters based on order type
    sell_params = {
        'EXIT_SELL_PROFIT': {
            'order_type': dhanhq.LIMIT,
            'trigger_price': trade_price - 0.1
        },
        'EXIT_SELL_LOSS': {
            'order_type': dhanhq.SL,
            'trigger_price': trade_price + 0.2
        },
    }
    
    # Get parameters for this correlation ID (handle breakeven case)
    params = sell_params.get(correlation_id, {
        'order_type': dhanhq.LIMIT,
        'trigger_price': trade_price - 0.1
    })
    
    # Common order parameters
    base_params = {
        "security_id": str(int(script_key)),
        "exchange_segment": dhanhq.NSE_FNO,
        "transaction_type": dhanhq.SELL,
        "order_type": params['order_type'],
        "product_type": dhanhq.INTRA,
        "price": float(trade_price),
        "trigger_price": params['trigger_price'],
        "disclosed_quantity": 0,
        "validity": 'DAY',
        "amo_time": 'OPEN',
        "tag": sell_tag
    }
    
    # Place slice order if needed
    if order_slices['slice_qty'] > 0:
        print(f"Placing sell slice order: {order_slices['slice_qty']} qty")
        dhan.place_slice_order(**base_params, quantity=str(int(order_slices['slice_qty'])))
    
    # Place non-slice order
    if order_slices['non_slice_qty'] > 0:
        print(f"Placing sell non-slice order: {order_slices['non_slice_qty']} qty")
        dhan.place_order(**base_params, quantity=str(int(order_slices['non_slice_qty'])))
    
    return sell_tag, execution_time

def cancel_orders(dhan, order_ids: List[str]) -> None:
    """Cancel multiple orders by their IDs."""
    for order_id in order_ids:
        try:
            dhan.cancel_order(order_id)
        except Exception as e:
            print(f"Error canceling order {order_id}: {e}")

# ============================================================================
# SHEET MANAGEMENT
# ============================================================================

def reset_sheet() -> None:
    """Reset order management sheet to initial state."""
    OrderMgmt.range('INITIATE').value = "STOP"
    OrderMgmt.range('TRADE_STATUS').value = "START"
    OrderMgmt.range('LMT_PRICE').value = None
    OrderMgmt.range('AVG_BUY_PRICE').value = 0.0
    OrderMgmt.range('BUY_QTY').value = 0
    OrderMgmt.range('MESSAGE').value = ""

def update_order_mgmt(open_qty: int, avg_price: float) -> None:
    """Update order management sheet with current values."""
    OrderMgmt.range('BUY_QTY').value = open_qty
    OrderMgmt.range('AVG_BUY_PRICE').value = avg_price

def get_sheet_values() -> Dict:
    """Get current values from order management sheet."""
    return {
        'limit_price': OrderMgmt.range('LMT_PRICE').value,
        'ltp': OrderMgmt.range('LTP').value,
        'symbol_key': OrderMgmt.range('SYMBOL_KEY').value,
        'trigger_profit': OrderMgmt.range('TRIGGER_PROFIT').value,
        'trigger_sl': OrderMgmt.range('TRIGGER_SL').value,
        'profit_target': OrderMgmt.range('PROFIT_TARGET').value,
        'sl_target': OrderMgmt.range('SL_TARGET').value,
        'initiate': OrderMgmt.range('INITIATE').value,
        'trade_status': OrderMgmt.range('TRADE_STATUS').value,
        'order_qty': OrderMgmt.range('ORDER_PLACED_QTY').value,
        'avg_buy_price': OrderMgmt.range('AVG_BUY_PRICE').value
    }

# ============================================================================
# MAIN ORDER MANAGEMENT LOGIC
# ============================================================================

def start_placing_orders(dhan):
    """Main order management loop."""
    
    # Initialize state variables
    reset_sheet()
    
    pending_buy_orders = []
    pending_sell_orders = []
    
    # Correlation IDs
    BUY_CORR_ID = "BUY_ENTER"
    SELL_PROFIT_CORR_ID = "EXIT_SELL_PROFIT"
    SELL_LOSS_CORR_ID = "EXIT_SELL_LOSS"
    SELL_BE_CORR_ID = "EXIT_SELL_BE"
    
    # Tags for tracking orders
    buy_tag = ""
    sell_tag = ""
    sell_profit_tag = ""
    sell_loss_tag = ""
    sell_be_tag = ""
    exec_time = ""
    
    print("Order management system started...")
    
    while True:
        try:
            # Get current sheet values
            sheet_values = get_sheet_values()
            
            # ================================================================
            # 1. INITIATE NEW TRADE
            # ================================================================
            if (sheet_values['initiate'] == "TRADE" and 
                sheet_values['limit_price'] and sheet_values['limit_price'] > 0.0 and
                sheet_values['trade_status'] == "START" and 
                sheet_values['ltp'] > sheet_values['limit_price']):
                
                # Check for existing positions
                positions = len(dhan.get_positions()['data'])
                
                if positions > 0:
                    OrderMgmt.range('MESSAGE').value = "ERROR: Close open positions first!"
                    reset_sheet()
                    continue
                
                # Place buy order
                OrderMgmt.range('MESSAGE').value = "Placing buy order..."
                buy_tag, exec_time = place_buy_order(
                    dhan, sheet_values['symbol_key'], 
                    sheet_values['order_qty'], 
                    sheet_values['limit_price'], 
                    BUY_CORR_ID
                )
                
                OrderMgmt.range('TRADE_STATUS').value = "PROCESSING"
                pending_buy_orders, avg_price = get_order_status(dhan, buy_tag)
                open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                update_order_mgmt(open_qty, avg_price)
            
            # ================================================================
            # 2. MONITOR PROCESSING ORDERS
            # ================================================================
            if (sheet_values['trade_status'] == "PROCESSING" and 
                len(pending_buy_orders) > 0):
                
                # Update order status
                pending_buy_orders, avg_price = get_order_status(dhan, buy_tag)
                open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                update_order_mgmt(open_qty, avg_price)
                
                # Check for exit triggers
                if sheet_values['ltp'] >= sheet_values['trigger_profit']:
                    OrderMgmt.range('TRADE_STATUS').value = "PROCESS-EXIT-PROFIT"
                elif sheet_values['ltp'] <= sheet_values['trigger_sl']:
                    OrderMgmt.range('TRADE_STATUS').value = "PROCESS-EXIT-LOSS"
            
            # ================================================================
            # 3. PROCESS PROFIT EXIT
            # ================================================================
            if sheet_values['trade_status'] == "PROCESS-EXIT-PROFIT":
                
                # Cancel pending orders
                if pending_buy_orders:
                    OrderMgmt.range('MESSAGE').value = "Canceling pending buy orders..."
                    cancel_orders(dhan, pending_buy_orders)
                    pending_buy_orders, avg_price = get_order_status(dhan, buy_tag)
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    update_order_mgmt(open_qty, avg_price)
                
                if pending_sell_orders:
                    OrderMgmt.range('MESSAGE').value = "Canceling pending sell orders..."
                    cancel_orders(dhan, pending_sell_orders)
                    pending_sell_orders, avg_price = get_order_status(dhan, sell_tag)
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    update_order_mgmt(open_qty, avg_price)
                
                # Place profit exit orders
                if not pending_sell_orders and not pending_buy_orders:
                    OrderMgmt.range('MESSAGE').value = "Placing profit exit orders..."
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    sell_tag, exec_time = place_sell_order(
                        dhan, sheet_values['symbol_key'], open_qty, 
                        sheet_values['profit_target'], exec_time, SELL_PROFIT_CORR_ID
                    )
                    sell_profit_tag = sell_tag
                    OrderMgmt.range('TRADE_STATUS').value = "EXIT-PROFIT"
                    pending_sell_orders, avg_price = get_order_status(dhan, sell_tag)
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    update_order_mgmt(open_qty, avg_price)
            
            # ================================================================
            # 4. MONITOR PROFIT EXIT
            # ================================================================
            if sheet_values['trade_status'] == "EXIT-PROFIT":
                if pending_sell_orders:
                    pending_sell_orders, avg_price = get_order_status(dhan, sell_tag)
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    update_order_mgmt(open_qty, avg_price)
                else:
                    print("Trade completed successfully (Profit)")
                    reset_sheet()
                    continue
            
            # ================================================================
            # 5. PROCESS LOSS EXIT
            # ================================================================
            if sheet_values['trade_status'] == "PROCESS-EXIT-LOSS":
                
                # Cancel pending orders
                if pending_buy_orders:
                    OrderMgmt.range('MESSAGE').value = "Canceling pending buy orders..."
                    cancel_orders(dhan, pending_buy_orders)
                    pending_buy_orders, avg_price = get_order_status(dhan, buy_tag)
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    update_order_mgmt(open_qty, avg_price)
                
                if pending_sell_orders:
                    OrderMgmt.range('MESSAGE').value = "Canceling pending sell orders..."
                    cancel_orders(dhan, pending_sell_orders)
                    pending_sell_orders, avg_price = get_order_status(dhan, sell_tag)
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    update_order_mgmt(open_qty, avg_price)
                
                # Place loss exit orders
                if not pending_sell_orders and not pending_buy_orders:
                    OrderMgmt.range('MESSAGE').value = "Placing stop loss orders..."
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    sell_tag, exec_time = place_sell_order(
                        dhan, sheet_values['symbol_key'], open_qty, 
                        sheet_values['sl_target'], exec_time, SELL_LOSS_CORR_ID
                    )
                    sell_loss_tag = sell_tag
                    OrderMgmt.range('TRADE_STATUS').value = "EXIT-LOSS"
                    pending_sell_orders, avg_price = get_order_status(dhan, sell_tag)
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    update_order_mgmt(open_qty, avg_price)
            
            # ================================================================
            # 6. MONITOR LOSS EXIT
            # ================================================================
            if sheet_values['trade_status'] == "EXIT-LOSS":
                if pending_sell_orders:
                    pending_sell_orders, avg_price = get_order_status(dhan, sell_tag)
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    update_order_mgmt(open_qty, avg_price)
                else:
                    print("Trade completed (Stop Loss)")
                    reset_sheet()
                    continue
            
            # ================================================================
            # 7. BREAKEVEN EXIT LOGIC
            # ================================================================
            if (sheet_values['trade_status'] in ["EXIT-PROFIT", "PROCESS-EXIT-PROFIT"] and
                sheet_values['initiate'] == "TRADE" and
                sheet_values['avg_buy_price'] > 0 and
                sheet_values['ltp'] < sheet_values['avg_buy_price']):
                OrderMgmt.range('TRADE_STATUS').value = "PROCESS-EXIT-BE"
            
            if sheet_values['trade_status'] == "PROCESS-EXIT-BE":
                
                # Cancel pending orders
                if pending_buy_orders:
                    cancel_orders(dhan, pending_buy_orders)
                    pending_buy_orders, avg_price = get_order_status(dhan, buy_tag)
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    update_order_mgmt(open_qty, avg_price)
                
                if pending_sell_orders:
                    cancel_orders(dhan, pending_sell_orders)
                    pending_sell_orders, avg_price = get_order_status(dhan, sell_tag)
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    update_order_mgmt(open_qty, avg_price)
                
                # Place breakeven exit orders
                if not pending_sell_orders and not pending_buy_orders:
                    OrderMgmt.range('MESSAGE').value = "Placing breakeven exit orders..."
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    avg_price = sheet_values['avg_buy_price']
                    sell_tag, exec_time = place_sell_order(
                        dhan, sheet_values['symbol_key'], open_qty, 
                        avg_price, exec_time, SELL_BE_CORR_ID
                    )
                    sell_be_tag = sell_tag
                    OrderMgmt.range('TRADE_STATUS').value = "EXIT-BE"
                    pending_sell_orders, avg_price = get_order_status(dhan, sell_tag)
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    update_order_mgmt(open_qty, avg_price)
            
            if sheet_values['trade_status'] == "EXIT-BE":
                if pending_sell_orders:
                    pending_sell_orders, avg_price = get_order_status(dhan, sell_tag)
                    open_qty = get_net_open_quantity(dhan, buy_tag, sell_profit_tag, sell_loss_tag, sell_be_tag)
                    update_order_mgmt(open_qty, avg_price)
                else:
                    print("Trade completed (Breakeven)")
                    reset_sheet()
        
        except Exception as e:
            print(f"Error in order management loop: {e}")
            OrderMgmt.range('MESSAGE').value = f"Error: {str(e)}"

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point for order management system."""
    
    # Validate command line arguments
    if len(sys.argv) < 2:
        print("Usage: python script.py [dev|prod]")
        sys.exit(1)
    
    environment = sys.argv[1].lower()
    
    if environment not in ['dev', 'prod']:
        print(f"Invalid environment: {environment}. Use 'dev' or 'prod'")
        sys.exit(1)
    
    # Connect to appropriate environment
    sandbox = (environment == 'dev')
    print(f"Connecting to {'SANDBOX' if sandbox else 'PRODUCTION'} environment...")
    
    try:
        connections = connect_to_dhan(sandbox=sandbox)
        dhan = connections['connection']
        print(f"Connected successfully to {environment.upper()} environment")
        print("Order management system started!")
        
        # Start order management
        start_placing_orders(dhan)
        
    except KeyboardInterrupt:
        print("\nOrder management stopped by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()