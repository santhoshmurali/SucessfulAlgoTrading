#streamlit: This library is used to build an interactive web interface.
#dhanhq.marketfeed: Contains the DhanFeed class to interact with the WebSocket for trading data.
#threading: Allows running WebSocket operations on a separate thread without blocking the Streamlit UI.
#queue.Queue: A thread-safe queue to exchange data between the WebSocket thread and the main Streamlit thread.
import streamlit as st
from dhanhq import marketfeed
import threading
import queue

#client_id and access_token: Credentials required to authenticate the WebSocket connection.
#data_queue: Stores real-time data received from the WebSocket in a thread-safe way.
#ws_client: Global variable to maintain the WebSocket connection.
client_id = "1100381471"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzMzOTg0NjAyLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDM4MTQ3MSJ9.58q269mGAwsKb0B0PMJPvD4N9La5MyXXFrW4JnPHNg4bNix7NMLoGQiBX1OiEL2frCsbrOCWwUKSs978lofLUg"
data_queue = queue.Queue()
ws_client = None

# websocket_handler:
# A function that initializes the WebSocket connection using the DhanFeed class.
# Subscribes to a blank list of instruments initially (instruments = []).
# Uses an infinite loop (while True) to maintain the WebSocket connection and fetch data continuously.

# run_forever:
# Keeps the WebSocket connection alive and listens for updates from the server.

# get_data:
# Fetches real-time data from the WebSocket.

# data_queue.put(response):
# Places the received data into a thread-safe queue so it can be displayed in the Streamlit UI.
# Exception Handling:
# If an error occurs, it’s logged into the data_queue so the main application knows about it.

def websocket_handler():
    global ws_client
    instruments = [(marketfeed.NSE, "1333", marketfeed.Ticker)]  # Start with no subscriptions
    version = "v2"
    print(instruments)
    ws_client = marketfeed.DhanFeed(client_id, access_token, instruments, version)
    try:
        
        print(ws_client)
        while True:
            ws_client.run_forever()
            response = ws_client.get_data()

            if response:
                data_queue.put(response)
    except Exception as e:
        data_queue.put({"error": str(e)})
    finally:
        if ws_client:
            ws_client.disconnect()


# threading.Thread:
# Starts the websocket_handler in a separate thread to keep the WebSocket running independently of Streamlit’s main thread.

# st.session_state:
# Streamlit’s state management object ensures that the WebSocket thread persists across reruns of the app (e.g., when a button is clicked).
if "websocket_thread" not in st.session_state:
    websocket_thread = threading.Thread(target=websocket_handler, daemon=True)
    websocket_thread.start()
    st.session_state["websocket_thread"] = websocket_thread


##Streamlit UI for Subscriptions
# st.sidebar:
# Used to create a sidebar UI for managing subscriptions and unsubscriptions.

# selectbox and text_input:
# Allow users to specify the exchange, security ID, and subscription type dynamically.

# marketfeed.Ticker, marketfeed.Quote, marketfeed.Full:
# Constants for the type of data packets you can subscribe to:
# Ticker: Real-time price updates.
# Quote: Bid/ask price information.
# Full: Detailed market data.


subscription_type_mapping = {
    marketfeed.Ticker: "Ticker",
    marketfeed.Quote: "Quote",
    marketfeed.Full: "Full",
}

st.sidebar.header("Manage Subscriptions")
st.button("Start",on_click=websocket_handler)
exchange_segment = st.sidebar.selectbox("Exchange Segment", ["NSE", "BSE"])
security_id = st.sidebar.text_input("Security ID")
subscription_type = st.sidebar.selectbox(
    "Subscription Type",
    list(subscription_type_mapping.keys()),
    format_func=lambda x: subscription_type_mapping[x],
)

def subscribe_instruments():
    print(ws_client) 
    if ws_client:
        ws_client.subscribe_symbols([(getattr(marketfeed, exchange_segment), security_id, subscription_type)])
        st.sidebar.success(f"Subscribed to {security_id}.")

def unsubscribe_instruments():
    if ws_client:
        ws_client.unsubscribe_symbols([(getattr(marketfeed, exchange_segment), security_id, subscription_type)])
        st.sidebar.success(f"Unsubscribed from {security_id}.")

##Subscribe/Unsubscribe Buttons
subscribe_btn =  st.sidebar.button("Subscribe",on_click=subscribe_instruments)
unsubscribe_btn =  st.sidebar.button("Unsubscribe", on_click=unsubscribe_instruments)





#st.sidebar.button():
#Creates buttons to trigger subscription and unsubscription actions.

#subscribe_symbols and unsubscribe_symbols:
#Methods provided by DhanFeed to dynamically manage active subscriptions.

#getattr(marketfeed, exchange_segment):
#Dynamically converts the selected exchange (e.g., "NSE") to the corresponding constant in the marketfeed module.


## Displaying Live Data
st.header("Live Market Feed Data")
st.write("Incoming market data will be displayed below:")

while not data_queue.empty():
    
    data = data_queue.get()
    st.json(data)

#st.header and st.write:
#Adds a section in the Streamlit app to display live data updates.

# data_queue.get():
# Retrieves data from the WebSocket queue to display in real-time.

# st.json(data):
# Formats and displays the WebSocket data as JSON for better readability."""




