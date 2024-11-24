import streamlit as st
from connect_to_dhan import Connection

# Maintaining Session State and Global Variables

if "dhan_obj" not in st.session_state:
    print("Connection is not created")
    connect_btn_visibility = False
else:
    print(st.session_state.dhan_obj)
    connect_btn_visibility = True 

@st.cache_data
def go_connect_to_dhan(clientid,accesstoken):
    DhanConnector = Connection(clientid,accesstoken)
    st.session_state.dhan_obj = DhanConnector.connect_dhan()
    return st.session_state.dhan_obj 

if "NIFTY_CAPITAL" not in st.session_state:
    capital_ready = False
else:
    capital_ready = True


## We are going to have as Side bar and 3 Rows or Orders
with st.sidebar:
    APPLICAITON_ID = st.text_input("Application ID",key="app_id")
    CLIENT_ID = st.text_input("Client ID", key="client_id")
    ACCESS_TOKEN = st.text_input("Access Token", key="access_token")
    CONNECT_BUTTON = st.button("Connect",on_click=go_connect_to_dhan,args=(CLIENT_ID,ACCESS_TOKEN),disabled=connect_btn_visibility)
    if "dhan_obj" in st.session_state:
    #This is based on return value from Connect method
    #You need to cache Dhan connect object and all the above tokens
        CONNECTION_STATUS = st.empty()
        if st.session_state.dhan_obj['status'] == "success":
            CONNECTION_STATUS.success("Connected")
        else:
            CONNECTION_STATUS.error(f"Couldn't connect : {st.session_state.dhan_obj['error']}")

    st.markdown("---")
    if capital_ready:
        OverallCapital = st.empty()
        OverallCapital.markdown(f"### 💰 Total Capital : {float(st.session_state.NIFTY_CAPITAL)+float(st.session_state.BNIFTY_CAPITAL)+float(st.session_state.CRUDE_CAPITAL)}")
    profit_factor = st.slider("Profit Factor",min_value=1, max_value=5, value=1)
    loss_factor = st.slider("Loss Factor",min_value=1, max_value=5, value=1)
    test_order =st.checkbox("Test Order",value=False)


niftycontainer = st.container(height=210)
with niftycontainer:
    header, capital = st.columns(2)
    header.subheader("NIFTY")
    capital.text_input("Capital",0.0,key="NIFTY_CAPITAL")
    niftycontrols = st.columns(7,gap="medium")
    for i, controls in enumerate(niftycontrols, start=1):
        if i<6:
            controls.button(f"ITM-{i}", key=f"BTN_NIFTY_ITM_{i}") 
            controls.checkbox(f"ITM-{i}", key=f"CHK_NIFTY_ITM_{i}")
        elif i==6:
            controls.button("BUY","NIFTY_BUY_ALL")
        else:
            controls.text("1.4%")            

bankniftycontainer = st.container(height=210)
with bankniftycontainer:
    header, capital = st.columns(2)
    header.subheader("BANKNIFTY")
    capital.text_input("Capital",0.0,key="BNIFTY_CAPITAL")
    bankniftycontrols = st.columns(7,gap="medium")
    for i, controls in enumerate(bankniftycontrols, start=1):
        if i<6:
            controls.button(f"ITM-{i}", key=f"BTN_BNIFTY_ITM_{i}") 
            controls.checkbox(f"ITM-{i}", key=f"CHK_BNIFTY_ITM_{i}")
        elif i==6:
            controls.button("BUY","BNIFTY_BUY_ALL")
        else:
            controls.text("1.4%")     

crudecontainer = st.container(height=210)
with crudecontainer:
    header, capital = st.columns(2)
    header.subheader("CRUDEOIL")
    capital.text_input("Capital",0.0,key="CRUDE_CAPITAL")
    crudecontrols = st.columns(7,gap="medium")
    for i, controls in enumerate(crudecontrols, start=1):
        if i<6:
            controls.button(f"ITM-{i}", key=f"BTN_CRUDE_ITM_{i}") 
            controls.checkbox(f"ITM-{i}", key=f"CHK_CRUDE_ITM_{i}")
        elif i==6:
            controls.button("BUY","CRUDE_BUY_ALL")
        else:
            controls.text("1.4%")  

