from dhanhq import marketfeed
import xlwings as xw

# Add your Dhan Client ID and Access Token
client_id = "1100381471"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzMzOTg0NjAyLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDM4MTQ3MSJ9.58q269mGAwsKb0B0PMJPvD4N9La5MyXXFrW4JnPHNg4bNix7NMLoGQiBX1OiEL2frCsbrOCWwUKSs978lofLUg"


excel_file = 'Dhan_orders.xlsx'
workbook = xw.Book(excel_file)
crude_options_chain_sheet = workbook.sheets['CRUDE']
CRUDE_CE_CURRENT = crude_options_chain_sheet.range('D3:D13').value


# Structure for subscribing is (exchange_segment, "security_id", subscription_type)

instruments = [(marketfeed.MCX,f"{str(int(x))}",marketfeed.Ticker) for x in CRUDE_CE_CURRENT]
print(instruments)

version = "v2"          # Mention Version and set to latest version 'v2'

# In case subscription_type is left as blank, by default Ticker mode will be subscribed.

# try:
#     data = marketfeed.DhanFeed(client_id, access_token, instruments, version)
#     while True:
#         data.run_forever()
#         response = data.get_data()
#         print(response)

# except Exception as e:
#     print(e)

