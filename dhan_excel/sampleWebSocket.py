from dhanhq import marketfeed

# Add your Dhan Client ID and Access Token
client_id = "1100381471"
access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzMzOTg0NjAyLCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDM4MTQ3MSJ9.58q269mGAwsKb0B0PMJPvD4N9La5MyXXFrW4JnPHNg4bNix7NMLoGQiBX1OiEL2frCsbrOCWwUKSs978lofLUg"

# Structure for subscribing is (exchange_segment, "security_id", subscription_type)

instruments = [(marketfeed.MCX, "435823", marketfeed.Ticker),   # Ticker - Ticker Data
    (marketfeed.MCX, "439863", marketfeed.Ticker),     # Quote - Quote Data
    (marketfeed.MCX, "439913", marketfeed.Ticker),      # Full - Full Packet
    (marketfeed.MCX, "439833", marketfeed.Ticker),
    (marketfeed.MCX, "439834", marketfeed.Ticker)]

version = "v2"          # Mention Version and set to latest version 'v2'

# In case subscription_type is left as blank, by default Ticker mode will be subscribed.

try:
    data = marketfeed.DhanFeed(client_id, access_token, instruments, version)
    while True:
        data.run_forever()
        response = data.get_data()
        print(response)

except Exception as e:
    print(e)

