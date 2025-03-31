#!/bin/sh

input="$1"  # Read first argument
input2="$2"

echo "%$input%"  # Print the value
echo "%$input2%"

if [ "$input2" = "lc" ] || [ "$input" = "lc" ]; then
    chmod +x kite_login_callback.py
    ./kite_login_callback.py
fi

if [ "$input" = "dm" ]; then
    chmod +x db_migrations.py
    ./db_migrations.py
fi

if [ "$input" = "di" ]; then
    chmod +x data_importer.py
    ./data_importer.py
fi

if [ "$input" = "lt" ]; then
    chmod +x live_trading.py
    caffeinate -i ./live_trading.py
fi

if [ "$input" = "tp" ]; then
    chmod +x trading_poc.py
    ./trading_poc.py
fi

if [ "$input" = "tl" ]; then
    chmod +x test_live_trading.py
    ./test_live_trading.py
fi