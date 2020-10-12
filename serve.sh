cd out
screen -X -S arketf_out quit
screen -S arketf_out -dm python3 -m http.server 3334
