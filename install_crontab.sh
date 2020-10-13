name="arketf"
echo "cd ${PWD} && ./start.sh" > /etc/cron.daily/$name
echo "@reboot ${USER} cd ${PWD}/out && screen -S ark_etf_out -dm python3 -m http.server 3334 >/dev/null 2>&1" > /etc/cron.d/rebootfs_$name

