echo "* */6 * * * cd ${PWD} && source env/bin/activate && pip install -r requirements.txt && python ape.py > /dev/null 2>&1" > /etc/cron.d/ark_etf
