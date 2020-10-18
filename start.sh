file="ape.py"
source env/bin/activate
pip install -r requirements.txt
screen -S ${PWD##*/} -dm python $file
#python $file
