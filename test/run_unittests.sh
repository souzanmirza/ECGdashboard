echo "[$(date)]" &>> results.txt
python -m unittest discover -v &>> results.txt

read -p "Press enter to continue"
