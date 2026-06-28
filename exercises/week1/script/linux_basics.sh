#!/usr/bin/env bash
set -euo pipefail

# Script luyen cac lenh Linux co ban trong container workspace.
# Chay:
# docker compose exec workspace bash exercises/week1/script/linux_basics.sh

echo "Thu muc hien tai:"
pwd

echo
echo "Cac file CSV Olist:"
ls -lh data/olist/*.csv

echo
echo "Tim file co chu orders:"
find data/olist -maxdepth 1 -type f -name "*orders*"

echo
echo "Doc header cua file orders bang head:"
head -n 1 data/olist/olist_orders_dataset.csv

echo
echo "Dem so dong cua tung CSV:"
wc -l data/olist/*.csv

echo
echo "Dung grep tim dong co trang thai delivered trong orders:"
grep -m 3 "delivered" data/olist/olist_orders_dataset.csv

echo
echo "Quan ly process: hien thi 5 process dau tien:"
ps aux | head -n 5

echo
echo "Thong tin quyen file script nay:"
ls -l exercises/week1/script/linux_basics.sh

echo
echo "Hoan thanh Linux basics."

