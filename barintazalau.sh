pkill -f ramforge || true
sleep 0.5
wait 2>/dev/null || true
sleep 0.5
sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
sleep 0.5
rm -r append.aof dump.rdb
