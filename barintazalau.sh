pkill -f ramforge || true
sleep 0.5
wait 2>/dev/null || true
sleep 0.5
sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
sleep 0.5
rm -r append.aof dump.rdb burn.0.0 dump_*_*.rdb append.aof_* zp_dump.rdb *.tmp.0 zp_dump_*_*.rdb append.aof_* test_chaos.rdb zp_dump_*.rdb *.sealed
sudo rm -f /dev/shm/ramforge_shared_*
