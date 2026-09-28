./perf-run.sh before                      # capacity snapshot, creates the run folder

# --- start JMeter ---

# terminal 1
./perf-run.sh during -d 3600              # infra + app collectors, stops itself

# terminal 2  (at the same time)
./perf-all.sh 10 w1                       # live screen; Ctrl-C when the load ends

# --- stop JMeter ---

./perf-run.sh after                       # capacity snapshot
./perf-run.sh report                      # -> PERF_Report_<ts>.html
python3 net_report.py --tag w1 -o NET_w1.html
