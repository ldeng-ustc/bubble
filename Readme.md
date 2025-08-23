# Bubble(TBD)

You may need to change system config in `env/base.h`

Before running some experiments, open kernel perf event to allow collecting required information.

```bash
sudo sh -c " echo 0 > /proc/sys/kernel/kptr_restrict"
sudo sh -c " echo -1 > /proc/sys/kernel/perf_event_paranoid"
# or, for permanent: 
sudo sysctl -w kernel.perf_event_paranoid=-1
sudo sysctl -w kernel.kptr_restrict=0
```

