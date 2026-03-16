# Scripts Guide

This directory contains helper scripts for chaos, benchmarks, and load generation.

## Main Scripts

- `chaos-ha.sh`: Docker-based HA partition/heal helper.
- `chaos-ha-local.sh`: local iptables-based partition/heal helper (used by Jepsen local runs).
- `run-chaos-suite.sh`: orchestrated chaos test run with artifact capture.
- `render-chaos-report.sh`: render summary report from collected chaos artifacts.
- `run_broker_bench_sealed.sh`: broker sealed-path benchmark wrapper.
- `bench_sealed.sh`: low-level sealed benchmark runner.

The `broker/` subdirectory contains broker load scripts and wrk/lua payload helpers.

## Quick Commands

### Local chaos (for Jepsen local mode)

```bash
sudo -v
bash ./scripts/chaos-ha-local.sh check
bash ./scripts/chaos-ha-local.sh status
sudo -n bash ./scripts/chaos-ha-local.sh isolate-leader
sudo -n bash ./scripts/chaos-ha-local.sh heal
```

### Docker chaos

```bash
bash ./scripts/chaos-ha.sh status
bash ./scripts/chaos-ha.sh leader-minority
bash ./scripts/chaos-ha.sh heal
```

### Chaos suite with artifact output

```bash
BUILD_ON_RUN=1 bash ./scripts/run-chaos-suite.sh
```

## Notes

- Always run `heal` after interrupted tests.
- Local chaos requires `sudo -n` and a compatible iptables match mode.
- If `check` fails, fix privileges before launching long Jepsen runs.