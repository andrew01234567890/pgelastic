# Memory over time for the benchmark stack

```bash
docker compose -f test/bench/observe/compose.yml up -d
```

Grafana on <http://localhost:13000>, dashboard **pgebench: proxy memory**. Anonymous access is on
and there is no persistence: this is a rig for looking at a sweep while it runs, not a
deployment.

Bring it up before `make bench-arms` and watch the working set move. The shape that matters is
whether memory returns to its floor between repetitions or steps up and stays there — two numbers
in a report cannot show that and a line can.

## Why this stack and not another

**Not Pyroscope.** Its Rust SDK profiles CPU only, through `pprof-rs`. It has no heap integration,
so it cannot answer where per-connection bytes go. Heap attribution here was done out of band by
ablation — change one allocation, rebuild, measure the `anon` delta — and is written up in
[docs/bench.md](../../../docs/bench.md).

**Not Tempo.** Tempo stores traces. A trace says how long a request took and what it called, not
what is resident.

**The proxy's own gauge, not cAdvisor.** `pgelastic_proxy_resident_bytes` is `memory.current` less
`inactive_file`, read by the proxy from its own cgroup — the same definition the benchmark probe
and `docker stats` use, so a dashboard and a report cannot quietly disagree. cAdvisor is scraped
too, but under Docker Desktop it cannot reach the Docker daemon and so cannot label a series with
a container name; its panel is a cross-check keyed on cgroup id rather than the primary source.

Every container here is pinned to cores 14-15, outside the sets the benchmark uses for the load
generator, the pooler and PostgreSQL. A sampler that competes with the thing it samples is
measuring itself.
