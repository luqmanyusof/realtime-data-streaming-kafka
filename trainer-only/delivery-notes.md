# Trainer delivery notes — Kafka (Node.js)

Not for participants. Session: 10–11 Nov 2026, 09:00–17:00.

**The setup in one line:** 4 containers (Kafka, Kafka UI, Grafana, PostgreSQL), 8 Node.js files (4 produce, 4 consume), 2 npm packages (`kafkajs`, `pg`). Simplified on 6 Nov 2026 from the 3-broker / REST Proxy / Postgres / MQTT version (backup: `../realtime-data-streaming-kafka-backup-2026-11-06`).

## The week before

- [ ] **Re-check versions** (all tested 6 Nov 2026 with Node 24 and 26):
  - `apache/kafka:4.3.2` still the current 4.3 patch?
  - `ghcr.io/kafbat/kafka-ui:v1.5.0`, `grafana/grafana:13.2.4-slim`, `postgres:18.6-alpine`.
  - Grafana Kafka plugin `hamedkarbasi93-kafka-datasource@1.9.1` still downloadable from grafana.com.
  - `npm view kafkajs version` still 2.2.4; Node LTS still 26.
  - The SoftwareMill visualiser still loads: https://softwaremill.com/kafka-visualisation/
- [ ] **Send the pre-course steps** (README → Before the course) at least 5 working days ahead. Ask each participant to reply with a screenshot of `docker images` showing the four images.
- [ ] **Corporate laptop risks — ask the client contact directly:**
  - Can participants install Docker Desktop? It needs **local admin rights**.
  - Is **virtualisation / WSL 2** enabled? Docker Desktop on Windows fails without it, and fixing it needs a BIOS change.
  - RAM: the stack uses well under 1.5 GB inside Docker (Kafka ≤ 512 MB heap, Kafka UI ~350 MB, Grafana ~200 MB, PostgreSQL ~50 MB). 8 GB laptops are fine.
  - Does the office network block `docker.io`, `ghcr.io`, `registry.npmjs.org` or `grafana.com`? Grafana downloads its Kafka plugin from grafana.com on **first** start; after that it is kept in the `grafana-data` volume.
- [ ] **Bring the images on USB** in case downloads fail on the day:
  ```powershell
  docker save -o kafka-images.tar apache/kafka:4.3.2 ghcr.io/kafbat/kafka-ui:v1.5.0 grafana/grafana:13.2.4-slim postgres:18.6-alpine
  ```
  Participants load them with `docker load -i kafka-images.tar`. Also bring a zipped `node_modules` (KafkaJS and `pg`, pure JavaScript, works on any OS) in case npm is blocked.
  If grafana.com is blocked, the plugin cannot install: download the plugin zip in advance and unzip it into the container's `/var/lib/grafana/plugins` (or run Grafana from a laptop that already has it).
- [ ] Venue: no location is set on the session yet. Confirm Wi-Fi and power points for every seat.

## Day 1 timing — tools only, no code

| Time | Topic | Notes |
|---|---|---|
| 09:00–09:20 | 1 Why streaming | Stage the CSV question as a quiz. The 19½-hour answer is the first "aha". |
| 09:20–10:05 | 2 Core concepts + visualiser | Three setups, real situation first. Draw the partition log on the whiteboard; keep it up all day. |
| 10:05–10:15 | 3 IoT architecture | The map of both days. |
| 10:15–10:30 | Tea | Walk the room: who has 4 containers up? |
| 10:30–11:10 | 4 Docker + Kafka | Most setup problems show up here. |
| 11:10–12:10 | 5 CLI + Kafka UI | Four terminals from here on; check everyone has Kafka shells 1 and 2. |
| 12:10–12:30 | 6 Simulator | `npm install` is the second place setups fail (blocked npm). |
| 12:30–13:30 | Lunch | Simulator keeps running: good lag data for Topic 7, and an hour of history for Topic 11. |
| 13:30–14:20 | 7 Groups, lag, replay | The core lab of the day. Do not rush it. |
| 14:20–15:00 | 8 Retention + compaction | The retention loop takes 2 minutes; explain segments while it runs. |
| 15:00–15:15 | 9 Replication (concept) | Back to the visualiser on the projector. |
| 15:15–15:30 | Tea | |
| 15:30–15:55 | 10 Headers, timestamps, limits | The Headers editor auto-closes brackets: check for one `}`. |
| 15:55–16:20 | 11 Find any message | Stage the 16:00 phone call from the district office. |
| 16:20–16:40 | 12 Housekeeping | |
| 16:40–17:00 | 13 Scale out live | The "silent gap" (13.5) is the last "aha" of the day. |

Tested on 9 Nov 2026: the trainer finished the original Topics 1–9 in about 3 hours, so Topics 10–13 were added. Day 1 is now full.

**Cut list, in this order:** Topic 12 becomes a 5-minute demo; Topic 11.3 (smart filter) is skipped; Topic 8.2 (compaction) becomes a 5-minute demo; Topic 7 Try it 1 (reset in the Kafka UI) is skipped.

## Day 2 timing — code

| Time | Topic | Notes |
|---|---|---|
| 09:00–09:15 | Before you start | Recreate `telemetri-sungai` |
| 09:15–09:55 | 14 Producer | Read `kafka.js` together first |
| 09:55–10:40 | 15 Consumer + groups | The poison message (Try it 2–3) is the morning's "aha" |
| 10:40–10:55 | Tea | |
| 10:55–11:20 | 16 Keys + ordering | |
| 11:20–11:50 | 17 Delivery + offsets | Ends on the duplicate-SMS question; resolved in Topic 20 |
| 11:50–12:30 | 18 Simulator code | Read it together. Restart the flood after lunch, so the alerts in Topic 19 happen in front of them |
| 12:30–13:30 | Lunch | |
| 13:30–14:30 | 19 Alerts | Try it 2 (two alert.js, keyed state) is the key insight |
| 14:30–15:00 | 20 Notifier | "You added a service and touched nothing else" |
| 15:00–15:30 | 21 Database | Replay with 0 duplicates, then stop the database: no gap |
| 15:30–15:40 | Tea | |
| 15:40–16:10 | 22 Grafana | Project the dashboard for the rest of the day |
| 16:10–16:25 | 23 Reliability, monitoring, security | |
| 16:25–17:00 | 24 Final project + presentations | 10 min break-it, 25 min presentations |

**Cut list, in this order:** Topic 23.4 (security) becomes a 5-minute talk; Topic 22.2 (alerts table) is imported instead of built; Topic 21 Try it 3 (hourly SQL) is skipped; Topic 24 presentations go to 3 minutes per group.

Do not cut Topic 15 Try it 2–3 (poison message) or Topic 19. They are the course's "why".

## Moments to stage

- **Topic 1:** the nightly report arrives 19½ hours after the river reached Bahaya.
- **Topic 4:** stop the Kafka UI: Kafka does not care. The UI is only a window.
- **Topic 7:** lag grows while nobody reads; replay the last 5 minutes.
- **Topic 13:** add partitions while the group runs: three stations go unread and the lag does not show it.
- **Day 2, Topic 15:** one `bukan json` message kills the consumer, and kills it again on every restart.
- **Day 2, Topic 19:** two `alert.js` processes, and each station's status is still right, because of keys.
- **Day 2, Topic 20:** a new service added with zero changes to the others. Then ask: "what does a replay do to the officer's phone?"
- **Day 2, Topic 21:** replay the whole topic into the database: thousands `already there`, zero duplicates. Then stop the database: the writer waits, and no reading is missing.

## Where participants get stuck

| Symptom | Cause | Fix |
|---|---|---|
| `open //./pipe/dockerDesktopLinuxEngine` | Docker Desktop not running | Start it, wait for the whale to settle |
| `Bind for 0.0.0.0:8085 failed` (or 3000, 5433, 9092) | Port in use | Change the left port in `docker-compose.yml` (for 5433 also in `db-writer.js`) |
| `docker exec` path like `C:/Program Files/Git/opt/kafka/...` | Git Bash path conversion | Use PowerShell |
| Containers `Exited (255)` | Docker Desktop restarted (sleep, update) | `docker compose up -d`; `restart: unless-stopped` normally handles it |
| `--timeout-ms` consumer ends with `ERROR … TimeoutException` | Normal: that is how it reports the timeout | Ignore |
| Consumer prints nothing for ~30 s | A crashed member is still in the group | Wait; next time stop with Ctrl+C |
| Red KafkaJS `ERROR` lines about rebalancing / group coordinator | Normal KafkaJS logging | Ignore unless no data after 30 s |
| `TimeoutNegativeWarning` | KafkaJS on Node 24+ | Harmless; `node --no-warnings` hides it |
| Every station appears twice | Two simulators running (two terminals) | Stop one |
| Day 2 consumer prints for minutes | `telemetri-sungai` not recreated in "Before you start" | Ctrl+C, recreate the topic |
| Grafana: no `kafka` data source, or plugin missing | Plugin download blocked on first start | `docker compose logs grafana`, look for `kafka-datasource`; see the USB note above |
| Grafana chart: one zig-zag line | Transformations missing | Filter fields by name `time\|stesen\|paras_air`, then Partition by values `stesen` |
| Grafana alerts table empty after reload | Offset set to `Earliest` | Use `Last N messages` |
| `db-writer.js` crashes with `Unhandled 'error' event` on a database restart | `database.on('error')` line missing | Add it back (Topic 21) |
| `Cannot find package 'pg'` | `package.json` copied before 6 Nov version, or `npm install` not run | `npm install pg@8.23.1` |
