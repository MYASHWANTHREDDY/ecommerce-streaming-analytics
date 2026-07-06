# Recording the dashboard demo GIF

This isn't automated — record it yourself once the pipeline is running.

## Setup

```bash
make up        # start Kafka, Postgres, Spark, Airflow
make produce   # start streaming order events (leave running)
make demo      # start the dashboard, in a third terminal
```

Open `http://localhost:8501` and let it run for ~30s so the Live tab has real data before you start recording.

## What to capture

20-30 seconds of the **Live** tab, showing:
- The metrics row (Orders / Revenue / Windows shown) changing between refreshes
- The data table and charts updating
- At least one full 5-second auto-refresh cycle, ideally two, so it's visibly live and not a static screenshot

## Tools

Any screen recorder that can export (or be converted to) a GIF works:
- **Windows**: Xbox Game Bar (`Win+Alt+R`) records an MP4 — convert with `ffmpeg -i input.mp4 -vf "fps=10,scale=900:-1" docs/dashboard-demo.gif`, or use [ScreenToGif](https://www.screentogif.com/) to capture directly as a GIF.
- **Mac**: QuickTime screen recording + the same `ffmpeg` conversion, or [Gifski](https://gif.ski/).
- **Linux**: [Peek](https://github.com/phw/peek) records directly to GIF.

## Output

Save the final file as `docs/dashboard-demo.gif`, referenced from the README. Keep it reasonably small (a few MB, not tens) — trim resolution/duration with `ffmpeg` if needed rather than shipping a huge file in the repo.
