# driveslow

if you drive slow...can you ski fast?

## background

can we use data from a variety of sources, like traffic webcams, weather and seasonality to predict how long it'll take to get up to ski in Tahoe region and how crowded it'll be?

## developing

you'll need python 3.12+. This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

To get started:
```bash
# Install dependencies
uv sync

# Run Python with uv
uv run python driveslow/fetcher.py
```

