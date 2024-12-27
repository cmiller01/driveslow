FROM python:3.12-slim

WORKDIR /app

# Install poetry
RUN pip install poetry && \
    poetry config virtualenvs.create false

# Copy dependency files
COPY pyproject.toml poetry.lock* ./

# Install dependencies
RUN poetry install --no-dev --no-interaction --no-ansi

# Copy application code
COPY driveslow/. driveslow/.

VOLUME ["/output"]

CMD ["/usr/local/bin/python", "./driveslow/fetcher.py"]