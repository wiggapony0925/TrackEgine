FROM debian:bookworm-slim AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY include ./include
COPY src ./src
COPY third_party ./third_party
COPY scripts ./scripts

RUN chmod +x ./scripts/build.sh && ./scripts/build.sh

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libsqlite3-0 \
        sqlite3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /src/build/bin/trackengine /usr/local/bin/trackengine
COPY scripts/prepare_schedule_db.sh /usr/local/bin/prepare_schedule_db.sh

RUN chmod +x /usr/local/bin/prepare_schedule_db.sh

ENV PORT=8080
ENV TRACK_ENGINE_SCHEDULE_DB=/app/data/transit_schedule.db
ENV TRACK_ENGINE_PREPARE_DB=1

EXPOSE 8080

CMD ["prepare_schedule_db.sh"]
