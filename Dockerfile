FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=UTC \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Python 3.13 via deadsnakes PPA (Ubuntu 22.04 ships 3.10 by default)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        software-properties-common \
        tzdata \
        curl \
        gcc \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        python3.13 \
        python3.13-dev \
        python3.13-venv \
    && curl -sS https://bootstrap.pypa.io/get-pip.py | python3.13 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.13 1 \
    && update-alternatives --install /usr/bin/python  python  /usr/bin/python3.13 1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

VOLUME ["/data"]

CMD ["python", "main.py"]
