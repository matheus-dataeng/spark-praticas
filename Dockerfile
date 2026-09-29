FROM python:3.11-slim-bookworm

WORKDIR /workspace

RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    bash \
    && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH


COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    bash \
    git \
    && rm -rf /var/lib/apt/lists/*

CMD ["bash"]