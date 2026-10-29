FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY pyproject.toml README.md ./
COPY src ./src
COPY contracts ./contracts
RUN pip install --no-deps . && useradd --uid 10001 --create-home aep && mkdir /artifacts && chown aep:aep /artifacts
USER aep
ENV ARTIFACT_ROOT=/artifacts
CMD ["python", "-m", "aep", "api", "--host", "0.0.0.0"]
