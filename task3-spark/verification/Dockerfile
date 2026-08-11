FROM mcr.microsoft.com/playwright/python:v1.55.0-noble

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN python3 -m pip install --no-cache-dir "playwright==1.55.0"

WORKDIR /opt/coursework/verification

COPY .github/scripts/validate_task3_outputs.py ./validate_task3_outputs.py
COPY .github/scripts/task3_capture_spark_ui.py ./task3_capture_spark_ui.py
COPY task3-spark/verification/write_evidence_manifest.py ./write_evidence_manifest.py
COPY task3-spark/verification/verify-evidence.sh ./verify-evidence.sh

RUN python3 -m py_compile \
      ./validate_task3_outputs.py \
      ./task3_capture_spark_ui.py \
      ./write_evidence_manifest.py \
    && bash -n ./verify-evidence.sh \
    && chmod 0755 ./verify-evidence.sh

ENTRYPOINT ["/opt/coursework/verification/verify-evidence.sh"]
