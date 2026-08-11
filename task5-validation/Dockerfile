FROM python:3.12.13-slim-bookworm

WORKDIR /coursework

COPY .github/scripts/validate_task5_literature.py /opt/coursework/validate_task5_literature.py
COPY .github/scripts/write_evidence_manifest.py /opt/coursework/write_evidence_manifest.py
COPY task5-literature-review.md /coursework/task5-literature-review.md
COPY task5-validation/Dockerfile /opt/coursework/source/task5-validation/Dockerfile
COPY task5-validation/docker-compose.yml /opt/coursework/source/task5-validation/docker-compose.yml
COPY task5-validation/run-validation.sh /opt/coursework/run-validation.sh
COPY task5-validation/tests /opt/coursework/tests

RUN chmod 0755 /opt/coursework/run-validation.sh

ENTRYPOINT ["/opt/coursework/run-validation.sh"]
