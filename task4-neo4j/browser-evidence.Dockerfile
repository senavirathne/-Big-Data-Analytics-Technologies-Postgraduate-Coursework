FROM alpine:3.22.1

RUN apk add --no-cache ca-certificates chromium curl

COPY --chmod=0555 scripts/capture_browser_evidence.sh /usr/local/bin/capture-browser-evidence

ENTRYPOINT ["/usr/local/bin/capture-browser-evidence"]
