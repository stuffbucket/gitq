# Linux test environment. Everything here is stdlib Python; the only real
# dependency is a git new enough for reftable, which is what the storage
# design rests on. Pinned by digest-free tag so dependabot can bump it.
FROM python:3.14-alpine

RUN apk add --no-cache git

WORKDIR /gitq
COPY . .

# Fail at build time, not halfway through a suite, if the base image ever
# drifts to a git without reftable support (it landed in 2.45).
RUN git --version \
 && git init --bare --ref-format=reftable /tmp/probe.git >/dev/null \
 && [ "$(git --git-dir=/tmp/probe.git rev-parse --show-ref-format)" = reftable ] \
 && rm -rf /tmp/probe.git

CMD ["python3", "tools/run_tests.py"]
