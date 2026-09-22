#!/bin/sh
# Manual unit-test runner for the carbon shutdown-drain changes.
#
# Usage:
#   ./test.sh            run the shutdown-drain regression tests, then the
#                        full unit suite
#   ./test.sh focused    run only the shutdown-drain regression tests
#   ./test.sh all        run the full unit suite (trial carbon)
#
# Note: 6 pre-existing errors exist on a clean checkout (missing optional
# deps such as cachetools, and unset storage settings); they are unrelated
# to the shutdown-drain changes.

cd "$(dirname "$0")"

export GRAPHITE_NO_PREFIX=true
export PYTHONPATH="lib${PYTHONPATH:+:$PYTHONPATH}"

MODE="${1:-}"
STATUS=0

run() {
  "$@" || STATUS=1
}

run_focused() {
  echo "== Shutdown-drain regression tests =="
  run trial \
    carbon.tests.test_client.CarbonClientShutdownDrainTest \
    carbon.tests.test_client.CarbonClientFactoryDisconnectTest \
    carbon.tests.test_service.TestFlowControl \
    carbon.tests.test_protocols.TestMetricReceiverShutdownFlowControl
}

run_modules() {
  echo "== Directly affected modules =="
  run trial \
    carbon.tests.test_client \
    carbon.tests.test_service \
    carbon.tests.test_protocols
}

run_all() {
  echo "== Full unit suite (trial carbon) =="
  run trial carbon
}

case "$MODE" in
  focused)
    run_focused
    ;;
  all)
    run_all
    ;;
  *)
    run_focused
    run_modules
    run_all
    ;;
esac

exit "$STATUS"
