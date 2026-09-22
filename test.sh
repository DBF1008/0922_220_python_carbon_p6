#!/bin/sh
# Run all carbon unit tests (manual regression script).
#
# Usage:
#   ./test.sh           # run the full unit test suite
#   ./test.sh -k        # run only the shutdown-drain regression tests
#
# Requirements: python with twisted + mock installed
#   (see requirements.txt and tests-requirements.txt).

set -e
cd "$(dirname "$0")"

export PYTHONPATH="lib${PYTHONPATH:+:$PYTHONPATH}"

if [ "$1" = "-k" ]; then
  # Focused regression tests for the shutdown drain lifecycle.
  exec python -m twisted.trial \
    carbon.tests.test_client.CarbonClientFactoryDisconnectTest \
    carbon.tests.test_client.CarbonClientShutdownDrainTest \
    carbon.tests.test_service.TestSetupFlowControl \
    carbon.tests.test_protocols.TestMetricReceiverShutdownGuard
fi

# Full unit test suite.
exec python -m twisted.trial carbon
