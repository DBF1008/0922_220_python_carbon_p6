from mock import Mock, patch
from unittest import TestCase

from carbon import events, state
from carbon.pipeline import Processor, run_pipeline, run_pipeline_generated
from carbon.service import (
  CarbonRootService, setupPipeline, setupFlowControl,
  resumeReceivingMetricsUnlessShuttingDown,
)
from carbon.tests.util import TestSettings

import sys
from types import ModuleType


class TestSetupPipeline(TestCase):
  def setUp(self):
    self.settings = TestSettings()
    self.root_service_mock = Mock(CarbonRootService)
    self.call_when_running_patch = patch('twisted.internet.reactor.callWhenRunning')
    self.call_when_running_mock = self.call_when_running_patch.start()

  def tearDown(self):
    self.call_when_running_patch.stop()
    state.pipeline_processors = []
    events.metricReceived.handlers = []
    events.metricGenerated.handlers = []

  def test_run_pipeline_chained_to_metric_received(self):
    setupPipeline([], self.root_service_mock, self.settings)
    self.assertTrue(run_pipeline in events.metricReceived.handlers)

  def test_run_pipeline_chained_to_metric_generated(self):
    setupPipeline([], self.root_service_mock, self.settings)
    self.assertTrue(run_pipeline_generated in events.metricGenerated.handlers)

  @patch('carbon.service.setupAggregatorProcessor')
  def test_aggregate_processor_set_up(self, setup_mock):
    setupPipeline(['aggregate'], self.root_service_mock, self.settings)
    setup_mock.assert_called_once_with(self.root_service_mock, self.settings)

  @patch('carbon.service.setupRewriterProcessor')
  def test_rewrite_processor_set_up(self, setup_mock):
    setupPipeline(['rewrite:pre'], self.root_service_mock, self.settings)
    setup_mock.assert_called_once_with(self.root_service_mock, self.settings)

  @patch('carbon.service.setupRelayProcessor')
  def test_relay_processor_set_up(self, setup_mock):
    setupPipeline(['relay'], self.root_service_mock, self.settings)
    setup_mock.assert_called_once_with(self.root_service_mock, self.settings)

  @patch('carbon.service.setupWriterProcessor')
  def test_write_processor_set_up(self, setup_mock):
    setupPipeline(['write'], self.root_service_mock, self.settings)
    setup_mock.assert_called_once_with(self.root_service_mock, self.settings)

  def test_unknown_processor_raises_value_error(self):
    self.assertRaises(
        ValueError, setupPipeline, ['foo'], self.root_service_mock, self.settings)

  @patch('carbon.service.setupRewriterProcessor', new=Mock())
  def test_parses_processor_args(self):
    # XXX Patch doesnt work on this import directly
    rewrite_mock = Mock()
    Processor.plugins['rewrite'] = rewrite_mock
    setupPipeline(['rewrite:pre'], self.root_service_mock, self.settings)
    rewrite_mock.assert_called_once_with('pre')

  def test_schedules_pipeline_ready(self):
    setupPipeline([], self.root_service_mock, self.settings)
    self.assertTrue(self.call_when_running_mock.called)


class TestFlowControl(TestCase):
  """Flow-control wiring must keep backpressure one-way during shutdown."""

  def setUp(self):
    self.settings = TestSettings()
    self.root_service_mock = Mock(CarbonRootService)
    self._cache_full_handlers = list(events.cacheFull.handlers)
    self._cache_space_handlers = list(events.cacheSpaceAvailable.handlers)
    self._resume_handlers = list(events.resumeReceivingMetrics.handlers)
    self._orig_shutting_down = state.shuttingDown

  def tearDown(self):
    events.cacheFull.handlers = self._cache_full_handlers
    events.cacheSpaceAvailable.handlers = self._cache_space_handlers
    events.resumeReceivingMetrics.handlers = self._resume_handlers
    state.shuttingDown = self._orig_shutting_down

  def test_setup_flow_control_binds_pause_and_guarded_resume(self):
    setupFlowControl()
    self.assertIn(events.pauseReceivingMetrics, events.cacheFull.handlers)
    self.assertIn(
        resumeReceivingMetricsUnlessShuttingDown,
        events.cacheSpaceAvailable.handlers)

  def test_cache_space_available_resumes_when_running(self):
    setupFlowControl()
    state.shuttingDown = False
    spy = Mock()
    events.resumeReceivingMetrics.addHandler(spy)
    events.cacheSpaceAvailable()
    spy.assert_called_once_with()

  def test_cache_space_available_suppressed_during_shutdown(self):
    """Draining below the watermark must not re-open intake mid-shutdown."""
    setupFlowControl()
    state.shuttingDown = True
    spy = Mock()
    events.resumeReceivingMetrics.addHandler(spy)
    events.cacheSpaceAvailable()
    spy.assert_not_called()

  def test_cache_full_pauses_even_during_shutdown(self):
    setupFlowControl()
    state.shuttingDown = True
    spy = Mock()
    events.pauseReceivingMetrics.addHandler(spy)
    try:
      events.cacheFull()
    finally:
      events.pauseReceivingMetrics.removeHandler(spy)
    spy.assert_called_once_with()

  @patch('carbon.service.setupFlowControl')
  @patch('carbon.rewrite.RewriteRuleManager.read_from')
  def test_rewrite_processor_uses_setup_flow_control(self, read_from, fc_mock):
    from carbon.service import setupRewriterProcessor
    self.settings['rewrite-rules'] = 'rewrite-rules.conf'
    setupRewriterProcessor(self.root_service_mock, self.settings)
    fc_mock.assert_called_once_with()

  @patch('carbon.service.setupFlowControl')
  @patch('carbon.service.exists', return_value=True)
  def test_aggregator_processor_uses_setup_flow_control(self, exists_mock, fc_mock):
    from carbon.service import setupAggregatorProcessor
    self.settings['aggregation-rules'] = 'aggregation-rules.conf'
    # Stub out the aggregator modules: they pull in optional dependencies
    # (cachetools) that are irrelevant to the flow-control wiring.
    fake_processor = ModuleType('carbon.aggregator.processor')
    fake_processor.AggregationProcessor = Mock()
    fake_rules = ModuleType('carbon.aggregator.rules')
    fake_rules.RuleManager = Mock()
    with patch.dict(sys.modules, {
        'carbon.aggregator.processor': fake_processor,
        'carbon.aggregator.rules': fake_rules}):
      setupAggregatorProcessor(self.root_service_mock, self.settings)
      fc_mock.assert_called_once_with()

  @patch('carbon.service.setupFlowControl')
  def test_relay_processor_uses_setup_flow_control(self, fc_mock):
    from carbon.service import setupRelayProcessor
    self.settings.RELAY_METHOD = 'consistent-hashing'
    saved_client_manager = state.client_manager
    resume_handlers = list(events.resumeReceivingMetrics.handlers)
    try:
      setupRelayProcessor(self.root_service_mock, self.settings)
      fc_mock.assert_called_once_with()
    finally:
      state.client_manager = saved_client_manager
      events.resumeReceivingMetrics.handlers = resume_handlers

  @patch('carbon.service.setupFlowControl')
  @patch('carbon.service.TCPServer')
  def test_writer_processor_uses_setup_flow_control(self, tcp_server_mock, fc_mock):
    from carbon.service import setupWriterProcessor
    # Stub out carbon.writer: it computes storage schema paths from global
    # settings at import time, which is irrelevant to flow-control wiring.
    fake_writer = ModuleType('carbon.writer')
    fake_writer.WriterService = Mock()
    with patch.dict(sys.modules, {'carbon.writer': fake_writer}):
      setupWriterProcessor(self.root_service_mock, self.settings)
      fc_mock.assert_called_once_with()
