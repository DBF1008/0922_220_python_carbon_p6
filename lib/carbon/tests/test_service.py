from mock import Mock, patch
from unittest import TestCase

from carbon import events, state
from carbon.pipeline import Processor, run_pipeline, run_pipeline_generated
from carbon.service import CarbonRootService, setupFlowControl, setupPipeline
from carbon.tests.util import TestSettings


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


class TestSetupFlowControl(TestCase):
  """Flow control must be one-directional during shutdown: cacheFull may
  always pause receiving, but cacheSpaceAvailable must never resume it
  once the service has started draining."""

  def setUp(self):
    self._orig_shutting_down = state.shuttingDown
    self._orig_receivers_paused = state.metricReceiversPaused
    self._cache_full_handlers = list(events.cacheFull.handlers)
    self._space_available_handlers = list(events.cacheSpaceAvailable.handlers)

  def tearDown(self):
    state.shuttingDown = self._orig_shutting_down
    state.metricReceiversPaused = self._orig_receivers_paused
    events.cacheFull.handlers = self._cache_full_handlers
    events.cacheSpaceAvailable.handlers = self._space_available_handlers

  def test_cache_full_pauses_receiving(self):
    setupFlowControl()
    self.assertTrue(events.pauseReceivingMetrics in events.cacheFull.handlers)

  def test_cache_full_pauses_receiving_even_during_shutdown(self):
    setupFlowControl()
    state.shuttingDown = True
    state.metricReceiversPaused = False
    events.cacheFull()
    self.assertTrue(state.metricReceiversPaused)

  def test_cache_space_available_resumes_receiving_when_running(self):
    setupFlowControl()
    state.shuttingDown = False
    with patch.object(events, 'resumeReceivingMetrics') as resume_mock:
      events.cacheSpaceAvailable()
      resume_mock.assert_called_once_with()

  def test_cache_space_available_suppressed_during_shutdown(self):
    setupFlowControl()
    state.shuttingDown = True
    with patch.object(events, 'resumeReceivingMetrics') as resume_mock:
      events.cacheSpaceAvailable()
      resume_mock.assert_not_called()
