from unittest.mock import MagicMock

import pytest

from pipeline.extract import ExtractionError, fetch_products


def _mock_session(json_payload=None, status_error=None, side_effect=None):
    """Build a mock object with the same shape as `requests` / a Session."""
    session = MagicMock()
    response = MagicMock()
    if status_error:
        response.raise_for_status.side_effect = status_error
    else:
        response.raise_for_status.return_value = None
    response.json.return_value = json_payload
    if side_effect:
        session.get.side_effect = side_effect
    else:
        session.get.return_value = response
    return session


class TestFetchProductsSuccess:
    def test_returns_records_on_first_try(self):
        session = _mock_session(json_payload=[{"id": 1}, {"id": 2}])
        records = fetch_products(session, retries=3)
        assert records == [{"id": 1}, {"id": 2}]
        assert session.get.call_count == 1

    def test_passes_the_configured_url_and_timeout(self):
        session = _mock_session(json_payload=[])
        fetch_products(session, api_url="https://example.test/products", timeout_s=3)
        session.get.assert_called_once_with("https://example.test/products", timeout=3)


class TestFetchProductsRetries:
    def test_succeeds_after_transient_failures(self):
        session = MagicMock()
        good_response = MagicMock()
        good_response.raise_for_status.return_value = None
        good_response.json.return_value = [{"id": 1}]
        session.get.side_effect = [ConnectionError("down"), ConnectionError("down"), good_response]

        records = fetch_products(session, retries=3, sleep=lambda s: None)

        assert records == [{"id": 1}]
        assert session.get.call_count == 3

    def test_sleeps_with_exponential_backoff_between_attempts(self):
        session = MagicMock()
        session.get.side_effect = [ConnectionError("down"), ConnectionError("down")]
        sleep_calls = []

        fallback = [{"id": "fb"}]
        fetch_products(session, retries=2, backoff_base_s=1.0, sleep=sleep_calls.append, fallback=fallback)

        assert sleep_calls == [1.0]  # backoff_base_s * 2**(attempt-1) for attempt 1 only (retries=2 -> 1 sleep)


class TestFetchProductsFailureHandling:
    def test_raises_extraction_error_when_no_fallback_given(self):
        session = _mock_session(side_effect=ConnectionError("network is down"))
        with pytest.raises(ExtractionError):
            fetch_products(session, retries=2, sleep=lambda s: None)

    def test_falls_back_to_sample_data_when_provided(self):
        session = _mock_session(side_effect=ConnectionError("network is down"))
        fallback = [{"id": 99, "title": "offline sample"}]

        records = fetch_products(session, retries=2, sleep=lambda s: None, fallback=fallback)

        assert records == fallback

    def test_http_error_status_also_triggers_retry_and_fallback(self):
        session = _mock_session(status_error=Exception("500 Server Error"))
        records = fetch_products(session, retries=2, sleep=lambda s: None, fallback=[{"id": 1}])
        assert records == [{"id": 1}]
