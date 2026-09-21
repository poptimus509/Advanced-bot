import json
import threading
import unittest
from unittest.mock import Mock, patch

from data.deriv_client import DerivClient


class DerivClientTests(unittest.TestCase):
    def test_public_connection_uses_single_origin_and_verified_tls(self):
        client = DerivClient()
        with patch('data.deriv_client.threading.Thread') as thread, \
                patch('data.deriv_client.websocket.WebSocketApp') as app:
            client.connect()
            app.return_value.run_forever.side_effect = lambda **kw: setattr(client, 'is_running', False)
            thread.call_args.kwargs['target']()
            self.assertEqual(app.call_args.args[0],
                             'wss://api.derivws.com/trading/v1/options/ws/public')
            self.assertNotIn('header', app.call_args.kwargs)
            options = app.return_value.run_forever.call_args.kwargs
            self.assertEqual(options['origin'], 'https://deriv.com')
            self.assertNotIn('sslopt', options)

    def test_public_connection_does_not_send_legacy_token(self):
        client = DerivClient()
        ws = Mock()
        with patch('data.deriv_client.cfg.API_TOKEN', 'test-secret'), \
                patch('data.deriv_client.cfg.FOREX_PAIRS', {'EURUSD': 'EUR/USD'}), \
                patch('data.deriv_client.cfg.ACTIVE_SYMBOLS', []), \
                patch('data.deriv_client.time.sleep'):
            client.on_open(ws)
        self.assertTrue(client.is_connected)
        self.assertEqual([json.loads(call.args[0]) for call in ws.send.call_args_list],
                         [{'ticks': 'frxEURUSD', 'subscribe': 1}])

    def test_api_error_wakes_request_even_with_original_message_type(self):
        client = DerivClient()
        event = threading.Event()
        client._pending_requests[7] = event
        with self.assertLogs('QuotexSignalBoard', level='WARNING') as logs:
            client.on_message(None, json.dumps({
                'msg_type': 'candles', 'req_id': 7,
                'error': {'code': 'InputValidationFailed', 'message': 'Invalid symbol'},
            }))
        self.assertTrue(event.is_set())
        self.assertEqual(client._request_results[7], [])
        self.assertIn('InputValidationFailed', logs.output[0])

    def test_candles_are_normalized_for_existing_consumers(self):
        client = DerivClient()
        client._pending_requests[8] = threading.Event()
        client.on_message(None, json.dumps({'msg_type': 'candles', 'req_id': 8,
            'candles': [{'epoch': 123, 'open': '1.1', 'high': '1.2', 'low': '1.0', 'close': '1.15'}]}))
        self.assertTrue(client._pending_requests[8].is_set())
        self.assertEqual(client._request_results[8], [{'epoch': 123, 'time': 123,
            'open': 1.1, 'high': 1.2, 'low': 1.0, 'close': 1.15, 'ticks_count': 1}])

    def test_invalid_tick_symbol_is_reported(self):
        client = DerivClient()
        ws = Mock()
        error = {'msg_type': 'tick', 'error': {'code': 'InvalidSymbol', 'message': 'Invalid symbol'},
                 'echo_req': {'ticks': 'frxCADJPY', 'subscribe': 1}}
        with self.assertLogs('QuotexSignalBoard', level='WARNING'):
            client.on_message(ws, json.dumps(error))
        ws.send.assert_not_called()


if __name__ == '__main__':
    unittest.main()
