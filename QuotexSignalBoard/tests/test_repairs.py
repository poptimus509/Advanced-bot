import importlib
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import config as cfg
import strategy
from indicators import calculate_adx, calculate_rsi
from data.candle_manager import CandleManager
from core.events import EventDispatcher


def trend_history(bearish=False, wick=0.04):
    x = np.arange(60)
    close = 100 + .08*x + .6*np.sin(x/3)
    opening = np.r_[close[0]-.1, close[:-1]]
    df = pd.DataFrame(dict(time=1800000000+x*60, open=opening, close=close,
                           high=np.maximum(opening, close)+wick, low=np.minimum(opening, close)-wick))
    if bearish:
        df[['open', 'close']] = 250-df[['open', 'close']]
        high, low = 250-df['low'], 250-df['high']
        df['high'], df['low'] = high, low
    return df


class IndicatorTests(unittest.TestCase):
    def test_rsi_extremes_and_flat(self):
        self.assertEqual(calculate_rsi(pd.Series(range(60))).iloc[-1], 100)
        self.assertEqual(calculate_rsi(pd.Series(range(60, 0, -1))).iloc[-1], 0)
        self.assertEqual(calculate_rsi(pd.Series([100.]*60)).iloc[-1], 50)

    def test_adx_symmetric_and_warmup_missing(self):
        for closes in (np.arange(100.,160.), np.arange(160.,100.,-1)):
            df = pd.DataFrame(dict(close=closes, high=closes+.5, low=closes-.5))
            self.assertAlmostEqual(calculate_adx(df).iloc[-1], 100)
            self.assertTrue(calculate_adx(df.iloc[:20]).iloc[-1:].isna().all())


class EntryTests(unittest.TestCase):
    def test_strong_call_and_put_without_5m(self):
        for bearish, expected, structure in [(False,'CALL','HH_HL'),(True,'PUT','LH_LL')]:
            result = strategy.evaluate_strategy(trend_history(bearish))
            self.assertEqual(result[:2], (expected,8))
            self.assertEqual(result[3]['structure'], structure)
            self.assertTrue(result[3]['m5_override'])

    def test_override_can_be_disabled(self):
        with patch.object(cfg,'ALLOW_STRONG_1M_OVERRIDE',False):
            self.assertEqual(strategy.evaluate_strategy(trend_history())[0],'NO_TRADE')

    def test_weak_pressure_cannot_override_missing_5m(self):
        self.assertEqual(strategy.evaluate_strategy(trend_history(wick=.13))[0], 'NO_TRADE')

    def test_strong_pressure_can_override_opposing_5m(self):
        htf = trend_history(True)
        htf['time'] = 1800000000 + np.arange(60)*300
        self.assertEqual(strategy.evaluate_strategy(trend_history(),htf,{'adx_5m':30})[0], 'CALL')

    def test_aligned_5m_uses_normal_gate(self):
        htf = trend_history()
        htf['time'] = 1800000000 + np.arange(60)*300
        result = strategy.evaluate_strategy(trend_history(),htf,{'adx_5m':30})
        self.assertEqual(result[0], 'CALL')
        self.assertFalse(result[3]['m5_override'])

    def test_mixed_structure_never_trades(self):
        df=trend_history()
        df['open']=100; df['close']=100.1; df['high']=100.2; df['low']=99.9
        self.assertEqual(strategy.evaluate_strategy(df)[0], 'NO_TRADE')

    def test_near_resistance_blocks_call(self):
        df=trend_history(); close=float(df.iloc[-1]['close'])
        swings=[{'index':50,'price':close+.01,'type':'HIGH'}]
        self.assertFalse(strategy.support_resistance_context(df,swings,close,.3,True)[0])

    def test_near_support_blocks_put(self):
        df=trend_history(True); close=float(df.iloc[-1]['close'])
        swings=[{'index':50,'price':close-.01,'type':'LOW'}]
        self.assertFalse(strategy.support_resistance_context(df,swings,close,.3,False)[0])


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory()
        cls.saved_db=cfg.DB_PATH
        cfg.DB_PATH=str(Path(cls.temp.name)/'test.db')
        with patch.object(threading.Thread,'start'), patch.object(cfg,'PUSHER_ENABLED',False):
            cls.bot=importlib.import_module('bot')
        cls.bot._threads_started=True

    @classmethod
    def tearDownClass(cls):
        cfg.DB_PATH=cls.saved_db
        cls.temp.cleanup()

    def setUp(self):
        conn=self.bot.get_db_connection()
        conn.execute('DELETE FROM signal_history'); conn.execute('DELETE FROM dispatch_ledger_v2')
        conn.commit(); conn.close()
        self.candidate=dict(symbol='EURUSD',display_name='EUR/USD',direction='CALL',score=8,
                            quality='A_PLUS',details={'setup_id':'fixture'},analysis_close=100)

    def test_cooldown_expires_after_signal(self):
        self.bot.save_signal(self.candidate,1800000000,100)
        with patch.object(self.bot.deriv_client,'get_server_time',return_value=1800000300):
            self.assertTrue(self.bot.symbol_in_cooldown('EURUSD',10))
        with patch.object(self.bot.deriv_client,'get_server_time',return_value=1800000600):
            self.assertFalse(self.bot.symbol_in_cooldown('EURUSD',10))

    def test_dispatch_persists_before_send_and_deduplicates(self):
        def sent(*args):
            self.assertEqual(self.bot.signal_rows()[0]['delivery_status'],'ATTEMPTING')
            return 'SENT'
        with patch.object(self.bot,'live_quote',return_value={'price':100}), \
             patch.object(self.bot.deriv_client,'get_server_time',return_value=1800000001), \
             patch.object(self.bot,'send_telegram_alert',side_effect=sent) as sender:
            self.assertEqual(self.bot.dispatch_best_signal(self.candidate,1800000000),'SENT')
            self.assertEqual(self.bot.dispatch_best_signal(self.candidate,1800000000),'DUPLICATE')
            self.assertEqual(sender.call_count,1)
        self.assertEqual(self.bot.signal_rows()[0]['expiry_epoch'],1800000060)

    def test_stale_quote_cannot_send(self):
        with patch.object(self.bot,'live_quote',return_value=None), patch.object(self.bot,'send_telegram_alert') as sender:
            self.assertEqual(self.bot.dispatch_best_signal(self.candidate,1800000000),'STALE_QUOTE')
            sender.assert_not_called()

    def test_database_failure_cannot_send(self):
        with patch.object(self.bot,'live_quote',return_value={'price':100}), \
             patch.object(self.bot.deriv_client,'get_server_time',return_value=1800000001), \
             patch.object(self.bot,'save_signal',side_effect=sqlite3.OperationalError('test')), \
             patch.object(self.bot,'send_telegram_alert') as sender:
            self.assertEqual(self.bot.dispatch_best_signal(self.candidate,1800000000),'STORE_FAILED')
            sender.assert_not_called()

    def test_late_candidate_cannot_send(self):
        with patch.object(self.bot,'live_quote',return_value={'price':100}), \
             patch.object(self.bot.deriv_client,'get_server_time',return_value=1800000020), \
             patch.object(self.bot,'send_telegram_alert') as sender:
            self.assertEqual(self.bot.dispatch_best_signal(self.candidate,1800000000),'ENTRY_WINDOW_EXPIRED')
            sender.assert_not_called()

    def test_outcomes_use_each_saved_expiry(self):
        self.bot.save_signal(self.candidate,1800000000,100)
        self.bot.save_signal(self.candidate,1800000060,100)
        conn=self.bot.get_db_connection()
        conn.execute("UPDATE signal_history SET timeframe='5M', expiry_seconds=300 WHERE candle_epoch=1800000060")
        conn.commit(); conn.close()
        candles=pd.DataFrame({'time':[1800000000,1800000300], 'close':[101,99]})
        with patch.object(self.bot.time,'sleep',side_effect=[None,StopIteration]), \
             patch.object(self.bot.deriv_client,'get_server_time',return_value=1800000400), \
             patch.object(self.bot,'history_snapshot',return_value=candles):
            with self.assertRaises(StopIteration): self.bot.run_outcome_worker()
        rows=self.bot.signal_rows()
        self.assertEqual([r['result'] for r in rows],['LOSS','WIN'])

    def test_history_and_dashboard_routes(self):
        self.bot.save_signal(self.candidate,1800000000,100)
        client=self.bot.app.test_client()
        for url in ['/api/history','/api/dashboard','/api/performance','/api/evaluations']:
            self.assertEqual(client.get(url).status_code,200,url)
        self.assertEqual(len(client.get('/api/history').json),1)

    def test_old_losses_do_not_permanently_disable_pair(self):
        conn=self.bot.get_db_connection()
        for i in range(30):
            self.bot.save_signal(self.candidate,1700000000+i*60,100)
        conn.execute("UPDATE signal_history SET result='LOSS'"); conn.commit(); conn.close()
        with patch.object(self.bot.deriv_client,'get_server_time',return_value=1800000000):
            self.bot.refresh_auto_disabled_pairs()
        self.assertFalse(self.bot.is_auto_disabled('EURUSD'))


class CandleTests(unittest.TestCase):
    def test_incomplete_5m_is_not_published(self):
        manager=CandleManager('EURUSD',EventDispatcher())
        manager.process_tick(1800000240,100,0)
        manager.process_tick(1800000300,101,1)
        self.assertTrue(manager.get_closed_history('5M').empty)

    def test_old_tick_does_not_modify_forming_candle(self):
        manager=CandleManager('EURUSD',EventDispatcher())
        manager.process_tick(1800000300,100,0)
        manager.process_tick(1800000000,1,1)
        self.assertEqual(manager.get_current_forming_candle()['close'],100)


if __name__=='__main__': unittest.main()
