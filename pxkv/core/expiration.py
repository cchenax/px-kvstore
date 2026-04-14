#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import threading
import logging

class BackgroundExpirer(threading.Thread):
    def __init__(self, store, interval: float = 60.0):
        super().__init__(daemon=True)
        self.store = store
        self.interval = interval
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def run(self):
        logging.info("Background expirer started (interval=%.1fs)", self.interval)
        while not self._stop_event.is_set():
            time.sleep(self.interval)
            if self._stop_event.is_set():
                break
            try:
                self.store.purge_expired()
            except Exception as e:
                logging.error("Background expiration failed: %s", e)
