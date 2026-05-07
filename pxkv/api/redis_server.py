#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
import threading
import logging
import time
import json
import ssl
from queue import Empty
from typing import Any, List, Optional

from ..metrics.registry import registry
from ..config.settings import settings
from ..auth import ROLE_ADMIN, ROLE_READER, ROLE_WRITER, best_role_for_secret, role_satisfies
from ..notifications import notifier

def encode_simple_string(s: str) -> bytes:
    return f"+{s}\r\n".encode("utf-8")

def encode_error(s: str) -> bytes:
    return f"-{s}\r\n".encode("utf-8")

def encode_integer(i: int) -> bytes:
    return f":{i}\r\n".encode("utf-8")

def encode_bulk_string(s: Any) -> bytes:
    if s is None:
        return b"$-1\r\n"
    if not isinstance(s, bytes):
        s = str(s).encode("utf-8")
    return f"${len(s)}\r\n".encode("utf-8") + s + b"\r\n"

def encode_array(arr: List[Any]) -> bytes:
    if arr is None:
        return b"*-1\r\n"
    res = f"*{len(arr)}\r\n".encode("utf-8")
    for item in arr:
        if isinstance(item, int):
            res += encode_integer(item)
        elif item is None:
            res += encode_bulk_string(None)
        else:
            res += encode_bulk_string(item)
    return res

class RedisServer(threading.Thread):
    def __init__(self, store, host="0.0.0.0", port=6379, ssl_context: Optional[ssl.SSLContext] = None):
        super().__init__(daemon=True)
        self.store = store
        self.host = host
        self.port = port
        self.ssl_context = ssl_context
        self._stop_event = threading.Event()
        self.server_socket = None

    def stop(self):
        self._stop_event.set()
        if self.server_socket:
            self.server_socket.close()

    def run(self):
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(128)
            self.server_socket.settimeout(1.0)
            scheme = "rediss" if self.ssl_context else "redis"
            logging.info("Redis compatible server listening on %s://%s:%d", scheme, self.host, self.port)
        except Exception as e:
            logging.error("Failed to start Redis server: %s", e)
            return

        while not self._stop_event.is_set():
            try:
                conn, addr = self.server_socket.accept()
                if self.ssl_context is not None:
                    try:
                        conn = self.ssl_context.wrap_socket(conn, server_side=True)
                    except Exception as e:
                        logging.warning("Redis TLS handshake failed from %s: %s", addr, e)
                        try:
                            conn.close()
                        except Exception:
                            pass
                        continue
                client_thread = threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True)
                client_thread.start()
            except socket.timeout:
                continue
            except Exception as e:
                if not self._stop_event.is_set():
                    logging.error("Redis server accept error: %s", e)
                break

    def handle_client(self, conn, addr):
        logging.info("Redis client connected from %s", addr)
        conn.settimeout(0.2)
        f = conn.makefile("rb")
        role: Optional[str] = None
        sub_sid: Optional[int] = None
        sub_q = None
        subs: set[str] = set()
        try:
            while not self._stop_event.is_set():
                if sub_q is not None and subs:
                    while True:
                        try:
                            ev = sub_q.get_nowait()
                        except Empty:
                            break
                        payload = ev.to_json()
                        op = str(ev.op)
                        for ch in list(subs):
                            if ch == "pxkv:keyspace" or ch == f"pxkv:keyspace:{op}":
                                conn.sendall(encode_array(["message", ch, payload]))

                try:
                    line = f.readline()
                except socket.timeout:
                    continue
                if not line:
                    break
                
                if line[0:1] != b"*":
                    continue
                
                num_args = int(line[1:].strip())
                args = []
                for _ in range(num_args):
                    header = f.readline()
                    if header[0:1] != b"$":
                        break
                    arg_len = int(header[1:].strip())
                    arg = f.read(arg_len)
                    f.read(2)
                    args.append(arg)
                
                if not args:
                    continue

                cmd = args[0].decode("utf-8", errors="replace").upper()
                if cmd in ("SUBSCRIBE", "UNSUBSCRIBE"):
                    if self._auth_enabled():
                        if role is None:
                            conn.sendall(encode_error("NOAUTH Authentication required."))
                            continue
                        if not role_satisfies(role, ROLE_READER):
                            conn.sendall(encode_error("NOPERM this user has no permissions to run the command"))
                            continue
                    if cmd == "SUBSCRIBE":
                        if len(args) < 2:
                            conn.sendall(encode_error("ERR wrong number of arguments for 'SUBSCRIBE' command"))
                            continue
                        if sub_sid is None:
                            sub_sid, sub_q = notifier.subscribe()
                        for i in range(1, len(args)):
                            ch = args[i].decode("utf-8", errors="replace")
                            subs.add(ch)
                        resp = b""
                        for ch in subs:
                            resp += encode_array(["subscribe", ch, len(subs)])
                        conn.sendall(resp)
                        continue

                    if cmd == "UNSUBSCRIBE":
                        if len(args) == 1:
                            subs.clear()
                        else:
                            for i in range(1, len(args)):
                                ch = args[i].decode("utf-8", errors="replace")
                                subs.discard(ch)
                        resp = b""
                        if subs:
                            for ch in subs:
                                resp += encode_array(["unsubscribe", ch, len(subs)])
                        else:
                            resp += encode_array(["unsubscribe", None, 0])
                            if sub_sid is not None:
                                notifier.unsubscribe(sub_sid)
                            sub_sid = None
                            sub_q = None
                        conn.sendall(resp)
                        continue

                if subs:
                    if cmd in ("PING",):
                        conn.sendall(encode_array(["pong", ""]))
                        continue
                    conn.sendall(encode_error("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING are allowed in this context"))
                    continue

                response, role = self.handle_command(args, role)
                conn.sendall(response)
        except Exception as e:
            logging.debug("Redis client error: %s", e)
        finally:
            if sub_sid is not None:
                notifier.unsubscribe(sub_sid)
            conn.close()
            logging.info("Redis client disconnected from %s", addr)

    def _auth_enabled(self) -> bool:
        return any(
            [
                settings.AUTH_ADMIN_TOKEN,
                settings.AUTH_WRITER_TOKEN,
                settings.AUTH_READER_TOKEN,
                settings.AUTH_ADMIN_PASSWORD,
                settings.AUTH_WRITER_PASSWORD,
                settings.AUTH_READER_PASSWORD,
            ]
        )

    def _role_for_secret(self, secret: str) -> Optional[str]:
        return best_role_for_secret(
            secret,
            admin_token=settings.AUTH_ADMIN_TOKEN,
            writer_token=settings.AUTH_WRITER_TOKEN,
            reader_token=settings.AUTH_READER_TOKEN,
            admin_password=settings.AUTH_ADMIN_PASSWORD,
            writer_password=settings.AUTH_WRITER_PASSWORD,
            reader_password=settings.AUTH_READER_PASSWORD,
        )

    def _required_role_for_cmd(self, cmd: str) -> str:
        if cmd in ("PING", "GET", "EXISTS", "INFO", "DBSIZE", "SUBSCRIBE", "UNSUBSCRIBE"):
            return ROLE_READER
        if cmd in ("SET", "DEL", "INCR", "INCRBY", "DECR", "DECRBY", "EXPIRE", "FLUSHALL"):
            return ROLE_WRITER
        if cmd == "AUTH":
            return ROLE_READER
        return ROLE_ADMIN

    def handle_command(self, args: List[bytes], role: Optional[str]) -> tuple[bytes, Optional[str]]:
        cmd = args[0].decode("utf-8").upper()
        start_time = time.time()
        registry.inc_requests(f"REDIS_{cmd}")
        
        try:
            if settings.REPLICATION_ROLE == "follower" and cmd in (
                "SET",
                "DEL",
                "INCR",
                "INCRBY",
                "DECR",
                "DECRBY",
                "EXPIRE",
                "FLUSHALL",
            ):
                return encode_error("READONLY You can't write against a read-only follower."), role

            if self._auth_enabled() and cmd != "AUTH":
                if role is None:
                    return encode_error("NOAUTH Authentication required."), role
                required = self._required_role_for_cmd(cmd)
                if not role_satisfies(role, required):
                    return encode_error("NOPERM this user has no permissions to run the command"), role

            if cmd == "AUTH":
                if len(args) not in (2, 3):
                    return encode_error("ERR wrong number of arguments for 'AUTH' command"), role
                secret = args[-1].decode("utf-8", errors="replace")
                new_role = self._role_for_secret(secret) if self._auth_enabled() else ROLE_ADMIN
                if new_role is None:
                    return encode_error("ERR invalid password"), role
                return encode_simple_string("OK"), new_role

            if cmd == "PING":
                return encode_simple_string("PONG"), role
            
            elif cmd == "SET":
                if len(args) < 3:
                    return encode_error("ERR wrong number of arguments for 'SET' command"), role
                key = args[1].decode("utf-8")
                val = args[2].decode("utf-8")
                ttl = None
                i = 3
                while i < len(args):
                    opt = args[i].decode("utf-8").upper()
                    if opt == "EX" and i + 1 < len(args):
                        ttl = float(args[i+1].decode("utf-8"))
                        i += 2
                    elif opt == "PX" and i + 1 < len(args):
                        ttl = float(args[i+1].decode("utf-8")) / 1000.0
                        i += 2
                    else:
                        break
                
                try:
                    self.store.read(key)
                    self.store.update(key, val, ttl)
                except KeyError:
                    self.store.create(key, val, ttl)
                return encode_simple_string("OK"), role
            
            elif cmd == "GET":
                if len(args) != 2:
                    return encode_error("ERR wrong number of arguments for 'GET' command"), role
                key = args[1].decode("utf-8")
                try:
                    val = self.store.read(key)
                    return encode_bulk_string(val), role
                except KeyError:
                    return encode_bulk_string(None), role
            
            elif cmd == "DEL":
                if len(args) < 2:
                    return encode_error("ERR wrong number of arguments for 'DEL' command"), role
                count = 0
                for i in range(1, len(args)):
                    key = args[i].decode("utf-8")
                    try:
                        self.store.delete(key)
                        count += 1
                    except KeyError:
                        pass
                return encode_integer(count), role
            
            elif cmd == "EXISTS":
                if len(args) < 2:
                    return encode_error("ERR wrong number of arguments for 'EXISTS' command"), role
                count = 0
                for i in range(1, len(args)):
                    key = args[i].decode("utf-8")
                    try:
                        self.store.read(key)
                        count += 1
                    except KeyError:
                        pass
                return encode_integer(count), role

            elif cmd in ("INCR", "INCRBY", "DECR", "DECRBY"):
                if len(args) < 2:
                    return encode_error(f"ERR wrong number of arguments for '{cmd}' command"), role
                key = args[1].decode("utf-8")
                delta = 1.0
                if cmd == "INCRBY":
                    if len(args) != 3:
                        return encode_error("ERR wrong number of arguments for 'INCRBY' command"), role
                    delta = float(args[2].decode("utf-8"))
                elif cmd == "DECR":
                    delta = -1.0
                elif cmd == "DECRBY":
                    if len(args) != 3:
                        return encode_error("ERR wrong number of arguments for 'DECRBY' command"), role
                    delta = -float(args[2].decode("utf-8"))
                
                try:
                    new_val = self.store.incr(key, delta)
                    return encode_integer(int(new_val)), role
                except TypeError:
                    return encode_error("ERR value is not an integer or out of range"), role
            
            elif cmd == "EXPIRE":
                if len(args) != 3:
                    return encode_error("ERR wrong number of arguments for 'EXPIRE' command"), role
                key = args[1].decode("utf-8")
                ttl = float(args[2].decode("utf-8"))
                try:
                    val = self.store.read(key)
                    self.store.update(key, val, ttl)
                    return encode_integer(1), role
                except KeyError:
                    return encode_integer(0), role

            elif cmd == "INFO":
                uptime = int(time.time() - registry.get_all()["started_at"])
                info = f"redis_version:2.0\r\nuptime_in_seconds:{uptime}\r\n"
                info += f"shards:{settings.SHARDS}\r\n"
                return encode_bulk_string(info), role

            elif cmd == "DBSIZE":
                return encode_integer(len(self.store.keys())), role

            elif cmd == "FLUSHALL":
                for shard in self.store._shards:
                    with shard._lock:
                        shard._map.clear()
                        shard._ttl.clear()
                        if hasattr(shard, '_skeys'):
                            shard._skeys.clear()
                return encode_simple_string("OK"), role

            else:
                return encode_error(f"ERR unknown command '{cmd}'"), role
        
        finally:
            elapsed_ms = (time.time() - start_time) * 1000.0
            registry.observe_latency(f"REDIS {cmd}", elapsed_ms)
