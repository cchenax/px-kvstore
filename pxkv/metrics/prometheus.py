#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Any, Dict

def registry_to_prometheus(metrics: Dict[str, Any]) -> str:
    """
    Convert metrics registry JSON to Prometheus exposition format.
    """
    lines = []
    
    lines.append("# HELP pxkv_requests_total Total number of requests.")
    lines.append("# TYPE pxkv_requests_total counter")
    lines.append(f"pxkv_requests_total {metrics['requests_total']}")
    
    lines.append("# HELP pxkv_errors_total Total number of request errors.")
    lines.append("# TYPE pxkv_errors_total counter")
    lines.append(f"pxkv_errors_total {metrics['errors_total']}")
    
    for method, count in metrics["requests_by_method"].items():
        lines.append(f'pxkv_requests_by_method_total{{method="{method}"}} {count}')
        
    ai = metrics["ai_cache"]
    lines.append("# HELP pxkv_ai_cache_lookups_total Total AI cache lookups.")
    lines.append(f"pxkv_ai_cache_lookups_total {ai['lookups']}")
    lines.append("# HELP pxkv_ai_cache_hits_total Total AI cache hits.")
    lines.append(f"pxkv_ai_cache_hits_total {ai['hits']}")
    lines.append("# HELP pxkv_ai_cache_misses_total Total AI cache misses.")
    lines.append(f"pxkv_ai_cache_misses_total {ai['misses']}")
    lines.append("# HELP pxkv_ai_cache_stores_total Total AI cache stores.")
    lines.append(f"pxkv_ai_cache_stores_total {ai['stores']}")
    
    latency = metrics.get("latency_ms", {})
    by_route = latency.get("by_route", {})
    for route, data in by_route.items():
        r_label = route.replace('"', '\\"')
        lines.append(f'pxkv_request_latency_ms_sum{{route="{r_label}"}} {data["sum_ms"]}')
        lines.append(f'pxkv_request_latency_ms_count{{route="{r_label}"}} {data["count"]}')
        
        buckets = data.get("buckets", {})
        sorted_buckets = sorted([b for b in buckets.keys() if b != "inf"], key=float)
        cumulative = 0
        for b in sorted_buckets:
            cumulative += buckets[b]
            lines.append(f'pxkv_request_latency_ms_bucket{{route="{r_label}",le="{b}"}} {cumulative}')
        cumulative += buckets.get("inf", 0)
        lines.append(f'pxkv_request_latency_ms_bucket{{route="{r_label}",le="+Inf"}} {cumulative}')

    repl = metrics.get("replication", {})
    lines.append("# HELP pxkv_replication_leader_lsn Current leader WAL LSN.")
    lines.append("# TYPE pxkv_replication_leader_lsn gauge")
    lines.append(f"pxkv_replication_leader_lsn {int(repl.get('leader_lsn', 0) or 0)}")
    lines.append("# HELP pxkv_replication_follower_ack_lsn Last acknowledged LSN by follower.")
    lines.append("# TYPE pxkv_replication_follower_ack_lsn gauge")
    lines.append("# HELP pxkv_replication_follower_lag_lsn Leader-to-follower lag in LSN units.")
    lines.append("# TYPE pxkv_replication_follower_lag_lsn gauge")
    followers = repl.get("followers", {})
    for follower, data in followers.items():
        f_label = str(follower).replace('"', '\\"')
        lines.append(
            f'pxkv_replication_follower_ack_lsn{{follower="{f_label}"}} {int(data.get("ack_lsn", 0) or 0)}'
        )
        lines.append(
            f'pxkv_replication_follower_lag_lsn{{follower="{f_label}"}} {int(data.get("lag_lsn", 0) or 0)}'
        )

    return "\n".join(lines) + "\n"
