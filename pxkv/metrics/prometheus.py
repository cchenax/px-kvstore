#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typing import Any, Dict

def registry_to_prometheus(metrics: Dict[str, Any]) -> str:
    """
    Convert metrics registry JSON to Prometheus exposition format.
    """
    lines = []
    
    # Global metrics
    lines.append("# HELP pxkv_requests_total Total number of requests.")
    lines.append("# TYPE pxkv_requests_total counter")
    lines.append(f"pxkv_requests_total {metrics['requests_total']}")
    
    lines.append("# HELP pxkv_errors_total Total number of request errors.")
    lines.append("# TYPE pxkv_errors_total counter")
    lines.append(f"pxkv_errors_total {metrics['errors_total']}")
    
    # Requests by method
    for method, count in metrics["requests_by_method"].items():
        lines.append(f'pxkv_requests_by_method_total{{method="{method}"}} {count}')
        
    # AI Cache metrics
    ai = metrics["ai_cache"]
    lines.append("# HELP pxkv_ai_cache_lookups_total Total AI cache lookups.")
    lines.append(f"pxkv_ai_cache_lookups_total {ai['lookups']}")
    lines.append("# HELP pxkv_ai_cache_hits_total Total AI cache hits.")
    lines.append(f"pxkv_ai_cache_hits_total {ai['hits']}")
    lines.append("# HELP pxkv_ai_cache_misses_total Total AI cache misses.")
    lines.append(f"pxkv_ai_cache_misses_total {ai['misses']}")
    lines.append("# HELP pxkv_ai_cache_stores_total Total AI cache stores.")
    lines.append(f"pxkv_ai_cache_stores_total {ai['stores']}")
    
    # Latency metrics (simplified for now, just sum and count per route)
    latency = metrics.get("latency_ms", {})
    by_route = latency.get("by_route", {})
    for route, data in by_route.items():
        # Sanitize route name for prometheus label
        r_label = route.replace('"', '\\"')
        lines.append(f'pxkv_request_latency_ms_sum{{route="{r_label}"}} {data["sum_ms"]}')
        lines.append(f'pxkv_request_latency_ms_count{{route="{r_label}"}} {data["count"]}')
        
        # Buckets
        buckets = data.get("buckets", {})
        sorted_buckets = sorted([b for b in buckets.keys() if b != "inf"], key=float)
        cumulative = 0
        for b in sorted_buckets:
            cumulative += buckets[b]
            lines.append(f'pxkv_request_latency_ms_bucket{{route="{r_label}",le="{b}"}} {cumulative}')
        cumulative += buckets.get("inf", 0)
        lines.append(f'pxkv_request_latency_ms_bucket{{route="{r_label}",le="+Inf"}} {cumulative}')

    return "\n".join(lines) + "\n"
