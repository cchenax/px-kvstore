# px-kvstore

Key-Value Store solution for PhysicsX.

# example

Single write with TTL (60s)
curl -X PUT "http://localhost:8000/kv/foo?ttl=60" -d "bar"

# Read
# curl http://localhost:8000/kv/foo
# {"key":"foo","value":"bar"}

# Batch write
# curl -X POST http://localhost:8000/kv/batch \
#     -H "Content-Type: application/json" \
#     -d '{"items": {"a":1,"b":2,"c":3}, "ttl":120}'

# Batch read
# curl "http://localhost:8000/kv/batch?keys=a,b,x"
# {"a":1,"b":2}

**Author**: Tony Chen
