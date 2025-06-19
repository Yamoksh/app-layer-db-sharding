# App Level DB Sharding

## Database Sharding Key Manager with Murmur Hash

A comprehensive Python library for managing database sharding using MurmurHash algorithm. This implementation provides both simple modulo-based sharding and advanced consistent hashing for better data distribution and easier scaling.

## Features

- **MurmurHash Implementation**: Fast, non-cryptographic hash function optimized for hash tables
- **Flexible Sharding**: Support for both simple modulo and consistent hashing approaches
- **Resharding Support**: Helper methods to analyze data migration during scaling
- **Distribution Analysis**: Tools to analyze key distribution across shards
- **Database Routing**: Utilities for routing queries to appropriate database shards

## Quick Start

### Basic Usage

```python
from shard_manager import create_user_shard_manager

# Create a shard manager with 4 shards
shard_manager = create_user_shard_manager(total_shards=4)

# Get shard ID for a key
user_id = "user_12345"
shard_id = shard_manager.get_shard_id(user_id)
print(f"User {user_id} belongs to shard {shard_id}")

# Analyze distribution
keys = ["user_1", "user_2", "user_3", "user_4", "user_5"]
distribution = shard_manager.get_shard_key_distribution(keys)
print(f"Distribution: {distribution}")
```

### Consistent Hashing

```python
from shard_manager import create_consistent_shard_manager

# Create consistent hash manager with virtual nodes
consistent_manager = create_consistent_shard_manager(
    total_shards=4,
    virtual_nodes=150
)

shard_id = consistent_manager.get_shard_id("user_12345")
```

### Database Routing

```python
from shard_manager import ShardManager, ShardConfig

# Configure sharding
config = ShardConfig(total_shards=8, hash_seed=42)
shard_manager = ShardManager(config)

# Get database configuration
db_configs = shard_manager.get_database_config("myapp")

# Route user to appropriate database
user_id = "user_12345"
shard_id = shard_manager.get_shard_id(user_id)
db_config = db_configs[shard_id]

print(f"Connect to: {db_config['host']}:{db_config['port']}")
print(f"Database: {db_config['database_name']}")
```

## API Reference

### ShardConfig

Configuration class for sharding parameters:

```python
@dataclass
class ShardConfig:
    total_shards: int        # Number of database shards
    hash_seed: int = 0       # Seed for hash function (for consistency)
    algorithm: str = "murmur3_32"  # Hash algorithm ("murmur3_32" or "murmur3_128")
```

### ShardManager

Main sharding management class:

#### Methods

- `get_shard_id(key)`: Returns shard ID for a given key
- `get_shard_key_distribution(keys)`: Analyzes key distribution across shards
- `get_database_config(base_name)`: Generates database configuration for all shards
- `migrate_key(key, old_total_shards)`: Analyzes if key needs migration during resharding

### ConsistentHashShardManager

Extended shard manager using consistent hashing:

```python
manager = ConsistentHashShardManager(config, virtual_nodes=150)
```

- Uses virtual nodes for better distribution
- Minimizes data movement during scaling
- Better for scenarios with frequent shard additions/removals

### MurmurHash

Low-level hash functions:

- `MurmurHash.murmur3_32(key, seed)`: 32-bit MurmurHash3
- `MurmurHash.murmur3_128(key, seed)`: 128-bit MurmurHash3 (returns 64-bit)

## Use Cases

### 1. User Data Sharding

```python
# Shard user data by user_id
shard_manager = create_user_shard_manager(total_shards=16)

def get_user_database(user_id):
    shard_id = shard_manager.get_shard_id(user_id)
    return f"user_db_shard_{shard_id}"

# Example: user_12345 -> user_db_shard_7
```

### 2. Order Processing

```python
# Shard orders by order_id or customer_id
order_shard_manager = create_user_shard_manager(total_shards=8, hash_seed=123)

def route_order_query(order_id, query):
    shard_id = order_shard_manager.get_shard_id(order_id)
    db_config = order_shard_manager.get_database_config("orders")[shard_id]
    return execute_on_shard(db_config, query)
```

### 3. Cache Partitioning

```python
# Distribute cache keys across Redis instances
cache_manager = create_consistent_shard_manager(total_shards=6)

def get_cache_instance(cache_key):
    shard_id = cache_manager.get_shard_id(cache_key)
    return redis_instances[shard_id]
```

## Resharding Strategy

When scaling your database shards:

```python
# Analyze migration requirements
old_shards = 4
new_shards = 8
new_manager = create_user_shard_manager(total_shards=new_shards)

keys_to_migrate = []
for key in all_keys:
    old_shard, new_shard, needs_migration = new_manager.migrate_key(key, old_shards)
    if needs_migration:
        keys_to_migrate.append((key, old_shard, new_shard))

print(f"Need to migrate {len(keys_to_migrate)} keys")
```

## Performance Characteristics

- **Hash Computation**: O(1) for key hashing
- **Shard Lookup**: O(1) for modulo sharding, O(log V) for consistent hashing (V = virtual nodes)
- **Memory Usage**: Minimal for modulo, O(V × S) for consistent hashing (S = shards)

## Best Practices

1. **Choose Appropriate Shard Count**: Start with 2-4x your current database capacity
2. **Use Consistent Seeds**: Keep `hash_seed` consistent across application restarts
3. **Monitor Distribution**: Regularly check key distribution to detect hotspots
4. **Plan for Growth**: Consider consistent hashing if you expect frequent scaling
5. **Test Resharding**: Always test migration logic in staging environment

## Example Applications

- **E-commerce**: Shard users, orders, and products
- **Social Media**: Distribute user profiles, posts, and relationships
- **IoT Platforms**: Partition device data and time-series metrics
- **Gaming**: Shard player data, game sessions, and leaderboards

## Files

- `shard_manager.py`: Core sharding implementation
- `example_usage.py`: Comprehensive usage examples and demonstrations
- `README.md`: This documentation

## Testing

Run the examples to see the sharding system in action:

```bash
python3 shard_manager.py       # Basic functionality demo
python3 example_usage.py       # Comprehensive examples
```

## Dependencies

- Python 3.7+
- `mmh3` library (recommended, will fall back to built-in hash if not available)

## Installation

First, install the mmh3 library for optimal performance:

```bash
pip install mmh3
```

The shard manager will automatically detect and use the mmh3 library if available. If mmh3 is not installed, it will fall back to a simple hash implementation (not recommended for production).

## License

This implementation is provided as-is for educational and production use.
