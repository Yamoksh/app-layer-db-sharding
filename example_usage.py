#!/usr/bin/env python3
"""
Example usage of the Shard Manager for database sharding
"""

from shard_manager import (
    ShardConfig, 
    ShardManager, 
    ConsistentHashShardManager,
    create_user_shard_manager,
    create_consistent_shard_manager
)


class DatabaseRouter:
    """Example database router using the shard manager"""
    
    def __init__(self, shard_manager: ShardManager):
        self.shard_manager = shard_manager
        self.db_configs = shard_manager.get_database_config("myapp")
        
    def get_database_connection_info(self, user_id: str) -> dict:
        """Get database connection info for a specific user"""
        shard_id = self.shard_manager.get_shard_id(user_id)
        return self.db_configs[shard_id]
    
    def route_query(self, user_id: str, query: str) -> str:
        """Route a query to the appropriate shard"""
        db_info = self.get_database_connection_info(user_id)
        return f"Execute '{query}' on {db_info['database_name']} at {db_info['host']}:{db_info['port']}"


def demonstrate_basic_sharding():
    """Demonstrate basic sharding functionality"""
    print("=== Basic Sharding Demo ===")
    
    # Create shard manager with 8 shards
    shard_manager = create_user_shard_manager(total_shards=8, hash_seed=123)
    
    # Test user IDs
    user_ids = [
        "user_12345", "user_67890", "user_11111", "user_22222",
        "user_33333", "user_44444", "user_55555", "user_66666",
        "admin_001", "admin_002", "guest_001", "guest_002"
    ]
    
    print("User ID -> Shard mapping:")
    for user_id in user_ids:
        shard_id = shard_manager.get_shard_id(user_id)
        print(f"  {user_id:12} -> Shard {shard_id}")
    
    # Check distribution
    distribution = shard_manager.get_shard_key_distribution(user_ids)
    print(f"\nDistribution across {shard_manager.config.total_shards} shards:")
    for shard_id in sorted(distribution.keys()):
        count = distribution[shard_id]
        percentage = (count / len(user_ids)) * 100
        print(f"  Shard {shard_id}: {count} users ({percentage:.1f}%)")
    

def demonstrate_database_routing():
    """Demonstrate database routing"""
    print("\n=== Database Routing Demo ===")
    
    shard_manager = create_user_shard_manager(total_shards=4)
    router = DatabaseRouter(shard_manager)
    
    users = ["alice_123", "bob_456", "charlie_789", "diana_000"]
    
    for user in users:
        db_info = router.get_database_connection_info(user)
        print(f"User {user}:")
        print(f"  Database: {db_info['database_name']}")
        print(f"  Host: {db_info['host']}")
        print(f"  Port: {db_info['port']}")
        
        # Example query routing
        query_result = router.route_query(user, "SELECT * FROM orders WHERE user_id = ?")
        print(f"  Query routing: {query_result}")
        print()


def demonstrate_consistent_hashing():
    """Demonstrate consistent hashing vs regular modulo hashing"""
    print("=== Consistent Hashing vs Regular Hashing ===")
    
    # Regular shard manager
    regular_manager = create_user_shard_manager(total_shards=4)
    
    # Consistent hash shard manager
    consistent_manager = create_consistent_shard_manager(total_shards=4, virtual_nodes=100)
    
    test_keys = [f"user_{i}" for i in range(20)]
    
    print("Key distribution comparison:")
    print("Key\t\tRegular\tConsistent")
    print("-" * 35)
    
    for key in test_keys[:10]:  # Show first 10 for clarity
        regular_shard = regular_manager.get_shard_id(key)
        consistent_shard = consistent_manager.get_shard_id(key)
        print(f"{key:12}\t{regular_shard}\t{consistent_shard}")
    
    # Show distribution
    regular_dist = regular_manager.get_shard_key_distribution(test_keys)
    consistent_dist = consistent_manager.get_shard_key_distribution(test_keys)
    
    print(f"\nDistribution comparison (20 keys across 4 shards):")
    print("Shard ID\tRegular\tConsistent")
    print("-" * 30)
    for shard_id in range(4):
        regular_count = regular_dist.get(shard_id, 0)
        consistent_count = consistent_dist.get(shard_id, 0)
        print(f"{shard_id}\t\t{regular_count}\t{consistent_count}")


def demonstrate_resharding():
    """Demonstrate resharding scenario"""
    print("\n=== Resharding Demo ===")
    
    # Start with 3 shards, then scale to 5 shards
    old_manager = create_user_shard_manager(total_shards=3)
    new_manager = create_user_shard_manager(total_shards=5)
    
    test_keys = [f"user_{i:03d}" for i in range(15)]
    
    print("Resharding analysis (3 shards -> 5 shards):")
    print("Key\t\tOld Shard\tNew Shard\tMigration Needed")
    print("-" * 55)
    
    migration_count = 0
    for key in test_keys:
        old_shard, new_shard, needs_migration = new_manager.migrate_key(key, old_total_shards=3)
        migration_status = "YES" if needs_migration else "NO"
        if needs_migration:
            migration_count += 1
        print(f"{key:12}\t{old_shard}\t\t{new_shard}\t\t{migration_status}")
    
    migration_percentage = (migration_count / len(test_keys)) * 100
    print(f"\nMigration summary:")
    print(f"  Total keys: {len(test_keys)}")
    print(f"  Keys to migrate: {migration_count} ({migration_percentage:.1f}%)")
    print(f"  Keys staying: {len(test_keys) - migration_count} ({100-migration_percentage:.1f}%)")


def demonstrate_hash_collision_analysis():
    """Demonstrate hash collision analysis"""
    print("\n=== Hash Collision Analysis ===")
    
    from shard_manager import MurmurHash, HAS_MMH3
    
    print(f"Using {'mmh3 library' if HAS_MMH3 else 'fallback hash implementation'}")
    
    # Test hash distribution with different seeds
    keys = [f"key_{i}" for i in range(1000)]
    
    seeds = [0, 42, 123, 999]
    print("Hash distribution analysis with different seeds:")
    
    for seed in seeds:
        hashes = [MurmurHash.murmur3_32(key, seed) for key in keys[:100]]
        
        # Simple collision check (not comprehensive, just for demo)
        unique_hashes = len(set(hashes))
        collision_rate = ((len(hashes) - unique_hashes) / len(hashes)) * 100
        
        print(f"Seed {seed:3d}: {unique_hashes:3d} unique hashes out of {len(hashes):3d} keys "
              f"(collision rate: {collision_rate:.2f}%)")


if __name__ == "__main__":
    demonstrate_basic_sharding()
    demonstrate_database_routing()
    demonstrate_consistent_hashing()
    demonstrate_resharding()
    demonstrate_hash_collision_analysis() 
