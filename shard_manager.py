try:
    import mmh3
    HAS_MMH3 = True
except ImportError:
    HAS_MMH3 = False
    import hashlib

from typing import Union, List, Optional
from dataclasses import dataclass


@dataclass
class ShardConfig:
    """Configuration for database sharding"""
    total_shards: int
    hash_seed: int = 0
    algorithm: str = "murmur3_32"  # or "murmur3_128" 


class MurmurHash:
    """Wrapper for MurmurHash algorithms using mmh3 library when available"""
    
    @staticmethod
    def murmur3_32(key: Union[str, bytes], seed: int = 0) -> int:
        """
        MurmurHash3 32-bit implementation using mmh3 library
        
        Args:
            key: The key to hash (string or bytes)
            seed: Hash seed for consistent hashing
            
        Returns:
            32-bit hash value (unsigned)
        """
        if HAS_MMH3:
            # mmh3.hash returns signed int by default, convert to unsigned
            return mmh3.hash(key, seed, signed=False)
        else:
            # Fallback implementation using Python's built-in hash
            if isinstance(key, str):
                key = key.encode('utf-8')
            
            # Simple fallback - combine key and seed for hashing
            combined = str(seed).encode() + key
            return abs(hash(combined.hex())) & 0xffffffff
    
    @staticmethod
    def murmur3_128(key: Union[str, bytes], seed: int = 0) -> int:
        """
        MurmurHash3 128-bit implementation using mmh3 library
        
        Args:
            key: The key to hash (string or bytes)
            seed: Hash seed for consistent hashing
            
        Returns:
            64-bit hash value (lower part of 128-bit hash)
        """
        if HAS_MMH3:
            # mmh3.hash128 returns a 128-bit integer, take lower 64 bits
            hash_128 = mmh3.hash128(key, seed, signed=False)
            return hash_128 & 0x7fffffffffffffff
        else:
            # Fallback implementation
            if isinstance(key, str):
                key = key.encode('utf-8')
            
            combined = str(seed).encode() + key
            return abs(hash(combined.hex())) & 0x7fffffffffffffff


class ShardManager:
    """Manages database sharding using murmur hash"""
    
    def __init__(self, config: ShardConfig):
        """
        Initialize the shard manager
        
        Args:
            config: Sharding configuration
        """
        self.config = config
        self._hash_func = self._get_hash_function()
        
    def _get_hash_function(self):
        """Get the appropriate hash function based on configuration"""
        if self.config.algorithm == "murmur3_32":
            return MurmurHash.murmur3_32
        elif self.config.algorithm == "murmur3_128":
            return MurmurHash.murmur3_128
        else:
            raise ValueError(f"Unsupported hash algorithm: {self.config.algorithm}")
    
    def get_shard_id(self, key: Union[str, int, bytes]) -> int:
        """
        Determine which shard a key belongs to
        
        Args:
            key: The sharding key
            
        Returns:
            Shard ID (0-based)
        """
        # Convert key to string if it's an integer
        if isinstance(key, int):
            key = str(key)
        
        # Calculate hash
        hash_value = self._hash_func(key, self.config.hash_seed)
        
        # Determine shard using modulo
        shard_id = hash_value % self.config.total_shards
        
        return shard_id
    
    def get_shard_key_distribution(self, keys: List[Union[str, int, bytes]]) -> dict:
        """
        Analyze the distribution of keys across shards
        
        Args:
            keys: List of keys to analyze
            
        Returns:
            Dictionary with shard_id -> count mapping
        """
        distribution = {}
        
        for key in keys:
            shard_id = self.get_shard_id(key)
            distribution[shard_id] = distribution.get(shard_id, 0) + 1
        
        return distribution
    
    def get_database_config(self, base_db_name: str = "myapp") -> dict:
        """
        Generate database configuration for each shard
        
        Args:
            base_db_name: Base name for databases
            
        Returns:
            Dictionary mapping shard_id to database configuration
        """
        db_configs = {}
        
        for shard_id in range(self.config.total_shards):
            db_configs[shard_id] = {
                'database_name': f"{base_db_name}_shard_{shard_id}",
                'shard_id': shard_id,
                'host': f"db-shard-{shard_id}.example.com",  # Customize as needed
                'port': 5432 + shard_id,  # Customize as needed
            }
        
        return db_configs
    
    def migrate_key(self, key: Union[str, int, bytes], 
                   old_total_shards: int) -> tuple:
        """
        Helper method for resharding - determines if a key needs to migrate
        
        Args:
            key: The key to check
            old_total_shards: Previous number of shards
            
        Returns:
            Tuple of (old_shard_id, new_shard_id, needs_migration)
        """
        # Calculate old shard
        hash_value = self._hash_func(key, self.config.hash_seed)
        old_shard_id = hash_value % old_total_shards
        
        # Calculate new shard
        new_shard_id = self.get_shard_id(key)
        
        needs_migration = old_shard_id != new_shard_id
        
        return old_shard_id, new_shard_id, needs_migration


class ConsistentHashShardManager(ShardManager):
    """
    Extended shard manager using consistent hashing for better redistribution
    """
    
    def __init__(self, config: ShardConfig, virtual_nodes: int = 150):
        """
        Initialize consistent hash shard manager
        
        Args:
            config: Sharding configuration
            virtual_nodes: Number of virtual nodes per shard for better distribution
        """
        super().__init__(config)
        self.virtual_nodes = virtual_nodes
        self._ring = self._build_hash_ring()
    
    def _build_hash_ring(self) -> List[tuple]:
        """Build the consistent hash ring"""
        ring = []
        
        for shard_id in range(self.config.total_shards):
            for vnode in range(self.virtual_nodes):
                # Create virtual node identifier
                vnode_key = f"shard_{shard_id}_vnode_{vnode}"
                hash_value = self._hash_func(vnode_key, self.config.hash_seed)
                ring.append((hash_value, shard_id))
        
        # Sort by hash value
        ring.sort(key=lambda x: x[0])
        return ring
    
    def get_shard_id(self, key: Union[str, int, bytes]) -> int:
        """
        Determine shard using consistent hashing
        
        Args:
            key: The sharding key
            
        Returns:
            Shard ID
        """
        if isinstance(key, int):
            key = str(key)
        
        hash_value = self._hash_func(key, self.config.hash_seed)
        
        # Find the first node in the ring with hash >= key_hash
        for ring_hash, shard_id in self._ring:
            if ring_hash >= hash_value:
                return shard_id
        
        # If no node found, wrap around to the first node
        return self._ring[0][1]


# Utility functions for common sharding scenarios
def create_user_shard_manager(total_shards: int, hash_seed: int = 42) -> ShardManager:
    """Create a shard manager optimized for user data"""
    config = ShardConfig(
        total_shards=total_shards,
        hash_seed=hash_seed,
        algorithm="murmur3_32"
    )
    return ShardManager(config)


def create_consistent_shard_manager(total_shards: int, 
                                  virtual_nodes: int = 150,
                                  hash_seed: int = 42) -> ConsistentHashShardManager:
    """Create a consistent hash shard manager"""
    config = ShardConfig(
        total_shards=total_shards,
        hash_seed=hash_seed,
        algorithm="murmur3_32"
    )
    return ConsistentHashShardManager(config, virtual_nodes)


if __name__ == "__main__":
    # Example usage
    print("=== Sharding Key Manager Demo ===\n")
    
    # Create shard manager with 4 shards
    shard_manager = create_user_shard_manager(total_shards=4)
    
    # Test with various keys
    test_keys = [
        "user_123", "user_456", "user_789", "user_101112",
        "order_1001", "order_1002", "order_1003", "order_1004",
        123, 456, 789, 101112
    ]
    
    print("Key -> Shard ID mapping:")
    for key in test_keys:
        shard_id = shard_manager.get_shard_id(key)
        print(f"  {key} -> Shard {shard_id}")
    
    # Analyze distribution
    print(f"\nDistribution analysis:")
    distribution = shard_manager.get_shard_key_distribution(test_keys)
    for shard_id in sorted(distribution.keys()):
        count = distribution[shard_id]
        percentage = (count / len(test_keys)) * 100
        print(f"  Shard {shard_id}: {count} keys ({percentage:.1f}%)")
    
    # Show database configuration
    print(f"\nDatabase configuration:")
    db_configs = shard_manager.get_database_config("ecommerce")
    for shard_id, config in db_configs.items():
        print(f"  Shard {shard_id}: {config}")
    
    # Test consistent hashing
    print(f"\n=== Consistent Hashing Demo ===")
    consistent_manager = create_consistent_shard_manager(total_shards=4)
    
    print("Consistent hash key -> Shard ID mapping:")
    for key in test_keys[:6]:  # Test with fewer keys for clarity
        shard_id = consistent_manager.get_shard_id(key)
        print(f"  {key} -> Shard {shard_id}") 
