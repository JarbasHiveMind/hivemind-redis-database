import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from hivemind_plugin_manager.database import Client
from hivemind_redis_database import RedisDB
from hivemind_redis_database.migration import _load_config, migrate_namespace


class FakeRedis:
    def __init__(self):
        self.storage = {}
        self.hashes = {}
        self.connection_pool = Mock()

    def exists(self, key):
        return key in self.storage or key in self.hashes

    def setnx(self, key, value):
        if not self.exists(key):
            self.storage[key] = value
            return True
        return False

    def set(self, key, value, nx=False, ex=None):
        del ex
        if nx:
            if self.exists(key):
                return False
            self.storage[key] = value
            return True
        self.storage[key] = value
        return True

    def get(self, key):
        return self.storage.get(key)

    def delete(self, key):
        removed = 0
        if key in self.storage:
            del self.storage[key]
            removed += 1
        if key in self.hashes:
            del self.hashes[key]
            removed += 1
        return removed

    def incr(self, key):
        value = int(self.storage.get(key, 0)) + 1
        self.storage[key] = str(value)
        return value

    def sadd(self, key, value):
        members = self.storage.setdefault(key, set())
        members.add(str(value))
        return 1

    def srem(self, key, value):
        members = self.storage.get(key)
        if isinstance(members, set):
            members.discard(str(value))
        return 1

    def smembers(self, key):
        members = self.storage.get(key, set())
        return set(members) if isinstance(members, set) else set()

    def hset(self, key, mapping):
        values = self.hashes.setdefault(key, {})
        values.update(mapping)
        return 1

    def scan_iter(self, pattern, count=None):
        del count
        import fnmatch

        for key in sorted(set(self.storage) | set(self.hashes)):
            if fnmatch.fnmatch(key, pattern):
                yield key

    def pipeline(self, transaction=False):
        del transaction
        return FakePipeline(self)

    def ping(self):
        return True


class FakePipeline:
    def __init__(self, redis_client):
        self.redis = redis_client
        self.commands = []

    def set(self, *args, **kwargs):
        self.commands.append(("set", args, kwargs))
        return self

    def sadd(self, *args, **kwargs):
        self.commands.append(("sadd", args, kwargs))
        return self

    def srem(self, *args, **kwargs):
        self.commands.append(("srem", args, kwargs))
        return self

    def hset(self, *args, **kwargs):
        self.commands.append(("hset", args, kwargs))
        return self

    def delete(self, *args, **kwargs):
        self.commands.append(("delete", args, kwargs))
        return self

    def incr(self, *args, **kwargs):
        self.commands.append(("incr", args, kwargs))
        return self

    def execute(self):
        for method, args, kwargs in self.commands:
            getattr(self.redis, method)(*args, **kwargs)
        self.commands = []
        return []


class MigrationTests(unittest.TestCase):
    def build_db(self, redis_client, *, cluster_hash_tag=None):
        db = object.__new__(RedisDB)
        db.redis = redis_client
        db.redis_pool = redis_client.connection_pool
        db.index_prefix = "client"
        db.redisearch_available = False
        db.is_cluster = True
        db.cluster_hash_tag = cluster_hash_tag
        return db

    def test_load_config_reads_server_json_shape(self):
        payload = {
            "database": {
                "module": "hivemind-redis-db-plugin",
                "hivemind-redis-db-plugin": {
                    "host": "redis.example.com",
                    "cluster_nodes": [{"host": "node1", "port": 6379}],
                },
            }
        }
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "server.json"
            path.write_text(json.dumps(payload), encoding="utf-8")

            config = _load_config(str(path), "hivemind-redis-db-plugin")

        self.assertEqual(config["host"], "redis.example.com")
        self.assertEqual(config["cluster_nodes"][0]["host"], "node1")

    def test_migrate_namespace_copies_raw_records_and_syncs_target(self):
        source_redis = FakeRedis()
        target_redis = FakeRedis()
        source_db = self.build_db(source_redis, cluster_hash_tag=None)
        target_db = self.build_db(target_redis, cluster_hash_tag="clients")

        source_redis.storage["client:client:1"] = Client(client_id=1, api_key="alpha-key", name="alpha").serialize()
        source_redis.storage["client:client:2"] = Client(client_id=2, api_key="revoked", name="").serialize()
        source_redis.storage["client:client:3"] = "__hivemind_creating__"

        summary = migrate_namespace(source_db, target_db, clear_target=True, dry_run=False)

        self.assertEqual(summary.copied_records, 2)
        self.assertEqual(summary.skipped_markers, 1)
        self.assertEqual(target_redis.get("client:{clients}:client:1"), source_redis.get("client:client:1"))
        self.assertEqual(target_redis.get("client:{clients}:client:2"), source_redis.get("client:client:2"))
        self.assertEqual(target_redis.get("client:{clients}:count"), 2)
        self.assertEqual(target_redis.get("client:{clients}:id_seq"), 2)
        self.assertEqual(target_redis.smembers("client:{clients}:name:alpha"), {"1"})
        self.assertEqual(target_redis.smembers("client:{clients}:api_key:alpha-key"), {"1"})
        self.assertEqual(target_redis.smembers("client:{clients}:api_key:revoked"), {"2"})

    def test_migrate_namespace_rejects_same_namespace(self):
        redis_client = FakeRedis()
        source_db = self.build_db(redis_client, cluster_hash_tag=None)
        target_db = self.build_db(redis_client, cluster_hash_tag=None)

        with self.assertRaises(ValueError):
            migrate_namespace(source_db, target_db)


if __name__ == "__main__":
    unittest.main()
