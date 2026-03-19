import unittest
from fnmatch import fnmatch
from unittest.mock import Mock, patch

from redis.cluster import ClusterNode

from hivemind_plugin_manager import DatabaseFactory
from hivemind_plugin_manager.database import Client
from hivemind_redis_database import RedisDB


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

    def set(self, key, value):
        self.storage[key] = value
        return True

    def get(self, key):
        return self.storage.get(key)

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
        for key in sorted(self.storage):
            if fnmatch(key, pattern):
                yield key

    def pipeline(self):
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

    def incr(self, *args, **kwargs):
        self.commands.append(("incr", args, kwargs))
        return self

    def execute(self):
        results = []
        for method, args, kwargs in self.commands:
            results.append(getattr(self.redis, method)(*args, **kwargs))
        self.commands = []
        return results


class ModuleListRedis:
    def execute_command(self, *args):
        if args == ("MODULE", "LIST"):
            return [["name", "search", "ver", 20600]]
        raise AssertionError(f"Unexpected command: {args}")


class FallbackSearchRedis:
    def execute_command(self, *args):
        if args == ("MODULE", "LIST"):
            raise RuntimeError("module list unavailable")
        if args == ("FT._LIST",):
            return []
        raise AssertionError(f"Unexpected command: {args}")


class RedisDBTests(unittest.TestCase):
    def build_db(self, redis_client):
        db = object.__new__(RedisDB)
        db.redis = redis_client
        db.redis_pool = redis_client.connection_pool
        db.index_prefix = "client"
        db.redisearch_available = False
        return db

    def test_database_factory_defaults_are_normalized(self):
        fake_redis = FakeRedis()

        with patch.object(DatabaseFactory, "get_class", return_value=RedisDB), \
                patch.object(RedisDB, "_detect_cluster", return_value=False), \
                patch.object(RedisDB, "_create_single_connection", return_value=fake_redis), \
                patch.object(RedisDB, "_check_redisearch_availability", return_value=False), \
                patch.object(RedisDB, "health_check", return_value=True):
            db = DatabaseFactory.create("hivemind-redis-db-plugin")

        self.assertEqual(db.host, "127.0.0.1")
        self.assertEqual(db.port, 6379)
        self.assertEqual(db.subfolder, "hivemind-core")

    def test_ssl_alias_is_accepted(self):
        fake_redis = FakeRedis()

        with patch.object(RedisDB, "_detect_cluster", return_value=False), \
                patch.object(RedisDB, "_create_single_connection", return_value=fake_redis), \
                patch.object(RedisDB, "_check_redisearch_availability", return_value=False), \
                patch.object(RedisDB, "health_check", return_value=True):
            db = RedisDB(host=None, port=None, ssl=True)

        self.assertEqual(db.host, "127.0.0.1")
        self.assertEqual(db.port, 6379)
        self.assertTrue(db.use_ssl)

    def test_get_startup_nodes_accepts_documented_dict_shape(self):
        db = object.__new__(RedisDB)
        db.cluster_nodes = [{"host": "redis-node1", "port": 6379}]
        db.host = "127.0.0.1"
        db.port = 6379

        startup_nodes = db._get_startup_nodes()

        self.assertEqual(len(startup_nodes), 1)
        self.assertIsInstance(startup_nodes[0], ClusterNode)
        self.assertEqual(startup_nodes[0].host, "redis-node1")
        self.assertEqual(startup_nodes[0].port, 6379)

    @patch("hivemind_redis_database.redis.StrictRedis")
    def test_detect_cluster_uses_redis_py_ssl_kwargs(self, strict_redis):
        client = Mock()
        client.info.return_value = {"cluster_enabled": 0}
        strict_redis.return_value = client

        db = object.__new__(RedisDB)
        db.host = "redis.example.com"
        db.port = 6380
        db.password = "secret"
        db.username = "default"
        db.use_ssl = True
        db.ssl = None
        db.ssl_certfile = None
        db.ssl_keyfile = None
        db.ssl_ca_certs = "/tmp/ca.crt"
        db.ssl_cert_reqs = "required"
        db.ssl_check_hostname = True
        db.cluster_nodes = None

        self.assertFalse(db._detect_cluster())

        kwargs = strict_redis.call_args.kwargs
        self.assertNotIn("ssl_context", kwargs)
        self.assertTrue(kwargs["ssl"])
        self.assertEqual(kwargs["ssl_ca_certs"], "/tmp/ca.crt")
        self.assertTrue(kwargs["ssl_check_hostname"])

    def test_get_ssl_kwargs_disables_hostname_check_when_verification_is_disabled(self):
        db = object.__new__(RedisDB)
        db.use_ssl = True
        db.ssl_cert_reqs = "none"
        db.ssl_check_hostname = True
        db.ssl_certfile = None
        db.ssl_keyfile = None
        db.ssl_ca_certs = None

        ssl_kwargs = db._get_ssl_kwargs()

        self.assertTrue(ssl_kwargs["ssl"])
        self.assertEqual(ssl_kwargs["ssl_cert_reqs"], "none")
        self.assertFalse(ssl_kwargs["ssl_check_hostname"])

    def test_redisearch_detected_from_module_list(self):
        db = object.__new__(RedisDB)
        db.redis = ModuleListRedis()

        self.assertTrue(db._check_redisearch_availability())

    def test_redisearch_falls_back_to_ft_list(self):
        db = object.__new__(RedisDB)
        db.redis = FallbackSearchRedis()

        self.assertTrue(db._check_redisearch_availability())

    def test_crud_flow_updates_indexes_and_revocation_state(self):
        redis_client = FakeRedis()
        db = self.build_db(redis_client)

        client = Client(client_id=1, api_key="alpha-key", name="alpha")
        self.assertTrue(db.add_item(client))
        self.assertEqual(len(db), 1)
        self.assertEqual(redis_client.smembers("client:name:alpha"), {"1"})
        self.assertEqual(redis_client.hashes["client:idx:1"]["api_key"], "alpha-key")

        found = db.search_by_value("name", "alpha")
        self.assertEqual([c.name for c in found], ["alpha"])

        client.name = "bravo"
        client.api_key = "bravo-key"
        self.assertTrue(db.update_client(client))
        self.assertEqual(redis_client.smembers("client:name:alpha"), set())
        self.assertEqual(redis_client.smembers("client:name:bravo"), {"1"})
        self.assertEqual(redis_client.hashes["client:idx:1"]["name"], "bravo")

        self.assertTrue(db.remove_client("1"))
        self.assertEqual(db.search_by_value("name", "bravo"), [])
        revoked = Client.deserialize(redis_client.get("client:client:1"))
        self.assertEqual(revoked.api_key, "revoked")
        self.assertEqual(redis_client.smembers("client:api_key:revoked"), {"1"})
        self.assertEqual(redis_client.hashes["client:idx:1"]["api_key"], "revoked")

    def test_add_item_updates_existing_client_in_place(self):
        redis_client = FakeRedis()
        db = self.build_db(redis_client)

        original = Client(client_id=7, api_key="key-1", name="first")
        updated = Client(client_id=7, api_key="key-2", name="second")

        self.assertTrue(db.add_item(original))
        self.assertTrue(db.add_item(updated))

        stored = Client.deserialize(redis_client.get("client:client:7"))
        self.assertEqual(stored.name, "second")
        self.assertEqual(stored.api_key, "key-2")
        self.assertEqual(len(db), 1)


if __name__ == "__main__":
    unittest.main()
