import unittest
from unittest.mock import Mock, patch

from redis.cluster import ClusterNode

from hivemind_plugin_manager import DatabaseFactory
from hivemind_redis_database import RedisDB


class FakeRedis:
    def __init__(self):
        self.storage = {}

    def exists(self, key):
        return key in self.storage

    def setnx(self, key, value):
        if key not in self.storage:
            self.storage[key] = value
            return True
        return False

    def ping(self):
        return True


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

    def test_redisearch_detected_from_module_list(self):
        db = object.__new__(RedisDB)
        db.redis = ModuleListRedis()

        self.assertTrue(db._check_redisearch_availability())

    def test_redisearch_falls_back_to_ft_list(self):
        db = object.__new__(RedisDB)
        db.redis = FallbackSearchRedis()

        self.assertTrue(db._check_redisearch_availability())


if __name__ == "__main__":
    unittest.main()
