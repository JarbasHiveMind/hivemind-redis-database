import unittest
from fnmatch import fnmatch
from unittest.mock import Mock, patch

from redis.cluster import ClusterNode, key_slot

from hivemind_plugin_manager import DatabaseFactory
from hivemind_plugin_manager.database import Client
from hivemind_redis_database import RedisDB


class FakeRedis:
    def __init__(self):
        self.storage = {}
        self.hashes = {}
        self.connection_pool = Mock()
        self.pipeline_transaction_flags = []

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
        for key in sorted(set(self.storage) | set(self.hashes)):
            if fnmatch(key, pattern):
                yield key

    def pipeline(self, transaction=False):
        self.pipeline_transaction_flags.append(transaction)
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


class SearchRedis:
    def __init__(self):
        self.docs = {
            "client:client:1": '{"client_id": 1, "api_key": "key-1", "name": "alpha beta"}',
            "client:client:2": '{"client_id": 2, "api_key": "key-2", "name": "alpha"}',
        }

    def execute_command(self, *args):
        if args[:2] == ("FT.SEARCH", "client_search_index"):
            return [2, "client:idx:1", [], "client:idx:2", []]
        raise AssertionError(f"Unexpected command: {args}")

    def get(self, key):
        return self.docs.get(key)


class RedisDBTests(unittest.TestCase):
    def build_db(self, redis_client, *, is_cluster=False, cluster_hash_tag=None):
        db = object.__new__(RedisDB)
        db.redis = redis_client
        db.redis_pool = redis_client.connection_pool
        db.index_prefix = "client"
        db.redisearch_available = False
        db.is_cluster = is_cluster
        db.cluster_hash_tag = cluster_hash_tag
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
        db.cluster_hash_tag = None
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

    def test_redisearch_results_are_filtered_to_exact_matches(self):
        db = object.__new__(RedisDB)
        db.redis = SearchRedis()
        db.index_prefix = "client"
        db.cluster_hash_tag = None

        results = db._search_with_redisearch("name", "alpha")

        self.assertEqual([(client.client_id, client.name) for client in results], [(2, "alpha")])

    def test_cluster_hash_tag_places_keys_in_one_slot(self):
        db = object.__new__(RedisDB)
        db.index_prefix = "client"
        db.cluster_hash_tag = "clients"

        slots = {
            key_slot(db._client_key(1).encode()),
            key_slot(db._name_index_key("alpha").encode()),
            key_slot(db._api_key_index_key("alpha-key").encode()),
            key_slot(db._search_doc_key(1).encode()),
            key_slot(db._counter_key().encode()),
            key_slot(db._id_sequence_key().encode()),
        }

        self.assertEqual(len(slots), 1)

    def test_cluster_hash_tag_uses_transactional_pipeline_and_namespaced_keys(self):
        redis_client = FakeRedis()
        db = self.build_db(redis_client, is_cluster=True, cluster_hash_tag="clients")

        client = Client(client_id=1, api_key="alpha-key", name="alpha")

        self.assertTrue(db.add_item(client))
        self.assertTrue(redis_client.pipeline_transaction_flags[-1])
        self.assertEqual(redis_client.get("client:{clients}:client:1"), client.serialize())
        self.assertEqual(redis_client.smembers("client:{clients}:name:alpha"), {"1"})
        self.assertEqual(redis_client.smembers("client:{clients}:api_key:alpha-key"), {"1"})
        self.assertEqual(redis_client.hashes["client:{clients}:idx:1"]["name"], "alpha")
        self.assertEqual(redis_client.get("client:{clients}:count"), "1")

    def test_setup_redisearch_index_uses_hash_tag_prefix(self):
        db = object.__new__(RedisDB)
        db.redis = Mock()
        db.index_prefix = "client"
        db.cluster_hash_tag = "clients"

        db._setup_redisearch_index()

        args = db.redis.execute_command.call_args.args
        self.assertEqual(args[1], "client_clients_search_index")
        self.assertEqual(args[6], "client:{clients}:idx:")

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

    def test_add_item_retries_generated_client_id_collision(self):
        redis_client = FakeRedis()
        redis_client.storage["client:client:1"] = Client(client_id=1, api_key="taken", name="taken").serialize()
        db = self.build_db(redis_client)
        db.retry_delay = 0

        created = Client(client_id=99, api_key="fresh-key", name="fresh")
        created.client_id = None

        self.assertTrue(db.add_item(created))
        self.assertEqual(created.client_id, 2)

        stored = Client.deserialize(redis_client.get("client:client:2"))
        self.assertEqual(stored.client_id, 2)
        self.assertEqual(stored.name, "fresh")
        self.assertEqual(stored.api_key, "fresh-key")

    def test_add_item_skips_stuck_create_marker_for_generated_id(self):
        redis_client = FakeRedis()
        redis_client.storage["client:client:1"] = "__hivemind_creating__"
        db = self.build_db(redis_client)
        db.retry_delay = 0
        db.retry_attempts = 0

        created = Client(client_id=0, api_key="fresh-key", name="fresh")
        created.client_id = None

        self.assertTrue(db.add_item(created))
        self.assertEqual(created.client_id, 2)
        self.assertEqual(Client.deserialize(redis_client.get("client:client:2")).name, "fresh")

    def test_add_item_returns_false_for_locked_explicit_id(self):
        redis_client = FakeRedis()
        redis_client.storage["client:client:7"] = "__hivemind_creating__"
        db = self.build_db(redis_client)
        db.retry_delay = 0
        db.retry_attempts = 0

        self.assertFalse(db.add_item(Client(client_id=7, api_key="fresh-key", name="fresh")))

    def test_iter_skips_revoked_and_in_progress_records(self):
        redis_client = FakeRedis()
        db = self.build_db(redis_client)

        active = Client(client_id=1, api_key="active-key", name="active")
        revoked = Client(client_id=2, api_key="revoked-key", name="revoked")

        self.assertTrue(db.add_item(active))
        self.assertTrue(db.add_item(revoked))
        self.assertTrue(db.remove_client("2"))

        redis_client.storage["client:client:3"] = "__hivemind_creating__"

        clients = list(db)

        self.assertEqual([(client.client_id, client.api_key) for client in clients], [(1, "active-key")])

    def test_sync_rebuilds_indexes_counters_and_removes_stale_markers(self):
        redis_client = FakeRedis()
        db = self.build_db(redis_client)
        db.redisearch_available = False

        redis_client.storage["client:client:1"] = Client(client_id=1, api_key="alpha-key", name="alpha").serialize()
        redis_client.storage["client:client:2"] = Client(client_id=2, api_key="revoked", name="").serialize()
        redis_client.storage["client:client:3"] = "__hivemind_creating__"
        redis_client.storage["client:count"] = "99"
        redis_client.storage["client:id_seq"] = "1"
        redis_client.storage["client:name:stale"] = {"999"}
        redis_client.storage["client:api_key:stale"] = {"999"}
        redis_client.hashes["client:idx:999"] = {"name": "stale", "api_key": "stale"}

        self.assertTrue(db.sync())

        self.assertEqual(len(db), 2)
        self.assertEqual(redis_client.get("client:id_seq"), 2)
        self.assertIsNone(redis_client.get("client:client:3"))
        self.assertEqual(redis_client.smembers("client:name:alpha"), {"1"})
        self.assertEqual(redis_client.smembers("client:api_key:alpha-key"), {"1"})
        self.assertEqual(redis_client.smembers("client:api_key:revoked"), {"2"})
        self.assertEqual(redis_client.smembers("client:name:stale"), set())
        self.assertEqual(redis_client.smembers("client:api_key:stale"), set())
        self.assertEqual(redis_client.hashes["client:idx:1"]["name"], "alpha")
        self.assertNotIn("client:idx:999", redis_client.hashes)


if __name__ == "__main__":
    unittest.main()
