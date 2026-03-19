import os
import time
import unittest
from uuid import uuid4

import redis
from redis.cluster import ClusterNode

from hivemind_plugin_manager.database import Client
from hivemind_redis_database import RedisDB


class RealRedisIntegrationTest(unittest.TestCase):
    MODE = None

    @classmethod
    def setUpClass(cls):
        if os.getenv("REDIS_TEST_MODE") != cls.MODE:
            raise unittest.SkipTest(f"Skipping {cls.__name__} for mode {os.getenv('REDIS_TEST_MODE')!r}")

        cls.host = os.environ["REDIS_HOST"]
        cls.port = int(os.environ["REDIS_PORT"])
        cls.password = os.getenv("REDIS_PASSWORD")
        cls.username = os.getenv("REDIS_USERNAME", "default")
        cls.use_ssl = os.getenv("REDIS_USE_SSL", "0") == "1"
        cls.ca_cert = os.getenv("REDIS_CA_CERT")
        cls.expect_redisearch = os.getenv("REDIS_EXPECT_REDISEARCH", "0") == "1"
        cls.wait_for_service()

    @classmethod
    def wait_for_service(cls):
        deadline = time.time() + 60
        last_error = None

        while time.time() < deadline:
            try:
                if cls.MODE == "cluster":
                    client = redis.RedisCluster(
                        startup_nodes=[ClusterNode(cls.host, cls.port)],
                        decode_responses=True,
                        skip_full_coverage_check=True,
                    )
                else:
                    kwargs = {
                        "host": cls.host,
                        "port": cls.port,
                        "password": cls.password,
                        "username": cls.username,
                        "decode_responses": True,
                    }
                    if cls.use_ssl:
                        kwargs.update({
                            "ssl": True,
                            "ssl_ca_certs": cls.ca_cert,
                            "ssl_check_hostname": True,
                        })
                    client = redis.Redis(**kwargs)
                client.ping()
                client.close()
                return
            except Exception as err:
                last_error = err
                time.sleep(1)

        raise RuntimeError(f"Redis service for mode {cls.MODE} did not become ready: {last_error}")

    def make_prefix(self, suffix):
        return f"ci_{self.MODE}_{suffix}_{uuid4().hex[:8]}"

    def cleanup_prefix(self, db, prefix):
        try:
            for key in list(db.redis.scan_iter(f"{prefix}:*", count=100)):
                try:
                    db.redis.delete(key)
                except Exception:
                    pass
        except Exception:
            pass

        try:
            db.redis.execute_command("FT.DROPINDEX", f"{prefix}_search_index", "DD")
        except Exception:
            pass

        try:
            db.redis.close()
        except Exception:
            pass

    def build_db(self, prefix, *, ssl_alias=False, cluster_nodes=None):
        kwargs = {
            "host": self.host,
            "port": self.port,
            "password": self.password,
            "username": self.username,
            "index_prefix": prefix,
        }
        if cluster_nodes is not None:
            kwargs.pop("host")
            kwargs.pop("port")
            kwargs["cluster_nodes"] = cluster_nodes
        if self.use_ssl:
            ssl_flag = "ssl" if ssl_alias else "use_ssl"
            kwargs[ssl_flag] = True
            kwargs["ssl_ca_certs"] = self.ca_cert
            kwargs["ssl_check_hostname"] = True
        db = RedisDB(**kwargs)
        self.addCleanup(self.cleanup_prefix, db, prefix)
        return db

    def assert_crud_flow(self, db, *, expected_cluster):
        self.assertEqual(db.is_cluster, expected_cluster)
        self.assertEqual(db.redisearch_available, self.expect_redisearch)
        self.assertTrue(db.health_check())

        client = Client(client_id=1, api_key="alpha-key", name="alpha")
        self.assertTrue(db.add_item(client))
        if self.expect_redisearch:
            self.assertEqual([c.name for c in db._search_with_redisearch("name", "alpha")], ["alpha"])
        self.assertEqual([c.name for c in db.search_by_value("name", "alpha")], ["alpha"])

        client.name = "beta"
        client.api_key = "beta-key"
        self.assertTrue(db.update_client(client))
        if self.expect_redisearch:
            self.assertEqual([c.name for c in db._search_with_redisearch("name", "beta")], ["beta"])
        self.assertEqual([c.name for c in db.search_by_value("name", "beta")], ["beta"])

        self.assertTrue(db.remove_client(str(client.client_id)))
        if self.expect_redisearch:
            self.assertEqual(db._search_with_redisearch("name", "beta"), [])
        self.assertEqual(db.search_by_value("name", "beta"), [])


class SingleRedisIntegrationTests(RealRedisIntegrationTest):
    MODE = "single"

    def test_single_instance_crud_and_search(self):
        db = self.build_db(self.make_prefix("single"))
        self.assert_crud_flow(db, expected_cluster=False)


class TlsRedisIntegrationTests(RealRedisIntegrationTest):
    MODE = "tls"

    def test_tls_with_use_ssl(self):
        db = self.build_db(self.make_prefix("tls-use"))
        self.assert_crud_flow(db, expected_cluster=False)

    def test_tls_with_ssl_alias(self):
        db = self.build_db(self.make_prefix("tls-alias"), ssl_alias=True)
        self.assert_crud_flow(db, expected_cluster=False)


class ClusterRedisIntegrationTests(RealRedisIntegrationTest):
    MODE = "cluster"

    def test_cluster_auto_detect_from_host_port(self):
        db = self.build_db(self.make_prefix("cluster-host"))
        self.assert_crud_flow(db, expected_cluster=True)

    def test_cluster_with_explicit_startup_nodes(self):
        db = self.build_db(
            self.make_prefix("cluster-nodes"),
            cluster_nodes=[{"host": self.host, "port": self.port}],
        )
        self.assert_crud_flow(db, expected_cluster=True)
