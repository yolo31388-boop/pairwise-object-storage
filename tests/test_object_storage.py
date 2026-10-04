"""
游戏内对象存储与CDN系统 - 验收测试
运行方式：python -m pytest tests/test_object_storage.py -q
共 14 个测试用例
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cdn.storage import ObjectStorage, StorageError


class TestObjectStorage:
    def setup_method(self):
        self.system = ObjectStorage()

    # 01 对象存储：大文件分片上传
    def test_case_01(self):
        data = b"x" * 10_240
        result = self.system.store_object("asset", data)
        info = self.system.object_info("asset")
        assert result["shards"] == 10
        assert info["shard_count"] == 10
        assert self.system.get_object("asset") == data

    # 02 对象存储：每个分片有多副本冗余，且分布在不同节点
    def test_case_02(self):
        data = b"y" * 3_000
        self.system.store_object("asset", data)
        info = self.system.object_info("asset")
        assert len(info["replicas"]) == 3
        for shard_id, nodes in info["replicas"].items():
            assert len(nodes) >= 2
            assert len(set(nodes)) == len(nodes)

    # 03 对象存储：一台存储节点宕机，对象仍可完整取回
    def test_case_03(self):
        data = b"abcdef" * 500
        self.system.store_object("asset", data)
        self.system.simulate_node_failure("storage-node-0")
        assert self.system.get_object("asset") == data
        # 修复后每个分片重新恢复到冗余副本数
        repaired = self.system.repair()
        assert repaired > 0
        for nodes in self.system.object_info("asset")["replicas"].values():
            assert len(nodes) >= 2

    # 04 对象存储：校验和能识别损坏副本，并从健康副本恢复
    def test_case_04(self):
        data = b"verify-me" * 400
        self.system.store_object("asset", data)
        shard_id = "asset#shard-0"
        for node in list(self.system._nodes.values())[:1]:
            node.shards[shard_id].data = b"corrupted"
        assert self.system.get_object("asset") == data
        # 所有副本都损坏时应报错，而不是返回错误数据
        for node in self.system._nodes.values():
            if shard_id in node.shards:
                node.shards[shard_id].data = b"corrupted"
        with pytest.raises(StorageError):
            self.system.get_object("asset")

    # 05 CDN分发：对象分发到所有边缘节点
    def test_case_05(self):
        self.system.store_object("a", b"aaa")
        self.system.store_object("b", b"bbb")
        distributed = self.system.cdn_distribution()
        assert set(distributed) == {"edge-asia", "edge-europe", "edge-america"}
        for nodes in distributed.values():
            assert nodes == ["a", "b"]
        assert self.system.validate_cache("edge-asia", "a")

    # 06 CDN分发：按延迟就近路由，玩家直连最近节点
    def test_case_06(self):
        self.system.store_object("a", b"aaa")
        assert self.system.route_request("europe") == "edge-europe"
        assert self.system.route_request("america") == "edge-america"
        result = self.system.download("p1", "a", "europe")
        assert result["node"] == "edge-europe"
        assert result["latency_ms"] < self.system.config["origin_latency"] + 100

    # 07 缓存控制：重复下载命中缓存，只回源一次
    def test_case_07(self):
        self.system.store_object("a", b"aaa")
        first = self.system.download("p1", "a", "asia")
        second = self.system.download("p1", "a", "asia")
        assert first["from_cache"] is False
        assert second["from_cache"] is True
        assert self.system.stats["origin_fetches"] == 1

    # 08 缓存控制：TTL 过期后缓存失效，重新回源
    def test_case_08(self):
        system = ObjectStorage({"cache_ttl": 60})
        system.store_object("a", b"aaa")
        system.download("p1", "a", "asia")
        assert system.validate_cache("edge-asia", "a") is True
        system.update(61)
        assert system.validate_cache("edge-asia", "a") is False
        assert system.download("p1", "a", "asia")["from_cache"] is False

    # 09 缓存控制：ETag 验证，资源更新后旧缓存不再有效
    def test_case_09(self):
        self.system.store_object("a", b"v1")
        self.system.cdn_distribution("a")
        etag = self.system.object_info("a")["etag"]
        assert self.system.validate_cache("edge-asia", "a", etag) is True
        assert self.system.validate_cache("edge-asia", "a", "stale-etag") is False
        self.system.store_object("a", b"v2")
        assert self.system.validate_cache("edge-asia", "a") is False
        assert self.system.download("p1", "a", "asia")["from_cache"] is False

    # 10 预取：根据玩家访问模式智能预测下一个资源
    def test_case_10(self):
        self.system.store_object("A", b"A" * 100)
        self.system.store_object("B", b"B" * 100)
        for _ in range(3):
            self.system.record_access("p1", "A")
            self.system.record_access("p1", "B")
        self.system.record_access("p1", "A")
        assert self.system.predict_next("A")[0] == "B"
        prefetched = self.system.prefetch("p1")
        assert prefetched[0] == "B"
        assert self.system.validate_cache("edge-asia", "B") is True

    # 11 预取：高优先级资源排在低优先级资源之前
    def test_case_11(self):
        self.system.store_object("X", b"X")
        self.system.store_object("hi-res", b"H" * 100, metadata={"priority": "high"})
        self.system.store_object("low-res", b"L" * 100, metadata={"priority": "low"})
        # X 之后等概率访问 hi-res / low-res
        for next_key in ("low-res", "hi-res"):
            self.system.record_access("p1", "X")
            self.system.record_access("p1", next_key)
        self.system.record_access("p1", "X")
        prefetched = self.system.prefetch("p1")
        assert prefetched.index("hi-res") < prefetched.index("low-res")

    # 12 带宽优化：可压缩资源压缩后传输，下行字节数变小
    def test_case_12(self):
        data = b"game-asset-" * 1000
        self.system.store_object("a", data)
        plan = self.system.bandwidth_optimization("p1", "a")
        assert plan["compressed"] is True
        assert plan["compressed_size"] < plan["original_size"]
        result = self.system.download("p1", "a", "asia")
        assert result["compressed"] is True
        assert result["bytes_transferred"] < result["original_size"]

    # 13 带宽优化：令牌桶限速，单玩家不能占满超过限额的带宽
    def test_case_13(self):
        system = ObjectStorage({"rate_limit_bps": 1_000_000})
        size = 2_000_000
        system.store_object("a", os.urandom(size))
        system.store_object("b", os.urandom(size))
        first = system.download("p1", "a", "asia")  # 消耗突发额度
        second = system.download("p1", "b", "asia")
        assert first["bytes_transferred"] == size
        assert second["duration_s"] >= size / 1_000_000 - 1e-6
        # update 后令牌按速率恢复
        system.update(0.5)
        assert 0 < system._tokens["p1"] <= 1_000_000

    # 14 端到端：远处玩家走最近 CDN 节点，二次下载命中缓存，延迟远低于回源
    def test_case_14(self):
        self.system.store_object("a", b"map" * 200)
        first = self.system.download("p1", "a", "america")
        second = self.system.download("p1", "a", "america")
        assert first["node"] == "edge-america"
        assert first["from_cache"] is False
        assert second["node"] == "edge-america"
        assert second["from_cache"] is True
        assert second["latency_ms"] < self.system.config["origin_latency"]
        assert second["latency_ms"] < first["latency_ms"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
