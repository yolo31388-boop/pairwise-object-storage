"""
游戏内对象存储与CDN系统 - 验收测试
运行方式：python -m pytest tests/test_object_storage.py -q
共 14 个测试用例
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cdn.storage import ObjectStorage


class TestObjectStorage:
    def setup_method(self):
        self.system = ObjectStorage({
            "shard_size": 1024 * 1024,   # 1MB 分片，便于测试
            "replica_count": 3,
            "storage_node_count": 6,
            "cache_ttl": 60.0,
            "total_bandwidth": 20.0,
            "per_player_limit": 5.0,
        })

    # ---- 对象存储：分片 ----
    def test_case_01(self):
        data = b"x" * (5 * 1024 * 1024 + 1)  # 5MB+，应被分成 6 片
        result = self.system.store_object("big-pack", data)
        assert result["shard_count"] == 6
        meta = self.system._objects["big-pack"]
        assert all(s["size"] <= 1024 * 1024 for s in meta["shards"])

    # ---- 对象存储：冗余副本 ----
    def test_case_02(self):
        self.system.store_object("res-a", b"a" * (3 * 1024 * 1024))
        meta = self.system._objects["res-a"]
        for shard in meta["shards"]:
            assert len(shard["nodes"]) == 3
            assert len(set(shard["nodes"])) == 3  # 副本落在不同节点

    # ---- 对象存储：节点故障后仍可恢复 ----
    def test_case_03(self):
        data = bytes(range(256)) * 8192  # 2MB
        self.system.store_object("res-b", data)
        self.system.fail_storage_node("storage-node-0")
        self.system.fail_storage_node("storage-node-3")
        assert self.system.retrieve_object("res-b") == data

    # ---- 对象存储：读写一致 ----
    def test_case_04(self):
        data = b"game-resource-payload" * 1000
        self.system.store_object("res-c", data)
        assert self.system.retrieve_object("res-c") == data

    # ---- CDN：分发到边缘节点 ----
    def test_case_05(self):
        self.system.store_object("map-01", b"map-data")
        for name in ("edge-bj", "edge-sh", "edge-us"):
            self.system.register_edge_node(name)
        result = self.system.cdn_distribution("map-01")
        assert result["node_count"] == 3
        assert result["origin_only"] is False
        for name in ("edge-bj", "edge-sh", "edge-us"):
            assert "map-01" in self.system._edge_content[name]

    # ---- CDN：路由选择最近节点而不是源站 ----
    def test_case_06(self):
        self.system.store_object("tex-01", b"texture")
        self.system.register_edge_node("edge-bj")
        self.system.register_edge_node("edge-us")
        self.system.cdn_distribution("tex-01")
        route = self.system.route_request(
            "tex-01",
            {"edge-bj": 8.0, "edge-us": 160.0},
            origin_latency=200.0,
        )
        assert route["node"] == "edge-bj"
        assert route["from_origin"] is False

    # ---- CDN：远距离玩家延迟显著降低 ----
    def test_case_07(self):
        self.system.store_object("audio-01", b"audio")
        self.system.register_edge_node("edge-local")
        self.system.cdn_distribution("audio-01")
        route = self.system.route_request(
            "audio-01", {"edge-local": 12.0}, origin_latency=300.0)
        assert route["latency"] == 12.0
        assert route["latency"] < 300.0

    # ---- 缓存：响应头包含过期与验证信息 ----
    def test_case_08(self):
        self.system.store_object("ui-01", b"ui")
        headers = self.system.cache_control("ui-01")
        assert headers["Cache-Control"] == "max-age=60"
        assert headers["max_age"] == 60.0
        assert headers["ETag"]

    # ---- 缓存：超过 TTL 后过期 ----
    def test_case_09(self):
        self.system.store_object("cfg-01", b"config")
        etag = self.system.cache_control("cfg-01")["ETag"]
        cached_at = self.system.now
        assert self.system.validate_cache("cfg-01", etag, cached_at) is True
        self.system.update(120.0)  # 超过 60s TTL
        assert self.system.validate_cache("cfg-01", etag, cached_at) is False

    # ---- 缓存：资源更新后旧缓存验证失败 ----
    def test_case_10(self):
        self.system.store_object("hero-skin", b"v1")
        old_etag = self.system.cache_control("hero-skin")["ETag"]
        self.system.store_object("hero-skin", b"v2-updated")
        new_etag = self.system.cache_control("hero-skin")["ETag"]
        assert old_etag != new_etag
        now = self.system.now
        assert self.system.validate_cache("hero-skin", old_etag, now) is False
        assert self.system.validate_cache("hero-skin", new_etag, now) is True

    # ---- 预取：根据访问规律智能预测 ----
    def test_case_11(self):
        for key in ("login", "lobby", "battle", "settings"):
            self.system.store_object(key, key.encode())
        for _ in range(5):
            self.system.record_access("login")
            self.system.record_access("lobby")
        self.system.record_access("login")
        self.system.record_access("settings")  # 噪声：只出现一次
        predicted = self.system.prefetch(current="login")
        assert predicted[0] == "lobby"

    # ---- 预取：重要资源优先下载 ----
    def test_case_12(self):
        self.system.store_object("bg-music", b"m", priority=1)
        self.system.store_object("hero-model", b"h", priority=10)
        self.system.store_object("npc-texture", b"n", priority=5)
        ordered = self.system.prefetch(
            candidates=["bg-music", "hero-model", "npc-texture"])
        assert ordered == ["hero-model", "npc-texture", "bg-music"]

    # ---- 带宽：传输前压缩 ----
    def test_case_13(self):
        data = b"repetitive-game-data" * 5000
        result = self.system.bandwidth_optimization(data=data)
        assert result["compressed_size"] < result["original_size"]
        assert result["compression_ratio"] < 1.0
        assert self.system.decompress_data(result["compressed"]) == data

    # ---- 带宽：单玩家限速 + 高峰期公平分配 ----
    def test_case_14(self):
        solo = self.system.bandwidth_optimization(player_count=1)
        assert solo["per_player_rate"] <= 5.0  # 单玩家不能超过限速
        peak = self.system.bandwidth_optimization(player_count=100)
        assert peak["rate_limited"] is True
        assert peak["per_player_rate"] * 100 <= 20.0  # 总带宽不被打爆


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
