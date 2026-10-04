"""
游戏内对象存储与CDN系统 - 核心模块

功能：
- 对象存储：分片 + 多副本冗余，存储节点故障可恢复
- CDN分发：边缘节点 + 就近路由，玩家不再全部回源
- 缓存控制：TTL过期 + ETag验证
- 预取：基于访问转移统计的智能预测 + 优先级排序
- 带宽优化：zlib压缩 + 每玩家限速/总带宽公平分配
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import hashlib
import math
import time
import zlib


DEFAULT_SHARD_SIZE = 4 * 1024 * 1024   # 4MB 分片
DEFAULT_REPLICA_COUNT = 3              # 3 副本冗余
DEFAULT_STORAGE_NODE_COUNT = 6
DEFAULT_CACHE_TTL = 300.0              # 缓存过期时间（秒）
DEFAULT_TOTAL_BANDWIDTH = 100.0        # 总带宽 MB/s
DEFAULT_PER_PLAYER_LIMIT = 10.0        # 单玩家限速 MB/s


@dataclass
class Config:
    shard_size: int = DEFAULT_SHARD_SIZE
    replica_count: int = DEFAULT_REPLICA_COUNT
    storage_node_count: int = DEFAULT_STORAGE_NODE_COUNT
    cache_ttl: float = DEFAULT_CACHE_TTL
    total_bandwidth: float = DEFAULT_TOTAL_BANDWIDTH
    per_player_limit: float = DEFAULT_PER_PLAYER_LIMIT


class ObjectStorage:
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.shard_size = int(self.config.get("shard_size", DEFAULT_SHARD_SIZE))
        self.replica_count = int(self.config.get("replica_count", DEFAULT_REPLICA_COUNT))
        self.cache_ttl = float(self.config.get("cache_ttl", DEFAULT_CACHE_TTL))
        self.total_bandwidth = float(self.config.get("total_bandwidth", DEFAULT_TOTAL_BANDWIDTH))
        self.per_player_limit = float(self.config.get("per_player_limit", DEFAULT_PER_PLAYER_LIMIT))
        node_count = int(self.config.get("storage_node_count", DEFAULT_STORAGE_NODE_COUNT))

        self._state = {}
        self._history = []
        self._now = 0.0

        # 对象存储集群（不再只放本地服务器）
        self._storage_nodes: Dict[str, Dict[str, bytes]] = {
            "storage-node-%d" % i: {} for i in range(node_count)
        }
        self._failed_nodes = set()
        self._objects: Dict[str, Dict] = {}

        # CDN 边缘节点
        self._edge_nodes: Dict[str, Dict] = {}
        self._edge_content: Dict[str, set] = {}

        # 预取：访问转移统计（智能预测依据）
        self._access_log: List[str] = []
        self._transition_counts: Dict[Tuple[str, str], int] = {}
        self._last_access: Optional[str] = None

    # ------------------------------------------------------------------
    # 基础
    # ------------------------------------------------------------------
    @property
    def now(self) -> float:
        return self._now

    def update(self, dt: float):
        """推进系统时钟（驱动缓存过期等时间逻辑）。"""
        self._now += float(dt)

    def reset(self):
        config = dict(self.config)
        self.__init__(config)

    # ------------------------------------------------------------------
    # 对象存储：分片 + 冗余
    # ------------------------------------------------------------------
    def _live_nodes(self) -> List[str]:
        return [n for n in self._storage_nodes if n not in self._failed_nodes]

    def store_object(self, key: str, data, priority: int = 1,
                     metadata: Optional[Dict] = None) -> Dict:
        """分片存储对象，每个分片写入 replica_count 个不同存储节点。"""
        if isinstance(data, str):
            data = data.encode("utf-8")
        data = bytes(data)

        live = self._live_nodes()
        if not live:
            raise RuntimeError("没有可用的存储节点")
        replicas = max(1, min(self.replica_count, len(live)))

        # 删除旧版本分片
        old = self._objects.get(key)
        if old:
            for shard in old["shards"]:
                for node in self._storage_nodes.values():
                    node.pop(shard["id"], None)

        version = (old["version"] if old else 0) + 1
        etag = hashlib.sha256(data + b":" + str(version).encode()).hexdigest()[:16]

        shards = []
        span = max(1, self.shard_size)
        for index, offset in enumerate(range(0, max(1, len(data)), span)):
            chunk = data[offset:offset + span]
            shard_id = "%s#shard-%d" % (key, index)
            # 轮转放置，保证同一分片的副本落在不同节点上
            nodes = [live[(index + r) % len(live)] for r in range(replicas)]
            for node in nodes:
                self._storage_nodes[node][shard_id] = chunk
            shards.append({"id": shard_id, "size": len(chunk), "nodes": list(nodes)})

        self._objects[key] = {
            "key": key,
            "size": len(data),
            "version": version,
            "etag": etag,
            "priority": priority,
            "metadata": metadata or {},
            "stored_at": self._now,
            "shards": shards,
        }
        return {
            "key": key,
            "size": len(data),
            "shard_count": len(shards),
            "replica_count": replicas,
            "nodes": sorted({n for s in shards for n in s["nodes"]}),
            "version": version,
            "etag": etag,
        }

    def retrieve_object(self, key: str) -> bytes:
        """从任意存活副本读取分片并重组对象。"""
        meta = self._objects.get(key)
        if meta is None:
            raise KeyError("对象不存在: %s" % key)
        chunks = []
        for shard in meta["shards"]:
            for node in shard["nodes"]:
                if node not in self._failed_nodes and shard["id"] in self._storage_nodes[node]:
                    chunks.append(self._storage_nodes[node][shard["id"]])
                    break
            else:
                raise RuntimeError("分片所有副本均丢失: %s" % shard["id"])
        return b"".join(chunks)

    def fail_storage_node(self, node: str):
        if node not in self._storage_nodes:
            raise KeyError("存储节点不存在: %s" % node)
        self._failed_nodes.add(node)

    def recover_storage_node(self, node: str):
        self._failed_nodes.discard(node)

    # ------------------------------------------------------------------
    # CDN 分发：边缘节点 + 就近路由
    # ------------------------------------------------------------------
    def register_edge_node(self, name: str, region: str = "",
                           base_latency: float = 0.0):
        self._edge_nodes[name] = {"region": region, "base_latency": float(base_latency)}
        self._edge_content.setdefault(name, set())

    def cdn_distribution(self, key: str,
                         nodes: Optional[List[str]] = None) -> Dict:
        """把对象推送到边缘节点，玩家不再全部从源站下载。"""
        if key not in self._objects:
            raise KeyError("对象不存在: %s" % key)
        targets = nodes if nodes is not None else list(self._edge_nodes)
        distributed = []
        for name in targets:
            if name not in self._edge_nodes:
                raise KeyError("边缘节点不存在: %s" % name)
            self._edge_content[name].add(key)
            distributed.append(name)
        return {
            "key": key,
            "nodes": distributed,
            "node_count": len(distributed),
            "origin_only": False,
        }

    def route_request(self, key: str, player_latencies: Dict[str, float],
                      origin_latency: float = 1000.0) -> Dict:
        """为玩家选择延迟最低且已缓存该资源的边缘节点。"""
        candidates = {
            name: float(lat)
            for name, lat in player_latencies.items()
            if key in self._edge_content.get(name, set())
        }
        if candidates:
            best = min(candidates, key=lambda n: (candidates[n], n))
            return {"node": best, "latency": candidates[best], "from_origin": False}
        return {"node": "origin", "latency": float(origin_latency), "from_origin": True}

    # ------------------------------------------------------------------
    # 缓存控制：过期 + 验证
    # ------------------------------------------------------------------
    def cache_control(self, key: str) -> Dict:
        meta = self._objects.get(key)
        if meta is None:
            raise KeyError("对象不存在: %s" % key)
        return {
            "Cache-Control": "max-age=%d" % int(self.cache_ttl),
            "ETag": meta["etag"],
            "Expires": self._now + self.cache_ttl,
            "max_age": self.cache_ttl,
            "etag": meta["etag"],
            "version": meta["version"],
        }

    def validate_cache(self, key: str, etag: Optional[str] = None,
                       cached_at: Optional[float] = None) -> bool:
        """校验客户端缓存：未过期且 ETag 与当前版本一致才有效。"""
        meta = self._objects.get(key)
        if meta is None:
            return False
        if cached_at is not None and self._now - cached_at >= self.cache_ttl:
            return False  # 已过期
        if etag is not None and etag != meta["etag"]:
            return False  # 资源已更新，旧缓存失效
        return True

    # ------------------------------------------------------------------
    # 预取：智能预测 + 优先级
    # ------------------------------------------------------------------
    def record_access(self, key: str):
        """记录资源访问，用于学习玩家的资源转移规律。"""
        if self._last_access is not None and self._last_access != key:
            pair = (self._last_access, key)
            self._transition_counts[pair] = self._transition_counts.get(pair, 0) + 1
        self._last_access = key
        self._access_log.append(key)

    def prefetch(self, current: Optional[str] = None,
                 candidates: Optional[List[str]] = None,
                 limit: Optional[int] = None) -> List[str]:
        """提前下载接下来可能需要的资源，按优先级从高到低排序。

        - 智能：基于历史访问转移统计预测 current 之后最可能需要的资源
        - 优先级：priority 高的重要资源排在前面
        """
        scores: Dict[str, int] = {}
        if current is not None:
            for (src, dst), count in self._transition_counts.items():
                if src == current:
                    scores[dst] = scores.get(dst, 0) + count
        if candidates is None:
            candidates = list(self._objects.keys())

        def sort_key(name):
            priority = self._objects.get(name, {}).get("priority", 1)
            return (-priority, -scores.get(name, 0), name)

        ordered = sorted(candidates, key=sort_key)
        if limit is not None:
            ordered = ordered[:limit]
        return ordered

    # ------------------------------------------------------------------
    # 带宽优化：压缩 + 限速
    # ------------------------------------------------------------------
    @staticmethod
    def compress_data(data: bytes) -> bytes:
        return zlib.compress(data)

    @staticmethod
    def decompress_data(data: bytes) -> bytes:
        return zlib.decompress(data)

    def bandwidth_optimization(self, data: Optional[bytes] = None,
                               player_count: int = 1) -> Dict:
        """压缩传输内容，并在总带宽约束下对单玩家限速。"""
        result = {}
        if data is not None:
            compressed = zlib.compress(bytes(data))
            result.update({
                "original_size": len(data),
                "compressed_size": len(compressed),
                "compression_ratio": (len(compressed) / len(data)) if data else 1.0,
                "compressed": compressed,
            })
        player_count = max(1, int(player_count))
        # 单玩家限速 + 总带宽公平分配，防止单个玩家占满带宽
        per_player = min(self.per_player_limit, self.total_bandwidth / player_count)
        result.update({
            "per_player_rate": per_player,
            "per_player_limit": self.per_player_limit,
            "total_bandwidth": self.total_bandwidth,
            "player_count": player_count,
            "rate_limited": per_player < self.per_player_limit,
        })
        return result
