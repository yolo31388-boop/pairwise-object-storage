"""
游戏内对象存储与CDN系统 - 核心模块

功能：
- 对象存储：分片存储、多副本冗余、SHA-256 校验和验证
- CDN分发：边缘节点缓存、按延迟就近路由
- 缓存控制：TTL 过期、ETag 验证
- 预取：基于访问转移模式的智能预测、优先级调度
- 带宽优化：zlib 压缩、单玩家令牌桶限速
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import hashlib
import zlib


PRIORITY_LEVELS = {"high": 0, "normal": 1, "low": 2}

DEFAULT_CONFIG = {
    "shard_size": 1024,            # 分片大小（字节）
    "redundancy": 2,               # 每个分片的副本数
    "storage_node_count": 4,       # 存储节点数量
    "cache_ttl": 300.0,            # CDN 缓存过期时间（秒）
    "rate_limit_bps": 1_000_000,   # 单玩家限速（字节/秒）
    "compression": True,           # 是否启用压缩传输
    "origin_latency": 250.0,       # 源站基准延迟（毫秒）
    "cdn_nodes": [
        {"node_id": "edge-asia", "region": "asia",
         "latencies": {"asia": 8.0, "europe": 120.0, "america": 150.0}},
        {"node_id": "edge-europe", "region": "europe",
         "latencies": {"asia": 120.0, "europe": 6.0, "america": 90.0}},
        {"node_id": "edge-america", "region": "america",
         "latencies": {"asia": 150.0, "europe": 90.0, "america": 5.0}},
    ],
}


class StorageError(Exception):
    """对象存储不可恢复错误（如所有副本损坏或丢失）。"""


@dataclass
class Config:
    pass


@dataclass
class Shard:
    shard_id: str
    index: int
    data: bytes
    checksum: str


@dataclass
class StoredObject:
    key: str
    size: int
    etag: str
    version: int
    shard_ids: List[str]
    metadata: Dict


@dataclass
class StorageNode:
    node_id: str
    shards: Dict[str, Shard] = field(default_factory=dict)
    alive: bool = True


@dataclass
class CacheEntry:
    data: bytes
    etag: str
    version: int
    expires_at: float


@dataclass
class CDNNode:
    node_id: str
    region: str
    latencies: Dict[str, float]
    cache: Dict[str, CacheEntry] = field(default_factory=dict)


class ObjectStorage:
    def __init__(self, config: Optional[Dict] = None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self._init_state()

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def _init_state(self):
        self._now = 0.0
        self._objects: Dict[str, StoredObject] = {}
        self._nodes: Dict[str, StorageNode] = {}
        for i in range(self.config["storage_node_count"]):
            node_id = f"storage-node-{i}"
            self._nodes[node_id] = StorageNode(node_id=node_id)
        self._cdn_nodes: Dict[str, CDNNode] = {}
        for spec in self.config["cdn_nodes"]:
            self._cdn_nodes[spec["node_id"]] = CDNNode(
                node_id=spec["node_id"],
                region=spec["region"],
                latencies=dict(spec["latencies"]),
            )
        self._transitions: Dict[str, Dict[str, int]] = {}
        self._last_access: Dict[str, str] = {}
        self._player_regions: Dict[str, str] = {}
        self._tokens: Dict[str, float] = {}
        self.stats = {"origin_fetches": 0}
        self._state = {}
        self._history = []

    def update(self, dt: float):
        """推进虚拟时钟：过期缓存、按速率补充限速令牌。"""
        self._now += dt
        for node in self._cdn_nodes.values():
            expired = [k for k, e in node.cache.items() if e.expires_at <= self._now]
            for k in expired:
                del node.cache[k]
        rate = self.config["rate_limit_bps"]
        for player_id, tokens in self._tokens.items():
            self._tokens[player_id] = min(float(rate), tokens + rate * dt)

    def reset(self):
        self._init_state()

    # ------------------------------------------------------------------
    # 对象存储：分片 + 冗余 + 校验
    # ------------------------------------------------------------------
    def _live_node_ids(self) -> List[str]:
        return [n.node_id for n in self._nodes.values() if n.alive]

    def store_object(self, key: str, data, metadata: Optional[Dict] = None) -> Dict:
        """分片存储对象，每个分片写入 redundancy 个不同节点，并维护 ETag/版本。"""
        if isinstance(data, str):
            data = data.encode("utf-8")
        data = bytes(data)
        shard_size = self.config["shard_size"]
        redundancy = self.config["redundancy"]
        live = self._live_node_ids()
        if not live:
            raise StorageError("没有可用的存储节点")

        if key in self._objects:  # 覆盖写：清理旧分片
            for shard_id in self._objects[key].shard_ids:
                for node in self._nodes.values():
                    node.shards.pop(shard_id, None)

        version = self._objects[key].version + 1 if key in self._objects else 1
        etag = hashlib.sha256(data).hexdigest()
        shard_ids = []
        offsets = range(0, max(len(data), 1), shard_size)
        for index, offset in enumerate(offsets):
            chunk = data[offset:offset + shard_size]
            shard_id = f"{key}#shard-{index}"
            checksum = hashlib.sha256(chunk).hexdigest()
            # 同一分片的副本放在不同节点上（环形分布）
            for replica in range(min(redundancy, len(live))):
                node = self._nodes[live[(index + replica) % len(live)]]
                node.shards[shard_id] = Shard(
                    shard_id=shard_id, index=index, data=chunk, checksum=checksum
                )
            shard_ids.append(shard_id)

        self._objects[key] = StoredObject(
            key=key, size=len(data), etag=etag, version=version,
            shard_ids=shard_ids, metadata=dict(metadata or {}),
        )
        # 旧版本缓存全部失效，避免客户端拿到过期资源
        for node in self._cdn_nodes.values():
            node.cache.pop(key, None)

        return {
            "key": key, "size": len(data), "shards": len(shard_ids),
            "replicas": min(redundancy, len(live)), "etag": etag, "version": version,
        }

    def _read_shard(self, shard_id: str) -> bytes:
        found_corrupt = False
        for node in self._nodes.values():
            if not node.alive or shard_id not in node.shards:
                continue
            shard = node.shards[shard_id]
            if hashlib.sha256(shard.data).hexdigest() == shard.checksum:
                return shard.data
            found_corrupt = True
        if found_corrupt:
            raise StorageError(f"分片 {shard_id} 所有副本校验失败")
        raise StorageError(f"分片 {shard_id} 没有可用副本")

    def get_object(self, key: str) -> bytes:
        """按分片重组对象，逐分片验证校验和，自动跳过故障节点和损坏副本。"""
        obj = self._objects.get(key)
        if obj is None:
            raise KeyError(f"对象不存在: {key}")
        return b"".join(self._read_shard(sid) for sid in obj.shard_ids)

    def object_info(self, key: str) -> Dict:
        obj = self._objects.get(key)
        if obj is None:
            raise KeyError(f"对象不存在: {key}")
        replicas = {}
        for shard_id in obj.shard_ids:
            replicas[shard_id] = [
                n.node_id for n in self._nodes.values()
                if n.alive and shard_id in n.shards
            ]
        return {
            "key": key, "size": obj.size, "version": obj.version,
            "etag": obj.etag, "shard_count": len(obj.shard_ids),
            "replicas": replicas,
        }

    def simulate_node_failure(self, node_id: str):
        self._nodes[node_id].alive = False

    def recover_node(self, node_id: str):
        self._nodes[node_id].alive = True

    def repair(self) -> int:
        """为副本数不足的分片补充副本，返回修复的分片数。"""
        redundancy = self.config["redundancy"]
        live = self._live_node_ids()
        repaired = 0
        for obj in self._objects.values():
            for index, shard_id in enumerate(obj.shard_ids):
                holders = [nid for nid in live
                           if shard_id in self._nodes[nid].shards]
                source = None
                for nid in holders:
                    shard = self._nodes[nid].shards[shard_id]
                    if hashlib.sha256(shard.data).hexdigest() == shard.checksum:
                        source = shard
                        break
                if source is None:
                    continue
                missing = min(redundancy, len(live)) - len(holders)
                if missing <= 0:
                    continue
                candidates = [nid for nid in live if nid not in holders]
                for offset, nid in enumerate(candidates[:missing]):
                    self._nodes[nid].shards[shard_id] = Shard(
                        shard_id=shard_id, index=index,
                        data=source.data, checksum=source.checksum,
                    )
                repaired += 1
        return repaired

    # ------------------------------------------------------------------
    # CDN 分发：边缘节点 + 就近路由
    # ------------------------------------------------------------------
    def cdn_distribution(self, key: Optional[str] = None) -> Dict:
        """把对象推送到所有 CDN 边缘节点，返回每个节点缓存的 key 列表。"""
        keys = [key] if key is not None else list(self._objects.keys())
        distributed = {}
        for node in self._cdn_nodes.values():
            for k in keys:
                obj = self._objects[k]
                node.cache[k] = CacheEntry(
                    data=self.get_object(k), etag=obj.etag,
                    version=obj.version,
                    expires_at=self._now + self.config["cache_ttl"],
                )
            distributed[node.node_id] = sorted(keys)
        return distributed

    def route_request(self, player_region: Optional[str]) -> str:
        """按玩家所在区域选择延迟最近的 CDN 节点，无节点时回源。"""
        best_id, best_latency = None, float("inf")
        for node in self._cdn_nodes.values():
            latency = node.latencies.get(
                player_region, self.config["origin_latency"])
            if latency < best_latency:
                best_id, best_latency = node.node_id, latency
        return best_id if best_id is not None else "origin"

    # ------------------------------------------------------------------
    # 缓存控制：TTL 过期 + ETag 验证
    # ------------------------------------------------------------------
    def validate_cache(self, node_id: str, key: str,
                       etag: Optional[str] = None) -> bool:
        """验证节点缓存是否有效：存在、未过期、与源站版本一致。"""
        node = self._cdn_nodes.get(node_id)
        entry = node.cache.get(key) if node else None
        if entry is None or entry.expires_at <= self._now:
            return False
        obj = self._objects.get(key)
        if obj is None or entry.etag != obj.etag:
            return False
        if etag is not None and entry.etag != etag:
            return False
        return True

    # ------------------------------------------------------------------
    # 预取：访问模式预测 + 优先级
    # ------------------------------------------------------------------
    def record_access(self, player_id: str, key: str):
        prev = self._last_access.get(player_id)
        if prev is not None and prev != key:
            successors = self._transitions.setdefault(prev, {})
            successors[key] = successors.get(key, 0) + 1
        self._last_access[player_id] = key

    def predict_next(self, after_key: str) -> List[str]:
        """根据全局访问转移频率，预测 after_key 之后最可能访问的资源。"""
        successors = self._transitions.get(after_key, {})
        return [k for k, _ in sorted(
            successors.items(), key=lambda item: (-item[1], item[0]))]

    def prefetch(self, player_id: str,
                 candidates: Optional[List[str]] = None) -> List[str]:
        """按 优先级 > 预测概率 的顺序，把预测资源预取到玩家就近的 CDN 节点。"""
        last = self._last_access.get(player_id)
        successors = self._transitions.get(last, {}) if last else {}
        keys = [k for k in successors if k in self._objects]
        if candidates is not None:
            keys = [k for k in keys if k in candidates]

        def sort_key(k):
            priority = self._objects[k].metadata.get("priority", "normal")
            return (PRIORITY_LEVELS.get(priority, 1), -successors[k], k)

        keys.sort(key=sort_key)
        node = self._cdn_nodes.get(
            self.route_request(self._player_regions.get(player_id)))
        prefetched = []
        for k in keys:
            if node is not None:
                obj = self._objects[k]
                node.cache[k] = CacheEntry(
                    data=self.get_object(k), etag=obj.etag,
                    version=obj.version,
                    expires_at=self._now + self.config["cache_ttl"],
                )
            prefetched.append(k)
        return prefetched

    # ------------------------------------------------------------------
    # 带宽优化：压缩 + 限速
    # ------------------------------------------------------------------
    def _compress(self, data: bytes):
        if not self.config["compression"]:
            return data, False
        packed = zlib.compress(data)
        if len(packed) < len(data):
            return packed, True
        return data, False

    def _throttle(self, player_id: str, size: int) -> float:
        """令牌桶限速：初始 1 秒突发额度，超出部分按速率排队，返回等待秒数。"""
        rate = float(self.config["rate_limit_bps"])
        tokens = self._tokens.get(player_id, rate)
        if size <= tokens:
            self._tokens[player_id] = tokens - size
            return 0.0
        wait = (size - tokens) / rate
        self._tokens[player_id] = 0.0
        return wait

    def bandwidth_optimization(self, player_id: str, key: str) -> Dict:
        """计算单个对象传输的优化方案：压缩率、限速下的传输时长。"""
        data = self.get_object(key)
        payload, compressed = self._compress(data)
        rate = float(self.config["rate_limit_bps"])
        return {
            "key": key,
            "original_size": len(data),
            "compressed_size": len(payload),
            "compressed": compressed,
            "compression_ratio": (len(payload) / len(data)) if data else 1.0,
            "rate_limit_bps": rate,
            "duration_s": len(payload) / rate,
        }

    # ------------------------------------------------------------------
    # 端到端下载：路由 + 缓存 + 压缩 + 限速
    # ------------------------------------------------------------------
    def download(self, player_id: str, key: str, region: str) -> Dict:
        self.record_access(player_id, key)
        self._player_regions[player_id] = region

        node_id = self.route_request(region)
        node = self._cdn_nodes.get(node_id)
        latency = (node.latencies.get(region, self.config["origin_latency"])
                   if node else self.config["origin_latency"])

        from_cache = False
        if node is not None and self.validate_cache(node_id, key):
            data = node.cache[key].data
            from_cache = True
        else:
            data = self.get_object(key)  # 缓存未命中，回源取数据
            self.stats["origin_fetches"] += 1
            latency += self.config["origin_latency"]
            if node is not None:
                obj = self._objects[key]
                node.cache[key] = CacheEntry(
                    data=data, etag=obj.etag, version=obj.version,
                    expires_at=self._now + self.config["cache_ttl"],
                )

        payload, compressed = self._compress(data)
        wait = self._throttle(player_id, len(payload))
        duration = wait + len(payload) / float(self.config["rate_limit_bps"])

        return {
            "key": key,
            "player_id": player_id,
            "node": node_id,
            "from_cache": from_cache,
            "compressed": compressed,
            "original_size": len(data),
            "bytes_transferred": len(payload),
            "duration_s": duration,
            "latency_ms": latency + duration * 1000.0,
        }
