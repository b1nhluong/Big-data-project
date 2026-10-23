"""Đọc cấu hình ShopPulse (conf/shoppulse.yaml) theo profile + biến môi trường.

Thứ tự ưu tiên (cao → thấp):
  1. Biến môi trường SHOPPULSE_<KEY>
  2. profiles.<profile> trong YAML
  3. common trong YAML

Bí mật (salt ẩn danh, mật khẩu) KHÔNG nằm trong YAML: lấy từ biến môi trường,
trên Kubernetes được mount từ Secret (xem infra/k8s/security/secrets.example.yaml).
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "conf" / "shoppulse.yaml"


def _coerce(value: str) -> Any:
    """Chuyển chuỗi từ biến môi trường sang kiểu phù hợp (số, bool, JSON)."""
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none"}:
        return None
    for cast in (int, float):
        try:
            return cast(value)
        except ValueError:
            pass
    if value[:1] in "[{":
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            pass
    return value


@dataclass
class Config:
    profile: str
    values: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------ access
    def __getitem__(self, key: str) -> Any:
        return self.values[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)

    def __getattr__(self, key: str) -> Any:  # cfg.lake_root
        try:
            return self.values[key]
        except KeyError as exc:
            raise AttributeError(key) from exc

    # ------------------------------------------------------------------ paths
    def _root(self, key: str) -> str:
        root = str(self.values[key])
        if "://" in root or os.path.isabs(root):
            return root.rstrip("/")
        return str((REPO_ROOT / root).resolve())

    def lake(self, layer: str, name: str) -> str:
        """Đường dẫn bảng trong data lake, ví dụ lake('silver', 'events')."""
        return f"{self._root('lake_root')}/{layer}/{name}"

    def raw(self, name: str = "") -> str:
        return f"{self._root('raw_root')}/{name}".rstrip("/")

    def checkpoint(self, job: str) -> str:
        return f"{self._root('checkpoint_root')}/{job}"

    def model(self, name: str) -> str:
        return f"{self._root('models_root')}/{name}"

    def stream_input(self) -> str:
        return self._root("stream_input_dir")

    @property
    def is_local(self) -> bool:
        return self.profile == "local"

    # ------------------------------------------------------------------ secrets
    @staticmethod
    def secret(name: str, default: str | None = None) -> str | None:
        return os.environ.get(name, default)

    @property
    def pii_salt(self) -> str:
        salt = os.environ.get("PII_SALT")
        if salt:
            return salt
        if self.is_local:
            return "local-dev-salt-do-not-use-in-prod"
        raise RuntimeError("PII_SALT chưa được cấu hình (Secret shoppulse-secrets)")

    def kafka_options(self) -> dict[str, str]:
        """Tuỳ chọn cho Spark Kafka source/sink (kafka.* prefix)."""
        opts = {"kafka.bootstrap.servers": self.kafka_bootstrap}
        security = self.get("kafka_security")
        if security:
            user = self.secret("KAFKA_USERNAME", "")
            pwd = self.secret("KAFKA_PASSWORD", "")
            opts.update(
                {
                    "kafka.security.protocol": security,
                    "kafka.sasl.mechanism": self.get("kafka_sasl_mechanism", "SCRAM-SHA-512"),
                    "kafka.sasl.jaas.config": (
                        "org.apache.kafka.common.security.scram.ScramLoginModule required "
                        f'username="{user}" password="{pwd}";'
                    ),
                }
            )
        return opts


def load_config(profile: str | None = None, path: str | Path | None = None) -> Config:
    profile = profile or os.environ.get("SHOPPULSE_PROFILE", "local")
    path = Path(path or os.environ.get("SHOPPULSE_CONFIG", DEFAULT_CONFIG))
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if profile not in raw["profiles"]:
        raise ValueError(f"Không có profile '{profile}' trong {path}")
    values: dict[str, Any] = dict(raw.get("common", {}))
    values.update(raw["profiles"][profile])
    prefix = "SHOPPULSE_"
    for env_key, env_val in os.environ.items():
        if env_key.startswith(prefix) and env_key not in {"SHOPPULSE_PROFILE", "SHOPPULSE_CONFIG"}:
            values[env_key[len(prefix):].lower()] = _coerce(env_val)
    return Config(profile=profile, values=values)
