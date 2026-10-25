from shoppulse.config import load_config


def test_profiles_and_env_override(monkeypatch):
    monkeypatch.delenv("SHOPPULSE_LAKE_ROOT", raising=False)   # fixture lake đặt biến này
    monkeypatch.setenv("SHOPPULSE_KAFKA_BOOTSTRAP", "broker:9999")
    monkeypatch.setenv("SHOPPULSE_TRENDING_TOP_K", "7")
    cfg = load_config("k8s")
    assert cfg.kafka_bootstrap == "broker:9999"
    assert cfg.trending_top_k == 7
    assert cfg.lake("silver", "events").startswith("hdfs://")
    assert cfg.kafka_options()["kafka.security.protocol"] == "SASL_PLAINTEXT"


def test_pii_salt_required_outside_local(monkeypatch):
    monkeypatch.delenv("PII_SALT", raising=False)
    cfg = load_config("k8s")
    try:
        _ = cfg.pii_salt
        raise AssertionError("phải báo lỗi khi thiếu PII_SALT trên cụm")
    except RuntimeError:
        pass
