"""Client-credential file (`~/.actae/client.json`) resolution.

These tests exercise the zero-copy self-hosted path: the `actae` binary (or
`actae login`) writes the endpoint + key, and the SDK picks them up with no
configuration. No server is required.
"""

import json

import pytest

from actae_client.client import ActaeClient, _load_client_credentials


def _write_cfg(tmp_path, payload):
    cfg = tmp_path / "actae-cfg"
    cfg.mkdir(parents=True, exist_ok=True)
    (cfg / "client.json").write_text(
        payload if isinstance(payload, str) else json.dumps(payload), encoding="utf-8"
    )
    return cfg


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    for var in ("ACTAE_API_KEY", "ACTAE_URL", "ACTAE_ENDPOINT", "ACTAE_WS_URL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("ACTAE_CONFIG_DIR", str(tmp_path / "actae-cfg"))


class TestLoadCredentials:
    def test_missing_file_is_none(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(tmp_path / "absent"))
        assert _load_client_credentials() is None

    def test_valid_file(self, tmp_path, monkeypatch):
        cfg = _write_cfg(
            tmp_path,
            {"schema": "actae.client/v1", "endpoint": "http://127.0.0.1:8002", "api_key": "sk-abc"},
        )
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        assert _load_client_credentials() == {
            "endpoint": "http://127.0.0.1:8002",
            "api_key": "sk-abc",
        }

    def test_trailing_slash_stripped(self, tmp_path, monkeypatch):
        cfg = _write_cfg(
            tmp_path,
            {"endpoint": "http://127.0.0.1:8002/", "api_key": "sk-abc"},
        )
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        assert _load_client_credentials()["endpoint"] == "http://127.0.0.1:8002"

    @pytest.mark.parametrize(
        "payload",
        [
            "{not json",
            "[]",
            '"a string"',
            {"endpoint": "ftp://x", "api_key": "sk-abc"},
            {"endpoint": "http://", "api_key": "sk-abc"},
            {"endpoint": "http://127.0.0.1:8002", "api_key": ""},
            {"endpoint": "http://127.0.0.1:8002", "api_key": "sk a b"},
            {"endpoint": 123, "api_key": "sk-abc"},
            {"endpoint": "http://127.0.0.1:8002"},
        ],
    )
    def test_invalid_payloads_are_absent(self, tmp_path, monkeypatch, payload):
        cfg = _write_cfg(tmp_path, payload)
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        assert _load_client_credentials() is None

    def test_single_secondary_fallback(self, tmp_path, monkeypatch):
        cfg = tmp_path / "actae-cfg"
        cfg.mkdir(parents=True, exist_ok=True)
        (cfg / "client-8003.json").write_text(
            json.dumps({"endpoint": "http://127.0.0.1:8003", "api_key": "sk-secondary"}),
            encoding="utf-8",
        )
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        assert _load_client_credentials() == {
            "endpoint": "http://127.0.0.1:8003",
            "api_key": "sk-secondary",
        }

    def test_multiple_secondaries_are_ambiguous(self, tmp_path, monkeypatch):
        cfg = tmp_path / "actae-cfg"
        cfg.mkdir(parents=True, exist_ok=True)
        for port in (8003, 8004):
            (cfg / f"client-{port}.json").write_text(
                json.dumps({"endpoint": f"http://127.0.0.1:{port}", "api_key": "sk-x"}),
                encoding="utf-8",
            )
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        assert _load_client_credentials() is None

    def test_default_wins_over_secondary(self, tmp_path, monkeypatch):
        cfg = _write_cfg(
            tmp_path, {"endpoint": "http://127.0.0.1:8002", "api_key": "sk-default"}
        )
        (cfg / "client-8003.json").write_text(
            json.dumps({"endpoint": "http://127.0.0.1:8003", "api_key": "sk-secondary"}),
            encoding="utf-8",
        )
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        assert _load_client_credentials()["api_key"] == "sk-default"

    def test_refuses_to_follow_a_symlink(self, tmp_path, monkeypatch):
        cfg = tmp_path / "actae-cfg"
        cfg.mkdir(parents=True, exist_ok=True)
        real = cfg / "real.json"
        real.write_text(
            json.dumps({"endpoint": "http://evil.example", "api_key": "sk-evil"}),
            encoding="utf-8",
        )
        link = cfg / "client.json"
        try:
            link.symlink_to(real)
        except (OSError, NotImplementedError):
            pytest.skip("symlinks unsupported on this platform")
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        assert _load_client_credentials() is None


class TestClientResolution:
    def test_bare_constructor_uses_file(self, tmp_path, monkeypatch):
        cfg = _write_cfg(
            tmp_path, {"endpoint": "http://127.0.0.1:8002", "api_key": "sk-from-file"}
        )
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        client = ActaeClient()
        assert client._api_key == "sk-from-file"
        assert client._endpoint == "http://127.0.0.1:8002"
        assert client._ws_endpoint == "ws://127.0.0.1:8002/ws"

    def test_explicit_args_win_over_file(self, tmp_path, monkeypatch):
        cfg = _write_cfg(
            tmp_path, {"endpoint": "http://127.0.0.1:8002", "api_key": "sk-from-file"}
        )
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        client = ActaeClient(api_key="sk-explicit", endpoint="http://host:9999")
        assert client._api_key == "sk-explicit"
        assert client._endpoint == "http://host:9999"

    def test_env_wins_over_file(self, tmp_path, monkeypatch):
        cfg = _write_cfg(
            tmp_path, {"endpoint": "http://127.0.0.1:8002", "api_key": "sk-from-file"}
        )
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        monkeypatch.setenv("ACTAE_API_KEY", "sk-from-env")
        monkeypatch.setenv("ACTAE_URL", "http://env-host:8000")
        client = ActaeClient()
        assert client._api_key == "sk-from-env"
        assert client._endpoint == "http://env-host:8000"

    def test_per_field_fallback(self, tmp_path, monkeypatch):
        """An explicit endpoint still takes the key from the file, and vice versa."""
        cfg = _write_cfg(
            tmp_path, {"endpoint": "http://127.0.0.1:8002", "api_key": "sk-from-file"}
        )
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        client = ActaeClient(endpoint="http://explicit:1234")
        assert client._api_key == "sk-from-file"
        assert client._endpoint == "http://explicit:1234"

    def test_from_env_uses_file(self, tmp_path, monkeypatch):
        cfg = _write_cfg(
            tmp_path, {"endpoint": "http://127.0.0.1:8002", "api_key": "sk-from-file"}
        )
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        client = ActaeClient.from_env()
        assert client._api_key == "sk-from-file"
        assert client._endpoint == "http://127.0.0.1:8002"

    def test_missing_everything_is_actionable(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(tmp_path / "absent"))
        with pytest.raises(ValueError, match="actae login"):
            ActaeClient()

    def test_invalid_file_is_actionable(self, tmp_path, monkeypatch):
        cfg = _write_cfg(tmp_path, "{not json")
        monkeypatch.setenv("ACTAE_CONFIG_DIR", str(cfg))
        with pytest.raises(ValueError, match="api_key is required"):
            ActaeClient()
