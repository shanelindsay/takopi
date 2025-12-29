from __future__ import annotations

from pathlib import Path

from takopi import onboarding


def test_check_setup_marks_missing_codex(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(onboarding.shutil, "which", lambda _name: None)
    monkeypatch.setattr(
        onboarding,
        "load_telegram_config",
        lambda: ({"bot_token": "token", "chat_id": 123}, tmp_path / "takopi.toml"),
    )

    result = onboarding.check_setup()

    assert result.missing_codex is True
    assert result.missing_or_invalid_config is False
    assert result.ok is False


def test_check_setup_marks_missing_config(monkeypatch) -> None:
    monkeypatch.setattr(onboarding.shutil, "which", lambda _name: "/usr/bin/codex")

    def _raise() -> None:
        raise onboarding.ConfigError("Missing config file")

    monkeypatch.setattr(onboarding, "load_telegram_config", _raise)

    result = onboarding.check_setup()

    assert result.missing_or_invalid_config is True
    assert result.config_path == onboarding.HOME_CONFIG_PATH


def test_check_setup_marks_invalid_chat_id(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(onboarding.shutil, "which", lambda _name: "/usr/bin/codex")
    monkeypatch.setattr(
        onboarding,
        "load_telegram_config",
        lambda: ({"bot_token": "token", "chat_id": "123"}, tmp_path / "takopi.toml"),
    )

    result = onboarding.check_setup()

    assert result.missing_or_invalid_config is True
