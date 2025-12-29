"""First-run setup validation and onboarding."""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from .config import ConfigError, load_telegram_config
from .constants import HOME_CONFIG_PATH

_OCTOPUS = "\N{OCTOPUS}"


@dataclass
class SetupResult:
    """Collected setup issues."""

    missing_codex: bool = False
    missing_or_invalid_config: bool = False
    config_path: Path | None = None

    @property
    def ok(self) -> bool:
        return not (self.missing_codex or self.missing_or_invalid_config)


def check_setup() -> SetupResult:
    """Check all prerequisites and return collected issues."""
    result = SetupResult()

    if not shutil.which("codex"):
        result.missing_codex = True

    try:
        config, config_path = load_telegram_config()
        result.config_path = config_path
    except ConfigError:
        result.missing_or_invalid_config = True
        result.config_path = HOME_CONFIG_PATH
        return result

    token = config.get("bot_token")
    if not isinstance(token, str) or not token.strip():
        result.missing_or_invalid_config = True

    chat_id_value = config.get("chat_id")
    if (
        chat_id_value is None
        or isinstance(chat_id_value, bool)
        or not isinstance(chat_id_value, int)
    ):
        result.missing_or_invalid_config = True

    return result


def _config_path_display(path: Path) -> str:
    """Format path for display, using ~ for home directory."""
    home = Path.home()
    try:
        return f"~/{path.relative_to(home)}"
    except ValueError:
        return str(path)


def _step_marker(step: int) -> str:
    return f"{step}."


def render_setup_guide(result: SetupResult) -> None:
    """Render a friendly setup guide panel to stderr."""
    console = Console(stderr=True)
    parts: list[str] = []
    step = 0
    needs_credentials_help = False

    if result.missing_codex:
        step += 1
        parts.append(
            f"[bold yellow]{_step_marker(step)}[/] [bold]Install the Codex CLI[/]"
        )
        parts.append("")
        parts.append("   [dim]$[/] npm install -g @openai/codex")
        parts.append("")

    config_display = (
        _config_path_display(result.config_path)
        if result.config_path
        else _config_path_display(HOME_CONFIG_PATH)
    )

    if result.missing_or_invalid_config:
        step += 1
        parts.append(f"[bold yellow]{_step_marker(step)}[/] [bold]Create a config[/]")
        parts.append("")
        parts.append(f"   [dim]{config_display}[/]")
        parts.append("")
        parts.append('   [cyan]bot_token[/] = [green]"123456789:ABCdef..."[/]')
        parts.append("   [cyan]chat_id[/]   = [green]123456789[/]")
        parts.append("")
        needs_credentials_help = True

    if needs_credentials_help:
        needs_token_help = True
        needs_chat_id_help = True

        parts.append("[dim]" + ("-" * 56) + "[/]")
        parts.append("")
        parts.append("[bold]Getting your Telegram credentials:[/]")
        parts.append("")
        if needs_token_help:
            parts.append(
                "   [cyan]bot_token[/]  create a bot with [link=https://t.me/BotFather]@BotFather[/]"
            )
        if needs_chat_id_help:
            parts.append(
                "   [cyan]chat_id[/]    get from [link=https://t.me/myidbot]@myidbot[/]"
            )

    while parts and not parts[-1].strip():
        parts.pop()

    panel = Panel(
        "\n".join(parts),
        title="[bold]Welcome to Takopi![/]",
        subtitle=f"{_OCTOPUS} setup required",
        border_style="yellow",
        padding=(1, 2),
        expand=False,
    )
    console.print(panel)


def demo_results() -> list[tuple[str, SetupResult]]:
    """Return sample setup results for previewing all onboarding modes."""
    config_path = HOME_CONFIG_PATH
    return [
        (
            "fresh-install",
            SetupResult(
                missing_codex=True,
                missing_or_invalid_config=True,
                config_path=config_path,
            ),
        ),
        (
            "missing-codex",
            SetupResult(
                missing_codex=True,
                config_path=config_path,
            ),
        ),
        (
            "missing-or-invalid-config",
            SetupResult(
                missing_or_invalid_config=True,
                config_path=config_path,
            ),
        ),
    ]
