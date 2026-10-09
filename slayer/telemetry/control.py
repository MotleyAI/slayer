"""``slayer telemetry status|enable|disable|show``."""

from slayer.telemetry import recorder, sender, settings, spool
from slayer.telemetry.payload import UsageReport


def status() -> str:
    state = settings.resolve_state()
    install_id = settings.read_config().install_id
    return "\n".join([
        f"Telemetry is {'on' if state.enabled else 'off'} (decided by: {state.rule.value}).",
        f"Install ID: {install_id or 'none'}",
        f"Config: {settings.config_path()}",
    ])


def enable() -> None:
    settings.write_config(settings.read_config().model_copy(update={"setting": "enabled"}))


def disable() -> None:
    """Persist ``disabled`` and delete the install ID and every unsent batch."""
    recorder.reset()
    settings.write_config(settings.read_config().model_copy(update={
        "setting": "disabled", "install_id": None, "install_id_created": None,
    }))
    spool.purge(settings.spool_dir())


def pending_report() -> UsageReport:
    """Exactly what the next send would contain (with a fresh batch ID)."""
    directory = settings.spool_dir()
    return sender.build_report(config=settings.read_config(), batches=spool.load(spool.pending_files(directory)))
