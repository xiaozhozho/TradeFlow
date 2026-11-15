"""Dolphin 连接管理改造测试（离线：不实例化、不连库）。"""
from tradeflow.stream.schema import TICK


def test_ensure_script_recreate_contains_drop():
    script = TICK.ensure_script(recreate=True)
    assert "dropStreamTable(`tickStreamTable)" in script
    assert "existsStreamTable(`tickStreamTable)" in script


def test_dolphin_provides_prepare_and_new_session():
    from tradeflow.stream.dolphin import Dolphin

    assert hasattr(Dolphin, "prepare_streams")
    assert hasattr(Dolphin, "prepare_engines")
    assert hasattr(Dolphin, "newSession")
