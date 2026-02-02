if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from datetime import datetime, timedelta


@transformer
def transform(*args, **kwargs):
    start_iso = kwargs['fecha_inicio']
    end_iso = kwargs['fecha_fin']

    start = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))

    windows = []
    cur = start
    while cur < end:
        nxt = min(cur + timedelta(days=1), end)
        windows.append((cur.isoformat(), nxt.isoformat()))
        cur = nxt

    print(f"Windows generados: {len(windows)}")
    return windows


@test
def test_output(output, *args) -> None:
    assert isinstance(output, list)
    assert len(output) > 0
