from concurrent.futures import ThreadPoolExecutor
from app.services.mfi_drafter.render_worker import RenderWorker
from app.services.mfi_drafter.reliable_contracts import fingerprint


def test_concurrent_spawned_workers_keep_run_and_chart_data_isolated(monkeypatch, tmp_path):
    import sys
    from types import ModuleType
    # Streamlit replaces __main__ with the page module. A worker must never
    # execute that script when launching its independent rendering process.
    page = tmp_path / "streamlit_page.py"
    page.write_text("raise RuntimeError('The rendering worker re-executed the UI')", encoding="utf-8")
    main = ModuleType("__main__")
    main.__file__ = str(page)
    monkeypatch.setitem(sys.modules, "__main__", main)
    def run(name, values):
        worker = RenderWorker(timeout=120)
        data = {"names":["Service","Infrastructure","Food Quality"], "values":values}
        job = {"run_id":name,"figure_id":"mfi_radar","kind":"radar","analytical_fingerprint":fingerprint(data),"data":data}
        try:
            result = worker.run(job)
            assert result["run_id"] == name and result["analytical_fingerprint"] == fingerprint(data)
            assert result["metadata"]["values"] == values
            return result["image"]
        finally:
            worker.close()
    with ThreadPoolExecutor(max_workers=2) as pool:
        benin = pool.submit(run,"Benin",[3.33,5.0,4.79])
        haiti = pool.submit(run,"Haiti",[7.0,2.0,8.0])
        assert benin.result() != haiti.result()
