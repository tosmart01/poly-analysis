from app.models import AnalysisRequest


def test_analysis_request_default_concurrency_is_5():
    req = AnalysisRequest(
        address="0xe00740bce98a594e26861838885ab310ec3b548c",
        start_ts=100,
        end_ts=200,
        symbols=["btc"],
        intervals=[5],
    )
    assert req.concurrency == 5
