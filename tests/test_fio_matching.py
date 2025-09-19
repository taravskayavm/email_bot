import pipelines.extract_emails as pipeline
from utils.name_match import fio_candidates, fio_match_score


def test_fio_match_score_full_name():
    text = "Автор: Сергей Иванов"
    pairs = fio_candidates(text)
    score = fio_match_score("sergey.ivanov", text, candidates=pairs)
    assert score >= 0.99


def test_fio_match_score_initials():
    text = "Иванов С. П."
    pairs = fio_candidates(text)
    score = fio_match_score("s_ivanov83", text, candidates=pairs)
    assert score >= 0.9


def test_pipeline_promotes_fio_match():
    text = "Сергей Иванов\nE-mail: s_ivanov83@example.com"
    emails, stats = pipeline.extract_emails_pipeline(text)
    addr = "s_ivanov83@example.com"
    assert addr in emails
    info = stats["classified"][addr]
    assert info["class"] == "personal"
    assert info["fio_score"] >= pipeline.FIO_PERSONAL_THRESHOLD
    assert "fio-match" in info.get("reason", "")
    assert stats["has_fio"] == 1
