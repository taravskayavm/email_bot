"""Smoke tests for basic name matching heuristics."""

from utils.name_match import fio_match_score


def test_fio_match_simple() -> None:
    score = fio_match_score("i.ivanov", "Ivan Ivanov")
    assert score >= 0.9


def test_fio_match_no_names() -> None:
    score = fio_match_score("service", "no names here")
    assert score == 0.0
