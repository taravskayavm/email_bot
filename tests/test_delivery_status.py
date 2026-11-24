from emailbot.delivery_status import failed_send_indices, unique_bounces


def sent(address="bad@example.com", ts="2026-11-06T08:10:00Z", mid="original-1"):
    return {"email": address, "ts": ts, "message_id": mid, "status": "success", "group": "sport"}


def failure(address="bad@example.com", original="2026-11-06T08:10:00Z", mid="original-1", **extra):
    return {"email": address, "status": "bounce", "ts": "2026-11-09T08:15:00Z",
            "original_sent_at": original, "message_id": mid, **extra}


def test_same_original_is_subtracted_once_for_multiple_dsns():
    rows = [failure(bounce_id="dsn-1"), failure(bounce_id="dsn-2")]
    assert len(unique_bounces(rows)) == 1
    assert failed_send_indices([sent()], rows) == {0}


def test_message_id_prevents_cancelling_a_successful_retry():
    sends = [sent(), sent(ts="2026-11-06T08:10:02Z", mid="retry-2")]
    assert failed_send_indices(sends, [failure()]) == {0}


def test_previous_day_failure_does_not_cancel_todays_send():
    sends = [sent(ts="2026-11-09T08:10:00Z", mid="")]
    assert failed_send_indices(sends, [failure()]) == set()


def test_idless_csv_copy_matches_same_send_but_other_recipient_does_not():
    sends = [sent(), sent(mid=""), sent(address="good@example.com")]
    assert failed_send_indices(sends, [failure()]) == {0, 1}


def test_gmail_variants_use_the_same_recipient_key():
    assert failed_send_indices([sent(address="first.last+tag@gmail.com")],
                               [failure(address="firstlast@gmail.com")]) == {0}


def test_old_records_without_original_metadata_use_latest_prior_send():
    sends = [sent(ts="2026-11-05T08:10:00Z", mid=""), sent(mid="")]
    assert failed_send_indices(sends, [failure(original="", mid="")]) == {1}


def test_positive_delivery_status_never_subtracts():
    assert failed_send_indices([sent()], [failure(dsn_status="2.0.0", dsn_action="delivered")]) == set()


def test_idless_retries_are_not_both_cancelled_by_one_failure():
    sends = [sent(mid=""), sent(ts="2026-11-06T08:10:03Z", mid="")]
    assert failed_send_indices(sends, [failure(mid="")]) == {0}


def test_equivalent_original_timestamps_deduplicate():
    rows = [failure(mid="", original="2026-11-06T08:10:00Z"),
            failure(mid="", original="2026-11-06T11:10:00+03:00")]
    assert len(unique_bounces(rows)) == 1
