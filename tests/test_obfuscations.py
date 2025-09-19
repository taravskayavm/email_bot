from pipelines.extract_emails import extract_emails_pipeline

SAMPLE = """
Author: Ivan Ivanov (ivan.ivanov@mail.ru), Corresponding author: nkichigina@mail.ru
Obfuscated: opek [at] mail [dot] ru ; tea_88(at)inbox(dot)ru
Noise: russia 1 elena - noskova - 2011 @ el6309 . spb . edu
Mixed: poccияalexdru9@mail.ru
Role: editor@journals.example.com; info@mysite.com
"""

def test_pipeline_russian_only_ru_com():
    emails, meta = extract_emails_pipeline(SAMPLE)
    # only allowed TLDs
    assert all(e.split("@", 1)[1].endswith((".ru", ".com")) for e in emails)
    # mixed-script must be dropped
    assert not any("pocc" in e for e in emails)
    # role-like filtered by default
    assert not any(e.startswith(("editor@", "info@")) for e in emails)
    # expected valid
    assert "ivan.ivanov@mail.ru" in emails
    assert "nkichigina@mail.ru" in emails
    assert "opek@mail.ru" in emails
    assert "tea_88@inbox.ru" in emails
