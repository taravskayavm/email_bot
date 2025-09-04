from emailbot.extraction_common import normalize_email

def test_normalize_email_gmail_variants():
    assert normalize_email('User.Name+tag@Gmail.com') == 'username@gmail.com'
    assert normalize_email('user.name@googlemail.com') == 'username@gmail.com'
