import inspect
import emailbot.bot_handlers as bh

def _src(fn):
    try:
        return inspect.getsource(fn)
    except OSError:
        return ""

def test_no_stubs_connected():
    # функции должны вызывать extraction, а не возвращать пустые наборы
    src1 = _src(bh.extract_from_uploaded_file)
    src2 = _src(bh.extract_emails_from_zip)
    src3 = _src(bh.async_extract_emails_from_url)
    for s in (src1, src2, src3):
        assert "return set(), set()" not in s
        assert "return set()" not in s
        assert "pass" not in s
        assert ".extraction" in s or "extraction." in s
