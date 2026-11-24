import smtplib
from unittest.mock import Mock

import pytest

from emailbot.smtp_client import RobustSMTP, SmtpClient


@pytest.mark.parametrize("error", [None, smtplib.SMTPServerDisconnected("closed"), OSError(10053, "aborted")])
def test_robust_close_releases_socket_and_can_be_repeated(error):
    client = RobustSMTP()
    server = Mock()
    server.quit.side_effect = error
    client._smtp = server
    client.close()
    client.close()
    server.quit.assert_called_once()
    if error is not None:
        server.close.assert_called_once()
    assert client._smtp is None


def test_context_cleanup_preserves_original_send_error():
    client = SmtpClient("localhost", 465, "user", "password")
    server = Mock()
    server.quit.side_effect = smtplib.SMTPServerDisconnected("closed")
    client._server = server
    original = ValueError("send failed")
    with pytest.raises(ValueError) as caught:
        try:
            raise original
        finally:
            client.__exit__(ValueError, original, None)
    assert caught.value is original
    server.close.assert_called_once()
    assert client._server is None
