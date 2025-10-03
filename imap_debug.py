import os, imaplib, traceback
from pathlib import Path
try:
    # если python-dotenv установлен, загрузим .env рядом со скриптом
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).parent/'.env')
except Exception:
    pass

IMAP = os.getenv('IMAP_HOST', 'imap.mail.ru')
PORT = int(os.getenv('IMAP_PORT') or 993)
USER = os.getenv('EMAIL_ADDRESS', '')
PWD  = os.getenv('EMAIL_PASSWORD', '')

print("IMAP", IMAP, PORT, "USER=", repr(USER))
try:
    imap = imaplib.IMAP4_SSL(IMAP, PORT)
    imap.debug = 4
    print("CONNECTED, trying LOGIN...")
    typ, data = imap.login(USER, PWD)
    print("LOGIN", typ, data)
    imap.logout()
except Exception:
    traceback.print_exc()
