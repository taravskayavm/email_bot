# Pre-commit: pre-commit run --all-files --config .pre-commit-config.local.yaml
python - <<'PY'
from emailbot.extraction import extract_emails_manual
s="name@uni.ru, user@mit.edu, 12345@lab.ru"
print(sorted(set(extract_emails_manual(s))))
PY
