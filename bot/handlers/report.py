import random
import time

def build_examples(emails: list[str], k: int = 10) -> list[str]:
    # крипто-рандом, чтобы телеграм-кэш и однаковый вход давали разные примеры
    rng = random.SystemRandom()
    n = min(k, len(emails))
    if n == 0:
        return []
    sample = rng.sample(emails, n)
    return sample

def make_summary_message(stats, emails: list[str]) -> str:
    examples = build_examples(emails)
    blocks = []
    blocks.append(stats)
    if examples:
        blocks.append("🧪 Примеры:\n" + "\n".join(examples))
    # удаляем второй дублирующийся блок примеров: формируем сообщение ровно один раз
    blocks.append("Дополнительные действия:")
    return "\n\n".join(blocks)
