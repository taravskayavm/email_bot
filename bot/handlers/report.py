import random

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from emailbot.notify import notify
from utils.email_clean import drop_leading_char_twins, parse_emails_unified


def build_examples(emails: list[str], k: int = 10) -> list[str]:
    """Return up to ``k`` unique examples in random order."""
    rng = random.SystemRandom()
    unique = list(dict.fromkeys(emails))
    if len(unique) <= k:
        return unique
    return rng.sample(unique, k)


async def send_report(update, context, extractor_result) -> None:
    """Send a summary of extracted e-mails to the user.

    Stores suspicious addresses (when the first letter might be missing) in
    ``context.user_data['emails_suspects']`` for further use.
    """
    cleaned, meta = parse_emails_unified(
        extractor_result.raw_text or " ", return_meta=True
    )
    cleaned = drop_leading_char_twins(cleaned)
    unique = sorted(set(cleaned))
    suspects = sorted(set(meta.get("suspects", [])))
    examples = build_examples(unique)

    lines = [
        "✅ Анализ завершён.",
        f"Найдено адресов: {len(unique)}",
        f"Уникальных (после очистки): {len(unique)}",
    ]
    kb = None
    if suspects:
        lines += [
            "",
            f"⚠️ Подозрительные (возможно, «съелась» первая буква): {len(suspects)}",
            *suspects[:5],
            "",
            "Вы можете принять их как есть или вручную исправить.",
        ]
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Принять подозрительные", callback_data="accept_suspects"
                    ),
                    InlineKeyboardButton(
                        "✍️ Исправить адреса", callback_data="edit_suspects"
                    ),
                ]
            ]
        )
    if examples:
        lines += ["", "🧪 Примеры:", *examples]

    if kb:
        await notify(update.message, "\n".join(lines), reply_markup=kb, event="report")
    else:
        await notify(update.message, "\n".join(lines), event="report")
    context.user_data["emails_suspects"] = suspects

def make_summary_message(
    stats: str, emails: list[str], suspects: list[str] | None = None
) -> str:
    """Compose a human-readable report based on stats and e-mail lists."""
    examples = build_examples(emails)
    blocks = [stats]
    if suspects:
        blocks.append(
            "⚠️ Подозрительные (возможно, «съелась» первая буква): "
            f"{len(suspects)}\n" + "\n".join(suspects[:5])
        )
    if examples:
        blocks.append("🧪 Примеры:\n" + "\n".join(examples))
    # удаляем второй дублирующийся блок примеров: формируем сообщение ровно один раз
    blocks.append("Дополнительные действия:")
    return "\n\n".join(blocks)
