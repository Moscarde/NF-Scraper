"""
state.py  (bot)
===============
Mantido por compatibilidade. Todas as operações de estado agora
delegam ao banco Postgres via db.py.

Para código legado que importe funções síncronas, este módulo provê
wrappers que executam as corrotinas assíncronas de db.py em um loop
de eventos gerenciado externamente.

Nas chamadas dentro dos handlers assíncronos, use diretamente db.py.
"""

import logging

log = logging.getLogger("bot.state")

# Mantido apenas para chamadas legadas síncronas fora do event loop.
# O dict _users ainda pode ser usado como cache local dentro do processo,
# mas a fonte de verdade é o Postgres.

_users: dict[int, dict] = {}


def register_user(
    user_id: int,
    username: str | None = None,
    full_name: str | None = None,
) -> None:
    """Cache local — apenas para manter compatibilidade."""
    if user_id not in _users:
        _users[user_id] = {
            "greeted": False,
            "username": username,
            "full_name": full_name,
            "message_count": 0,
            "photos_received": 0,
        }
        log.debug("Cache local: novo usuário id=%d", user_id)
    else:
        _users[user_id]["username"] = username
        _users[user_id]["full_name"] = full_name


def mark_greeted(user_id: int) -> None:
    if user_id in _users:
        _users[user_id]["greeted"] = True


def was_greeted(user_id: int) -> bool:
    return _users.get(user_id, {}).get("greeted", False)


def increment_messages(user_id: int) -> None:
    if user_id in _users:
        _users[user_id]["message_count"] += 1


def increment_photos(user_id: int) -> None:
    if user_id in _users:
        _users[user_id]["photos_received"] += 1


def get_user(user_id: int) -> dict | None:
    return _users.get(user_id)


def all_users() -> dict[int, dict]:
    return dict(_users)
