"""
handlers.py  (bot)
==================
Handlers de mensagens do bot Telegram.
Estado de saudação persiste em bronze.telegram_users (via db.py).

Handlers registrados:
    - handle_start          → /start
    - handle_text_message   → mensagens de texto
    - handle_photo          → fotos / documentos de imagem
    - handle_unknown        → comandos desconhecidos
"""

import logging

import db
import image_store
import state
from telegram import ReplyParameters, Update
from telegram.constants import ChatAction, ParseMode
from telegram.ext import ContextTypes

log = logging.getLogger("bot.handlers")

# ---------------------------------------------------------------------------
# Mensagens
# ---------------------------------------------------------------------------

_MSG_WELCOME = """\
👋 *Olá, {first_name}\\! Seja bem\-vindo\(a\)\\!*

Eu sou o assistente de leitura de *Notas Fiscais Eletrônicas \(NF\-e\)*\.

📸 *Como usar:*
Envie uma foto da nota fiscal contendo o *QR Code* impresso no cupom e eu extrairei automaticamente os dados da compra para você\.

💡 *Dica:* Certifique\-se de que o QR Code esteja bem iluminado e centralizado na foto para melhor leitura\.

Pode enviar a foto quando quiser\!
"""

_MSG_ALREADY_GREETED = """\
📸 Para consultar uma nota fiscal, envie uma foto com o *QR Code* do cupom\.
"""

_MSG_PHOTO_RECEIVED = """\
✅ *Foto recebida com sucesso\!*

Seu QR Code está na fila de processamento\.
Assim que os dados da nota forem extraídos, enviarei o resultado aqui\.

⏳ _Aguarde um momento\.\.\._
"""

_MSG_PHOTO_ERROR = """\
❌ Não consegui salvar sua foto\. Tente enviar novamente\.
Se o problema persistir, entre em contato com o suporte\.
"""

_MSG_NO_TEXT = """\
📸 Para usar este bot, envie uma *foto* com o QR Code da sua nota fiscal\.
"""

_MSG_UNKNOWN_COMMAND = """\
❓ Comando não reconhecido\.

Use /start para ver as instruções ou envie diretamente uma foto com o QR Code\.
"""


# ---------------------------------------------------------------------------
# Utilitários
# ---------------------------------------------------------------------------


def _escape(text: str) -> str:
    """Escapa caracteres especiais do MarkdownV2."""
    special = r"\_*[]()~`>#+-=|{}.!"
    for ch in special:
        text = text.replace(ch, f"\\{ch}")
    return text


async def _ensure_user(update: Update) -> None:
    """Garante que o usuário existe em bronze.telegram_users e no cache local."""
    user = update.effective_user
    await db.upsert_telegram_user(
        user_id=user.id,
        username=user.username,
        full_name=user.full_name,
    )
    # Mantém cache local por compatibilidade
    state.register_user(
        user_id=user.id,
        username=user.username,
        full_name=user.full_name,
    )
    state.increment_messages(user.id)


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Responde ao /start com a mensagem de boas-vindas e (re)marca greeted."""
    await _ensure_user(update)
    user = update.effective_user

    log.info("/start de user_id=%d (%s)", user.id, user.full_name)

    await db.mark_greeted(user.id)
    state.mark_greeted(user.id)

    await update.message.reply_text(
        _MSG_WELCOME.format(first_name=_escape(user.first_name or "usuário")),
        parse_mode=ParseMode.MARKDOWN_V2,
    )


async def handle_text_message(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    Trata mensagens de texto:
      - Primeira mensagem → boas-vindas
      - Demais → lembrete de enviar foto
    """
    await _ensure_user(update)
    user = update.effective_user

    log.info("Texto de user_id=%d: %r", user.id, update.message.text[:80])

    greeted = await db.was_greeted(user.id)

    if not greeted:
        await db.mark_greeted(user.id)
        state.mark_greeted(user.id)
        await update.message.reply_text(
            _MSG_WELCOME.format(first_name=_escape(user.first_name or "usuário")),
            parse_mode=ParseMode.MARKDOWN_V2,
        )
    else:
        await update.message.reply_text(
            _MSG_ALREADY_GREETED,
            parse_mode=ParseMode.MARKDOWN_V2,
        )


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Trata mensagens com foto:
      1. Garante que o usuário foi saudado
      2. Salva a imagem + insere em bronze.received_images
      3. Publica image_id em queue:qr_parse
      4. Confirma recebimento em reply à foto
    """
    await _ensure_user(update)
    user = update.effective_user
    message = update.message

    log.info(
        "Foto de user_id=%d msg_id=%d | %d variações de tamanho",
        user.id,
        message.message_id,
        len(message.photo),
    )

    greeted = await db.was_greeted(user.id)
    if not greeted:
        await db.mark_greeted(user.id)
        state.mark_greeted(user.id)
        await message.reply_text(
            _MSG_WELCOME.format(first_name=_escape(user.first_name or "usuário")),
            parse_mode=ParseMode.MARKDOWN_V2,
        )

    await context.bot.send_chat_action(
        chat_id=message.chat_id,
        action=ChatAction.TYPING,
    )

    metadata = await image_store.save_photo(message)

    if metadata is None:
        log.error("Falha ao salvar foto msg=%d user=%d", message.message_id, user.id)
        await message.reply_text(
            _MSG_PHOTO_ERROR,
            parse_mode=ParseMode.MARKDOWN_V2,
            reply_parameters=ReplyParameters(message_id=message.message_id),
        )
        return

    state.increment_photos(user.id)

    await message.reply_text(
        _MSG_PHOTO_RECEIVED,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_parameters=ReplyParameters(message_id=message.message_id),
    )

    log.info(
        "Foto processada: image_id=%d filename=%s user=%d",
        metadata["image_id"],
        metadata["filename"],
        user.id,
    )


async def handle_unknown(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Responde a comandos não reconhecidos."""
    user = update.effective_user
    log.debug("Comando desconhecido de user_id=%d: %r", user.id, update.message.text)
    await update.message.reply_text(
        _MSG_UNKNOWN_COMMAND,
        parse_mode=ParseMode.MARKDOWN_V2,
    )
