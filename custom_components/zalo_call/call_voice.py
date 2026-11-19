"""Giọng nói TRONG cuộc gọi Zalo: thông báo bằng TTS và AI nói chuyện (STT → trợ lý → TTS).

Dùng đúng Assist pipeline và TTS của HA nên STT/TTS/trợ lý là thứ người dùng đã cấu hình (không thêm dịch vụ ngoài):
  * ``async_say``: TTS → tệp âm thanh → app (POST /api/call/say) phát lên cuộc gọi qua đường mic.
  * ``async_run_ai``: nghe tiếng người gọi (luồng WAV của app /api/call/audio/out/<token>) → Assist pipeline
    (VAD + STT + trợ lý hội thoại) → TTS của pipeline → phát lại; lặp tới khi cuộc gọi hết, im lặng quá lâu hoặc quá giới hạn.
  * ``Plan``: kế hoạch cho một cuộc gọi (đọc thông báo, bật AI, cúp máy sau khi xong) — đăng ký lúc make_call/answer_call,
    chạy khi webhook báo «đã nghe máy». Cuộc gọi ĐẾN thì công tắc «AI trong cuộc gọi» tự tạo kế hoạch.
Không ghi nội dung hội thoại vào log (chỉ độ dài/số lượt).
"""
from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

import aiohttp
from homeassistant.core import Context, HomeAssistant, callback
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from . import caller_name, video_policy
from .api import ZaloCallApi, ZaloCallError
from .bridge_core import candidates, entity_of_label
from .const import CONF_VIDEO_CONSENT
from .rules import CONNECT_PARTS, SCOPES, get_rules, parse_connect, resolve as resolve_rule
from .styler import _speech

_LOGGER = logging.getLogger(__name__)

POLL_S = 0.4                 # nhịp hỏi app xem đã phát xong chưa
SAY_MAX_WAIT_S = 120.0       # chờ phát xong tối đa
TURN_TIMEOUT_S = 45.0        # một lượt nghe → hiểu (VAD cũng tự dừng sớm hơn)
MAX_CALL_S = 600.0           # AI nói chuyện tối đa 10 phút
MAX_TURNS = 30
IDLE_TURNS = 2               # im lặng/không nghe ra chữ liên tiếp ngần này lượt thì kết thúc
CHUNK_BYTES = 3200           # 100 ms PCM16 mono 16 kHz
WAV_HEADER = 44
QUEUE_MAX = 100              # ~10 s âm thanh chờ; đầy thì bỏ khúc cũ nhất (không để tụt hậu)
DEFAULT_GREETING = "Xin chào, tôi là trợ lý của gia đình. Bạn cần gì ạ?"
PHONE_HINT = ("Bạn đang nói chuyện qua điện thoại bằng giọng nói. Trả lời ngắn gọn, tự nhiên bằng lời nói: không dùng markdown, "
              "danh sách, ký hiệu hay emoji.")
HANDOVER_MARK = "[[CHUYEN_MAY]]"
# Mô hình hay làm rơi ngoặc vuông hoặc đổi "_" thành khoảng trắng (đo thật 11/11: "… ạ. CHUYEN MAY" → không bắt được, lại bị đọc to). Nên nhận: có ngoặc
# (không phân biệt hoa thường, 1-2 ngoặc) HOẶC không ngoặc nhưng VIẾT HOA nguyên cụm, đứng riêng (lời thường có dấu tiếng Việt không bao giờ khớp).
HANDOVER_RE = re.compile(r"\[{1,2}\s*(?i:CHUYEN[_\s]?MAY)(?:\s*[:=]\s*(?i:VOICE|CAMERA))?\s*\]{1,2}|(?<![A-Za-z])CHUYEN[_\s]?MAY(?:\s*[:=]\s*(?:VOICE|CAMERA))?(?![A-Za-z])")
# Kết thúc cuộc trò chuyện: người gọi nói không cần gì nữa/tạm biệt thì AI chào ngắn rồi kết thúc bằng dấu này; vòng AI dừng và cuộc gọi được cúp
# (đo thật 11/11: AI chào tạm biệt hai lần nhưng không có tín hiệu nên vòng AI vẫn chờ nghe, cuộc gọi mở mãi). Dung sai như dấu chuyển máy.
END_MARK = "[[KET_THUC]]"
END_RE = re.compile(r"\[{1,2}\s*(?i:KET[_\s]?THUC)\s*\]{1,2}|(?<![A-Za-z])KET[_\s]?THUC(?![A-Za-z])")
END_HINT = (" Khi người gọi nói không cần gì nữa, cảm ơn, hoặc chào tạm biệt, hãy chào lại thật ngắn rồi kết thúc câu trả lời bằng đúng dấu " + END_MARK +
            " (dấu này sẽ không được đọc ra); sau dấu đó cuộc gọi được cúp. Chưa xong việc thì KHÔNG dùng dấu này.")
# Chế độ connect "ai": AI kèm lựa chọn sau dấu (VOICE = chỉ loa+mic; CAMERA = nối cả camera); không kèm thì VOICE (kín hơn).
HANDOVER_CHOICE_RE = re.compile(r"CHUYEN[_\s]?MAY\s*[:=]\s*(VOICE|CAMERA)", re.IGNORECASE)
CONNECT_HINT = (" Khi chuyển máy hãy chọn cách nối: [[CHUYEN_MAY:VOICE]] nếu chỉ cần nói chuyện (nối loa và mic nhà), [[CHUYEN_MAY:CAMERA]] nếu cần "
                "xem hình ngay (khẩn cấp, có người ngã, cháy, người lạ trong nhà...). Chỉ nối được thiết bị đã được cài sẵn.")
HANDOVER_HINT = ("Khi người gọi muốn nói chuyện với người trong nhà, khi có việc khẩn cấp hoặc nguy hiểm, hoặc khi bạn không giúp "
                 "được, hãy nói ngắn gọn rằng bạn sẽ chuyển máy rồi kết thúc câu trả lời bằng đúng dấu " + HANDOVER_MARK +
                 " (dấu này sẽ không được đọc ra). Sau dấu đó cuộc gọi được nối trực tiếp với thiết bị trong nhà.")


PUBLIC_HINT = ("Người gọi CHƯA được xác thực: không tiết lộ bất kỳ thông tin nào về gia đình, thiết bị, lịch sinh hoạt hay vị trí; không điều "
               "khiển thiết bị. Chỉ chào hỏi, ghi nhận lời nhắn và chuyển máy khi cần.")


def phone_hint(handover_rule: str = "", trusted: bool = False, connect_ai: bool = False) -> str:
    """Gợi ý cho trợ lý: nói chuyện qua điện thoại + phạm vi (người chưa tin cậy) + khi nào tự chuyển sang trực tiếp.
    Đây chỉ là lớp MỀM; giới hạn thật nằm ở trợ lý được chọn cho cuộc gọi (không có quyền thiết bị) — xem rules.py."""
    rule = (" Điều kiện chuyển máy riêng: %s." % handover_rule.strip().rstrip(".")) if handover_rule and handover_rule.strip() else ""
    return PHONE_HINT + ("" if trusted else " " + PUBLIC_HINT) + " " + HANDOVER_HINT + (CONNECT_HINT if connect_ai else "") + END_HINT + rule


def pipeline_agent(hass: HomeAssistant, pipeline_id: str | None) -> str:
    """Trợ lý hội thoại (entity) của một Assist pipeline; "" nếu không lấy được."""
    from homeassistant.components.assist_pipeline import pipeline as pipeline_mod
    try:
        return str(pipeline_mod.async_get_pipeline(hass, pipeline_id).conversation_engine or "")
    except Exception:  # noqa: BLE001
        return ""


def pipeline_tts(hass: HomeAssistant, pipeline_id: str | None) -> tuple[str | None, str | None, dict | None]:
    """(thực thể TTS, ngôn ngữ, tuỳ chọn giọng) của một Assist pipeline (None = pipeline ưu tiên). Không lấy được thì (None, None, None)
    = TTS mặc định của HA."""
    from homeassistant.components.assist_pipeline import pipeline as pipeline_mod
    try:
        pl = pipeline_mod.async_get_pipeline(hass, pipeline_id)
    except Exception:  # noqa: BLE001 - không có pipeline: dùng TTS mặc định
        return None, None, None
    return pl.tts_engine, pl.tts_language, ({"voice": pl.tts_voice} if pl.tts_voice else None)


@dataclass
class Plan:
    """Việc cần làm khi cuộc gọi được nghe máy."""
    message: str = ""            # đọc (TTS) ngay khi nghe máy
    repeat: int = 1              # đọc mấy lần (1-3)
    ai: bool = False             # sau đó nghe và trả lời bằng AI
    hangup: bool = False         # xong thì cúp máy
    engine: str | None = None    # thực thể tts.* (trống = của pipeline / mặc định)
    language: str | None = None
    pipeline_id: str | None = None
    greeting: str = ""           # AI nói trước khi chưa có `message` (cuộc gọi đến)
    handover_rule: str = ""      # điều kiện riêng để AI tự chuyển sang trực tiếp (ngoài các điều kiện chung)
    agent_id: str = ""           # trợ lý hội thoại RIÊNG cho cuộc gọi (trống = trợ lý của pipeline)
    trusted: bool = False        # người gọi tin cậy (không dặn kiểu «chưa xác thực»)
    alert: bool = False          # cuộc gọi CẢNH BÁO: đọc `message` rồi nối thẳng thiết bị nhà, không hỏi, không trò chuyện
    video: bool = False          # cuộc gọi video: chuyển máy mới bật camera (thoại thì không)
    # Thiết bị nhà sẽ nối KHI CHUYỂN MÁY (speaker/mic/mic_url/camera/volume), chỉ gồm thứ ĐÃ CÀI trong dịch vụ. Trống = KHÔNG nối gì, kể cả khi AI muốn.
    connect: dict = field(default_factory=dict)
    thread_id: str = ""          # người nhận (có trong sự kiện «đọc xong» của cảnh báo)
    after: str = ""              # cảnh báo: sau khi đọc xong làm gì - hangup (cúp) / connect (nối thiết bị) / wait (giữ máy); trống = tự chọn
    connect_now: dict = field(default_factory=dict)   # thiết bị nối NGAY khi nghe máy (mặc định trống: mọi thứ chờ tới lúc người nhà nói trực tiếp)
    echo_guard: str = ""         # chống vọng của quy tắc (on | duplex | off; trống/auto = theo mặc định thiết bị)
    connect_ai: bool = False     # connect: ai - AI chọn voice/camera lúc chuyển máy (connect chứa TẤT CẢ thiết bị được phép)


@dataclass
class Claim:
    """Một quy tắc do automation áp cho MỘT cuộc gọi (dịch vụ apply_call_rule). rank = độ cụ thể của phạm vi (số nhỏ = cụ thể hơn)."""
    rule: dict[str, Any]
    rank: int
    source: str = ""         # automation đã áp (entity_id): chống áp lặp và nói rõ ai thắng trong nhật ký
    seq: int = 0             # thứ tự tới (cùng độ cụ thể thì cái tới trước làm chính)


def best_claims(claims: list[Claim]) -> list[Claim]:
    """Chỉ giữ các quy tắc CỤ THỂ NHẤT (cùng bậc): quy tắc theo người thay hẳn quy tắc theo nhóm danh bạ, như quy tắc lưu sẵn."""
    if not claims:
        return []
    top = min(c.rank for c in claims)
    return sorted((c for c in claims if c.rank == top), key=lambda c: c.seq)


def primary_claim(claims: list[Claim]) -> Claim | None:
    """Quy tắc LÀM CHÍNH (nghe máy, thiết bị, AI, xin video): quy tắc đầu tiên có `answer`, không có thì quy tắc đầu tiên. Việc báo ra loa thì mọi quy tắc đều làm."""
    top = best_claims(claims)
    return next((c for c in top if c.rule.get("answer")), top[0] if top else None)


@dataclass
class CallVoice:
    plans: dict[str, Plan] = field(default_factory=dict)
    incoming: set[str] = field(default_factory=set)
    answered_ids: set[str] = field(default_factory=set)          # call_id đã có người nghe (vòng báo ra loa không trả lại âm lượng cũ)
    announce_tasks: dict[str, asyncio.Task] = field(default_factory=dict)   # call_id -> vòng báo (và báo lại) ra loa; huỷ khi nghe máy/kết thúc
    announce_done: dict[str, asyncio.Event] = field(default_factory=dict)   # call_id -> đã đọc xong câu báo tên người gọi (tự nghe máy chờ tín hiệu này)
    announced: set[str] = field(default_factory=set)             # call_id đã phát thông báo «có cuộc gọi» ra loa (mỗi cuộc một lần)
    scheduled: set[str] = field(default_factory=set)            # call_id đã lên lịch nghe máy theo quy tắc (mỗi cuộc một lần)
    standby_sent: set[str] = field(default_factory=set)         # call_id đã đẩy «thiết bị chờ» của quy tắc xuống app (mỗi cuộc một lần)
    timers: dict[str, Any] = field(default_factory=dict)        # call_id -> hàm huỷ lịch
    tasks: dict[str, asyncio.Task] = field(default_factory=dict)
    ai_running: list[str] = field(default_factory=list)          # cuộc gọi đang có vòng AI (mới nhất ở cuối)
    handover: dict[str, str] = field(default_factory=dict)       # call_id -> lý do: AI xin chuyển sang trực tiếp (qua công cụ LLM)
    call_context_ids: set[str] = field(default_factory=set)      # id Context của các lượt AI trong cuộc gọi (để LLM API nhận ra)

    handover_connect: dict[str, str] = field(default_factory=dict)   # call_id -> "voice"|"camera": AI chọn cách nối (connect: ai)
    claims: dict[str, list[Claim]] = field(default_factory=dict)     # call_id -> quy tắc do automation áp cho cuộc này
    decided: dict[str, list[Claim]] = field(default_factory=dict)    # call_id -> quy tắc đã chốt lúc cuộc gọi đến (rỗng = không làm gì)
    rule_of: dict[str, dict[str, Any]] = field(default_factory=dict) # call_id -> quy tắc LÀM CHÍNH đã chốt (mọi chỗ đọc quy tắc của cuộc gọi dùng cái này)
    call_payload: dict[str, dict[str, Any]] = field(default_factory=dict)
    decide_timers: dict[str, Any] = field(default_factory=dict)
    decide_grace: float | None = None                                # None = DECIDE_GRACE
    claim_seq: int = 0

    def request_handover(self, reason: str = "", connect: str = "") -> bool:
        """Công cụ LLM `chuyen_sang_truc_tiep`: đánh dấu cuộc gọi AI mới nhất cần chuyển trực tiếp. False nếu không có AI nào đang giữ."""
        if not self.ai_running:
            return False
        self.handover[self.ai_running[-1]] = reason or "ai"
        if connect in ("voice", "camera"):
            self.handover_connect[self.ai_running[-1]] = connect
        return True

    def cancel_all(self) -> None:
        for task in list(self.tasks.values()):
            task.cancel()
        self.tasks.clear()
        for cancel in list(self.timers.values()) + list(self.decide_timers.values()):
            cancel()
        self.timers.clear()
        self.decide_timers.clear()


def plan_from_service(data: dict[str, Any]) -> Plan | None:
    """Kế hoạch từ trường của make_call/answer_call; None nếu không yêu cầu gì."""
    message = str(data.get("message") or "").strip()
    ai = bool(data.get("ai"))
    if not message and not ai:
        return None
    # Chỉ thông báo (không AI) thì mặc định cúp máy sau khi đọc; AI thì cúp khi cuộc trò chuyện xong. Người dùng ghi đè được.
    hangup = data["hangup_after"] if "hangup_after" in data else True
    return Plan(message=message, repeat=max(1, min(3, int(data.get("repeat") or 1))), ai=ai, hangup=bool(hangup),
                engine=data.get("tts_engine") or None, language=data.get("language") or None,
                pipeline_id=data.get("pipeline_id") or data.get("pipeline") or None, agent_id=data.get("agent") or "", greeting=str(data.get("greeting") or "").strip(),
                handover_rule=str(data.get("handover_rule") or "").strip(),
                video=str(data.get("type") or "") == "video")


# -- TTS và phát -----------------------------------------------------------------------------------------------------

async def async_tts_audio(hass: HomeAssistant, message: str, engine: str | None = None,
                          language: str | None = None, options: dict | None = None) -> tuple[str, bytes]:
    """(đuôi tệp, byte) của giọng đọc `message` bằng TTS của HA."""
    from homeassistant.components import tts      # nạp muộn: chỉ cần khi thật sự đọc

    media_id = tts.generate_media_source_id(hass, message, engine=engine or None, language=language or None,
                                           options=options, cache=False)
    return await tts.async_get_media_source_audio(hass, media_id)


async def _wait_playback_done(api: ZaloCallApi, call_id: str) -> None:
    deadline = time.monotonic() + SAY_MAX_WAIT_S
    await asyncio.sleep(POLL_S)
    while time.monotonic() < deadline:
        res = await api.media_control({"call_id": call_id, "action": "status", "target": "mic"})
        mic = (res or {}).get("mic")
        if not mic or not mic.get("alive"):
            return
        await asyncio.sleep(POLL_S)
    _LOGGER.warning("Phát giọng nói trong cuộc gọi %s quá %.0f s, bỏ chờ", call_id, SAY_MAX_WAIT_S)


async def _restore_mic(api: ZaloCallApi, call_id: str) -> None:
    """Đọc xong thì trả mic về nguồn mặc định (đường mic bị thay bằng tệp TTS trong lúc đọc). Không có mic mặc định thì thôi."""
    try:
        await api.media_control({"call_id": call_id, "action": "start", "target": "mic"})
    except ZaloCallError as err:
        _LOGGER.debug("Không trả được mic mặc định cho cuộc gọi %s: %s", call_id, err)


# Cuộc gọi AI/cảnh báo giữ máy: lúc nghe máy TẤT CẢ thiết bị nhà ĐỀU TẮT (ghi đè rỗng tường minh = không rơi về mặc định); chỉ nối khi chuyển máy.
HELD_DEVICES: dict[str, str] = {"speaker": "", "mic": "", "mic_url": "", "camera": ""}
_DEVICE_KEYS = ("speaker", "mic", "mic_url", "camera")


_PART_KEYS = {"mic": ("mic", "mic_url", "mic_gain"), "speaker": ("speaker", "volume", "speaker_gain"), "camera": ("camera",)}


def devices_for_mode(devices: dict[str, Any], mode: Any) -> dict[str, Any]:
    """Thiết bị sẽ nối theo `connect` (chuỗi hay danh sách, xem rules.parse_connect): chỉ những PHẦN được chọn (mic / speaker / camera, ghép bất kỳ),
    và chỉ thiết bị ĐÃ cài. ["ai"] = tập được phép là tất cả (lúc chuyển máy AI chọn voice hay camera trong đó). Không có gì để nối thì {}."""
    parts = [p for p in parse_connect(mode) if p != "ai"]
    out = {k: devices[k] for p in parts for k in _PART_KEYS[p] if devices.get(k) not in (None, "")}
    return out if any(k in out for k in _DEVICE_KEYS) else {}


def rule_devices(rule: dict[str, Any]) -> dict[str, Any]:
    """Thiết bị nhà do QUY TẮC cài, lọc theo `connect` của quy tắc (mặc định never). Chỉ trường ĐÃ chọn, không rơi về mặc định."""
    out: dict[str, Any] = {k: rule[k] for k in _DEVICE_KEYS if rule.get(k)}
    out.update({k: rule[k] for k in ("volume", "mic_gain", "speaker_gain") if out and rule.get(k)})
    return devices_for_mode(out, rule.get("connect"))


def rule_devices_now(rule: dict[str, Any]) -> dict[str, Any]:
    """Thiết bị quy tắc nối NGAY khi nghe máy (ô `connect_now`), lọc từ các thiết bị đã chọn trong quy tắc."""
    src: dict[str, Any] = {k: rule[k] for k in _DEVICE_KEYS if rule.get(k)}
    src.update({k: rule[k] for k in ("volume", "mic_gain", "speaker_gain") if src and rule.get(k)})
    return devices_for_mode(src, rule.get("connect_now"))


def explicit_devices(connect: dict[str, Any]) -> dict[str, Any]:
    """Ghi đè đủ bốn nhóm thiết bị: nhóm không cài = "" (tắt tường minh), không rơi về mặc định."""
    return {**{k: "" for k in _DEVICE_KEYS}, **{k: v for k, v in connect.items() if v not in (None, "")}}


async def async_connect_devices(rt: Any, call_id: str, connect: dict[str, Any], video: bool = False, already: dict[str, Any] | None = None,
                                echo_mode: str | None = None) -> bool:
    """Nối thiết bị nhà vào cuộc gọi đang nói (khi chuyển máy / cảnh báo). Chỉ nối thứ đã cài; connect trống thì KHÔNG nối gì. `already` = thiết bị đã
    nối từ lúc nghe máy (connect_now): không nối lại (khỏi giật). Trả về True nếu sau bước này có thiết bị đang nối."""
    already = already or {}
    if not connect and not already:
        return False
    done = {p for p, keys in _PART_KEYS.items() if any(k in already for k in keys)}
    new = {k: v for p, keys in _PART_KEYS.items() if p not in done for k in keys if (v := connect.get(k)) not in (None, "")}
    if not new:
        return bool(already)                              # mọi thứ cần nối đã nối sẵn từ lúc nghe máy
    override = explicit_devices(new)
    camera = override.get("camera", "")
    if not video:
        override["camera"] = ""                           # cuộc thoại: camera chỉ vào qua «nâng lên video» bên dưới (có tích camera)
    await rt.bridge.reconnect(call_id, override)           # loa kiểu media_player (play_media) + âm lượng
    fields = await rt.bridge.async_call_fields(override, echo_mode)    # mic_url/mic_format/camera_url/speaker_url/echo_guard; nhóm không nối thêm = "off"
    res = await rt.api.media_control({"call_id": call_id, "action": "start", "target": "both", **fields})
    if res and res.get("ok") is False:
        raise ZaloCallError("App không nối được thiết bị nhà: %s" % res.get("error", "lỗi"))
    if camera and not video:
        cam = await rt.bridge.async_call_fields({"camera": camera})
        up = await rt.api.upgrade_video({"call_id": call_id, **({"camera_url": cam["camera_url"]} if cam.get("camera_url") else {})})
        if up and up.get("ok") is False:
            _LOGGER.warning("Cuộc gọi %s: nối được loa/mic nhưng không nâng lên video: %s", call_id, up.get("error", "lỗi"))
    return True


async def async_say(hass: HomeAssistant, api: ZaloCallApi, call_id: str, message: str,
                    engine: str | None = None, language: str | None = None, wait: bool = True,
                    options: dict | None = None, restore_mic: bool = False) -> None:
    """Đọc `message` lên cuộc gọi đang nói. wait: chờ đọc xong mới trả về. restore_mic: rồi trả mic về mặc định."""
    message = (message or "").strip()
    if not message:
        return
    ext, data = await async_tts_audio(hass, message, engine, language, options)
    res = await api.call_say(call_id, "tts." + (ext or "mp3"), data)
    if not (res or {}).get("ok"):
        raise ZaloCallError("Không phát được giọng nói: %s" % (res or {}).get("error", "lỗi"))
    if wait:
        await _wait_playback_done(api, call_id)
        await asyncio.sleep(0.3)                 # khung cuối còn trong hàng đợi gửi
        if restore_mic:
            await _restore_mic(api, call_id)


async def _call_status(api: ZaloCallApi, call_id: str) -> str | None:
    for c in (await api.calls() or {}).get("calls", []):
        if str(c.get("call_id")) == str(call_id):
            return c.get("status")
    return None


# -- AI nói chuyện ---------------------------------------------------------------------------------------------------

class _Listener:
    """Đọc luồng tiếng người gọi từ app vào hàng đợi PCM; tạm bỏ tiếng khi đang đọc (tránh nghe lại chính mình)."""

    def __init__(self, hass: HomeAssistant, url: str) -> None:
        self._hass, self._url = hass, url
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(QUEUE_MAX)
        self.muted = False
        self.ended = False
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._pump())

    async def _pump(self) -> None:
        try:
            async with async_get_clientsession(self._hass).get(self._url, timeout=aiohttp.ClientTimeout(total=None, sock_read=30)) as r:
                if r.status != 200:
                    _LOGGER.warning("Không mở được luồng tiếng người gọi (HTTP %s)", r.status)
                    return
                first = True
                async for chunk in r.content.iter_chunked(CHUNK_BYTES):
                    if first:
                        first = False
                        if chunk[:4] == b"RIFF":
                            chunk = chunk[WAV_HEADER:]
                    if self.muted or not chunk:
                        continue
                    if self.queue.full():
                        self.queue.get_nowait()
                    self.queue.put_nowait(chunk)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            pass
        finally:
            self.ended = True

    def drain(self) -> None:
        while not self.queue.empty():
            self.queue.get_nowait()

    async def chunks(self) -> AsyncIterator[bytes]:
        while not self.ended or not self.queue.empty():
            try:
                yield await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if self.ended:
                    return

    def stop(self) -> None:
        if self._task:
            self._task.cancel()


async def _one_turn(hass: HomeAssistant, listener: _Listener, pipeline_id: str | None, language: str,
                    conversation_id: str | None, hint: str = PHONE_HINT,
                    context: Context | None = None, agent_id: str = "") -> tuple[str, str, str | None]:
    """Nghe một câu → (chữ người gọi nói, câu trả lời của trợ lý, conversation_id). Không nghe ra chữ thì chữ = ""."""
    from homeassistant.components import assist_pipeline, stt          # nạp muộn: chỉ cần khi bật AI trong cuộc gọi
    from homeassistant.components.assist_pipeline import pipeline as pipeline_mod

    got: dict[str, Any] = {"text": "", "reply": "", "conv": conversation_id}

    def on_event(event: Any) -> None:
        data = event.data or {}
        if event.type == pipeline_mod.PipelineEventType.STT_END:
            got["text"] = str((data.get("stt_output") or {}).get("text") or "").strip()
        elif event.type == pipeline_mod.PipelineEventType.INTENT_END:
            out = data.get("intent_output") or {}
            got["conv"] = out.get("conversation_id") or got["conv"]
            got["reply"] = str((((out.get("response") or {}).get("speech") or {}).get("plain") or {}).get("speech") or "").strip()
        elif event.type == pipeline_mod.PipelineEventType.ERROR:
            _LOGGER.debug("Assist pipeline trong cuộc gọi: %s", (data or {}).get("code"))

    meta = stt.SpeechMetadata(language=language, format=stt.AudioFormats.WAV, codec=stt.AudioCodecs.PCM,
                              bit_rate=stt.AudioBitRates.BITRATE_16, sample_rate=stt.AudioSampleRates.SAMPLERATE_16000,
                              channel=stt.AudioChannels.CHANNEL_MONO)
    stream = listener.chunks()
    extra: dict[str, Any] = {}
    if "conversation_extra_system_prompt" in inspect.signature(assist_pipeline.async_pipeline_from_audio_stream).parameters:
        extra["conversation_extra_system_prompt"] = hint      # HA cũ không có tham số này thì bỏ qua
    try:
        async with asyncio.timeout(TURN_TIMEOUT_S):
            await assist_pipeline.async_pipeline_from_audio_stream(
                hass, context=context or Context(), event_callback=on_event, stt_metadata=meta, stt_stream=stream,
                pipeline_id=pipeline_id, conversation_id=conversation_id,
                # Có trợ lý riêng: pipeline chỉ làm STT, câu hỏi đi thẳng tới TRỢ LÝ ĐÓ (quyền của nó mới là giới hạn thật).
                end_stage=pipeline_mod.PipelineStage.STT if agent_id else pipeline_mod.PipelineStage.INTENT,
                audio_settings=pipeline_mod.AudioSettings(is_vad_enabled=True, silence_seconds=0.8), **extra)
    except (asyncio.TimeoutError, TimeoutError):
        pass
    finally:
        await stream.aclose()
    if agent_id and got["text"]:
        resp = await hass.services.async_call(
            "conversation", "process",
            {"text": "%s\n\n(%s)" % (got["text"], hint), "language": language, "agent_id": agent_id,
             **({"conversation_id": got["conv"]} if got["conv"] else {})},
            blocking=True, return_response=True, context=context or Context())
        got["reply"] = _speech(resp)
        got["conv"] = (resp or {}).get("conversation_id") or got["conv"]
    return got["text"], got["reply"], got["conv"]


async def async_run_ai(hass: HomeAssistant, api: ZaloCallApi, call_id: str, plan: Plan,
                       voice: CallVoice | None = None) -> dict[str, Any]:
    """Vòng AI nói chuyện cho tới khi cuộc gọi hết / im lặng / quá giới hạn / AI tự chuyển sang trực tiếp.
    -> {"handover": bool, "heard": câu cuối người gọi nói}."""
    from homeassistant.components.assist_pipeline import pipeline as pipeline_mod

    pipeline = pipeline_mod.async_get_pipeline(hass, plan.pipeline_id)
    language = plan.language or pipeline.stt_language or hass.config.language
    tts_engine = plan.engine or pipeline.tts_engine
    tts_language = plan.language or pipeline.tts_language
    tts_options = {"voice": pipeline.tts_voice} if pipeline.tts_voice else None
    media = await api.call_media_status(call_id)
    token = (media or {}).get("audio_token")
    if not token:
        raise ZaloCallError("App chưa có luồng tiếng của cuộc gọi %s" % call_id)
    listener = _Listener(hass, "%s/api/call/audio/tap/%s" % (api.url, token))
    listener.start()
    start, conv, idle, turns = time.monotonic(), None, 0, 0
    hint, handover, heard, choice = phone_hint(plan.handover_rule, plan.trusted, plan.connect_ai), False, "", ""
    finished = False
    context = Context()                           # lượt AI trong cuộc gọi có Context riêng: LLM API nhận ra để chỉ cho công cụ chuyển máy
    if voice is not None:
        voice.call_context_ids.add(context.id)
        voice.ai_running.append(call_id)
    try:
        if plan.greeting:
            listener.muted = True
            await async_say(hass, api, call_id, plan.greeting, tts_engine, tts_language, options=tts_options)
            listener.drain()
            listener.muted = False
        while turns < MAX_TURNS and time.monotonic() - start < MAX_CALL_S and not listener.ended:
            if await _call_status(api, call_id) != "answered":
                break
            text, reply, conv = await _one_turn(hass, listener, plan.pipeline_id, language, conv, hint, context, plan.agent_id)
            turns += 1
            if not text:
                idle += 1
                if idle >= IDLE_TURNS:
                    break
                continue
            idle = 0
            heard = text
            if HANDOVER_RE.search(reply):                 # AI tự quyết chuyển sang trực tiếp: đọc nốt câu (không có dấu) rồi dừng
                m = HANDOVER_CHOICE_RE.search(reply)
                choice = m.group(1).lower() if m else ""
                reply, handover = HANDOVER_RE.sub("", reply).strip(), True
            if voice is not None and call_id in voice.handover:      # hoặc AI gọi công cụ chuyen_sang_truc_tiep
                handover = True
                choice = choice or voice.handover_connect.get(call_id, "")
            if END_RE.search(reply):                      # AI báo xong cuộc trò chuyện: đọc nốt lời chào (không có dấu) rồi dừng
                reply, finished = END_RE.sub("", reply).strip(), not handover
            if reply:
                listener.muted = True
                try:
                    await async_say(hass, api, call_id, reply, tts_engine, tts_language, options=tts_options)
                finally:
                    listener.drain()
                    listener.muted = False
            if handover or finished:
                break
        _LOGGER.debug("AI trong cuộc gọi %s xong sau %d lượt (chuyển máy: %s, AI báo xong: %s)", call_id, turns, handover, finished)
        return {"handover": handover, "heard": heard, "connect": choice, "finished": finished}
    finally:
        listener.stop()
        if voice is not None:
            voice.call_context_ids.discard(context.id)
            if call_id in voice.ai_running:
                voice.ai_running.remove(call_id)
            voice.handover.pop(call_id, None)
            voice.handover_connect.pop(call_id, None)


# -- Vòng đời: chạy kế hoạch khi cuộc gọi được nghe máy ---------------------------------------------------------------

async def _finish_alert(hass: HomeAssistant, rt: Any, call_id: str, plan: Plan) -> None:
    """Cảnh báo đã đọc xong: làm theo `after` rồi báo HA (`zalo_call_alert_done`) để automation biết đúng lúc đó.
      hangup  = cúp máy;  connect = nối thiết bị đã cài (không có thiết bị nào thì cúp);  wait = giữ máy, người nhận hoặc automation tự cúp.
    after trống: có thiết bị để nối thì connect, không thì hangup (báo mất điện... đọc xong là cúp)."""
    after = plan.after or ("connect" if plan.connect else "hangup")
    if after == "connect" and not plan.connect:
        after = "hangup"
    connected = False
    if after == "connect":
        connected = await async_connect_devices(rt, call_id, plan.connect, plan.video, plan.connect_now, plan.echo_guard)
    elif after == "hangup":
        await rt.api.call_end(call_id)
    camera = bool(connected and plan.connect.get("camera"))
    hass.bus.async_fire("zalo_call_alert_done", {"call_id": call_id, "thread_id": plan.thread_id, "after": after, "connected": connected,
                                                  "camera": camera, "ended": after == "hangup"})
    if connected:                                       # tương thích: automation đang nghe chuyển máy vẫn nhận (reason alert)
        hass.bus.async_fire("zalo_call_ai_handover", {"call_id": call_id, "heard": "", "reason": "alert", "connected": True, "camera": camera})


# -- Thông báo «có cuộc gọi đến» ra loa (bật/tắt theo quy tắc, mặc định TẮT) ------------------------------------------

DEFAULT_ANNOUNCE = "Có cuộc gọi {loai} từ {ten}."


class _Safe(dict):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def announce_text(rule: dict[str, Any], payload: dict[str, Any]) -> str:
    """Câu thông báo: {loai} = thoại/video, {ten} = tên trong quy tắc, không thì tên Zalo của người gọi, không có thì «người lạ»/«một người»."""
    kind = "video" if str(payload.get("call_type_raw")) == "1" else "thoại"
    name = caller_name.spoken_name(rule, payload)             # không bao giờ là dãy số (peer là mã Zalo khi sự kiện không có tên)
    return str(rule.get("announce_text") or DEFAULT_ANNOUNCE).format_map(_Safe(loai=kind, ten=name))


async def _wait_player_idle(hass: HomeAssistant, entity_id: str, start_timeout: float = 3.0, end_timeout: float = 45.0) -> None:
    """Chờ loa đọc xong: đợi nó bắt đầu phát (tối đa start_timeout; tin ngắn có thể đã xong) rồi đợi hết phát (tối đa end_timeout)."""
    loop = asyncio.get_running_loop()
    busy = ("playing", "buffering")
    t = loop.time()
    while loop.time() - t < start_timeout and getattr(hass.states.get(entity_id), "state", None) not in busy:
        await asyncio.sleep(POLL_S)
    t = loop.time()
    while loop.time() - t < end_timeout and getattr(hass.states.get(entity_id), "state", None) in busy:
        await asyncio.sleep(POLL_S)


def announce_targets(rule: dict[str, Any]) -> list[str]:
    """Các loa báo cuộc gọi đến: `announce_to` có thể là NHIỀU loa cách nhau dấu phẩy (nhà có nhiều loa R1): báo ra TẤT CẢ."""
    return [entity_of_label(c) for c in candidates(rule.get("announce_to"))]


def rule_for_call(rt: Any, payload: dict[str, Any]) -> dict[str, Any] | None:
    """Quy tắc của MỘT cuộc gọi — chỗ DUY NHẤT trả lời câu «cuộc này làm theo quy tắc nào». Thứ tự: quy tắc đã chốt lúc cuộc đến → quy tắc automation đã áp
    (apply_call_rule) → quy tắc lưu sẵn (set_rule, bản dự phòng cho cách dùng cũ). Có quy tắc từ automation thì quy tắc lưu sẵn KHÔNG được áp
    (nếu không một cuộc gọi bị xử lý hai lần)."""
    voice: CallVoice = rt.voice
    cid = str(payload.get("call_id"))
    chosen = voice.rule_of.get(cid)
    if chosen is not None:
        return chosen
    main = primary_claim(voice.claims.get(cid, []))
    if main is not None:
        return main.rule
    return resolve_rule(get_rules(rt.defaults), payload.get("thread_id"), "user", payload.get("contact_status"))


async def _announce_once(hass: HomeAssistant, rule: dict[str, Any], payload: dict[str, Any]) -> bool:
    engine, language, options = pipeline_tts(hass, rule.get("pipeline") or None)
    if not engine:
        _LOGGER.warning("Thông báo cuộc gọi đến: chưa có giọng đọc (TTS) - chọn pipeline trong quy tắc")
        return False
    targets = announce_targets(rule)
    data: dict[str, Any] = {"entity_id": engine, "media_player_entity_id": targets if len(targets) > 1 else (targets[0] if targets else rule["announce_to"]),
                            "message": announce_text(rule, payload)}
    if language:
        data["language"] = language
    if options:
        data["options"] = options
    try:
        await hass.services.async_call("tts", "speak", data, blocking=False)
    except Exception:  # noqa: BLE001 - lỗi loa/TTS không được làm hỏng việc nghe máy
        _LOGGER.warning("Không phát được thông báo cuộc gọi đến ra %s", rule["announce_to"], exc_info=True)
        return False
    return True


DECIDE_GRACE = 1.0              # giây chờ các automation «áp quy tắc» của cuộc gọi đến báo về trước khi quyết (xem _decide); 0 = quyết ngay
ANNOUNCE_WAIT_MAX = 10.0         # giây tối đa tự nghe máy chờ câu báo tên người gọi đọc xong (loa/TTS hỏng thì không chờ mãi)
MAX_ANNOUNCE_LOOPS = 20          # chốt an toàn: một cuộc gọi không bị báo quá chừng này lần


async def _announce_incoming(hass: HomeAssistant, rt: Any, payload: dict[str, Any], ev: asyncio.Event | None, rule: dict[str, Any] | None = None) -> bool:
    """Cuộc gọi ĐẾN: nếu quy tắc của người đó bật `announce` (và chọn loa `announce_to`) thì đọc thông báo ra loa đó, để người trong nhà biết ai
    gọi. Có `announce_repeat` (giây) thì đọc xong chờ chừng đó rồi đọc lại, tới khi cuộc gọi được nghe hoặc kết thúc (vòng này bị huỷ lúc đó).
    Không có quy tắc/không bật = im lặng. Giọng đọc = của pipeline trong quy tắc (không có thì pipeline ưu tiên, chưa có thì không đọc).
    Trả về True nếu đã gửi lệnh phát."""
    rule = rule if rule is not None else rule_for_call(rt, payload)
    if not rule or not rule.get("announce") or not rule.get("announce_to"):
        return False
    try:                                                          # tên ĐÃ LƯU của người gọi (sự kiện cuộc gọi đến thường không có tên, chỉ có mã)
        contacts = await asyncio.wait_for(rt.api.contacts(), 4)
    except Exception:  # noqa: BLE001 - app chưa lên / chậm: báo bằng tên có sẵn, không làm hỏng việc báo
        contacts = None
    payload = dict(payload, contact_name=caller_name.find_contact_name(contacts, payload.get("thread_id")))
    cid = str(payload.get("call_id"))
    players = announce_targets(rule)
    old_levels: dict[str, Any] = {}
    level = int(rule.get("announce_volume") or 0)
    if level > 0:                                                 # đặt âm lượng riêng cho tiếng báo (nhớ mức cũ từng loa để trả lại nếu không ai nghe)
        for player in players:
            prev = getattr(hass.states.get(player), "attributes", {}).get("volume_level")
            try:
                await hass.services.async_call("media_player", "volume_set", {"entity_id": player, "volume_level": level / 100}, blocking=True)
                if prev is not None:
                    old_levels[player] = prev
            except Exception:  # noqa: BLE001 - loa không đặt được âm lượng thì vẫn báo
                _LOGGER.warning("Không đặt được âm lượng báo cuộc gọi cho %s", player, exc_info=True)

    async def _wait_all() -> None:
        await asyncio.gather(*(_wait_player_idle(hass, p) for p in players))
    try:
        if not await _announce_once(hass, rule, payload):
            return False

        async def _mark_first_done() -> None:                     # tự nghe máy chỉ chờ câu ĐẦU, không chờ các lần báo lại
            try:
                await _wait_all()
            finally:
                if ev is not None:
                    ev.set()
        hass.async_create_task(_mark_first_done())
        gap = int(rule.get("announce_repeat") or 0)
        for _ in range(MAX_ANNOUNCE_LOOPS if gap > 0 else 0):
            await _wait_all()
            await asyncio.sleep(gap)
            if cid not in rt.voice.incoming:
                break
            if not await _announce_once(hass, rule, payload):
                break
        else:
            if gap <= 0:
                await _wait_all()                                 # báo một lần: chờ đọc xong rồi mới trả âm lượng
        return True
    finally:
        if old_levels and cid not in rt.voice.answered_ids:        # không ai nghe: trả các loa về mức cũ (nghe rồi thì cuộc gọi tự đặt âm lượng)
            for player, prev in old_levels.items():
                with contextlib.suppress(Exception):
                    await hass.services.async_call("media_player", "volume_set", {"entity_id": player, "volume_level": prev}, blocking=True)


async def async_announce_incoming(hass: HomeAssistant, rt: Any, payload: dict[str, Any], rules: list[dict[str, Any] | None] | None = None) -> bool:
    """Báo tên người gọi ra loa (xem _announce_incoming). `rules` = MỌI quy tắc cùng bậc khớp cuộc gọi (mỗi automation báo ra loa của riêng nó);
    không nêu thì lấy quy tắc của cuộc gọi. Luôn đánh dấu «xong» khi thoát (không báo, lỗi, huỷ) để tự nghe máy không chờ vô ích."""
    ev = rt.voice.announce_done.get(str(payload.get("call_id")))
    try:
        todo = [r for r in (rules if rules is not None else [rule_for_call(rt, payload)]) if r]
        results = await asyncio.gather(*(_announce_incoming(hass, rt, payload, ev, r) for r in todo))
        return any(results)
    finally:
        if ev is not None:
            ev.set()


async def _run_plan(hass: HomeAssistant, rt: Any, call_id: str, plan: Plan) -> None:
    api: ZaloCallApi = rt.api
    try:
        if plan.connect_now.get("camera") and not plan.video:       # nối cam ngay mà cuộc gọi đang là thoại: nâng lên video luôn
            cam = await rt.bridge.async_call_fields({"camera": plan.connect_now["camera"]})
            await api.upgrade_video({"call_id": call_id, **({"camera_url": cam["camera_url"]} if cam.get("camera_url") else {})})
        engine, language, options = plan.engine, plan.language, None
        if plan.pipeline_id and not engine:            # đã chọn thiết bị Assist/pipeline: đọc thông báo bằng đúng giọng của nó
            engine, tts_lang, options = pipeline_tts(hass, plan.pipeline_id)
            language = language or tts_lang
        for i in range(plan.repeat if plan.message else 0):
            await async_say(hass, api, call_id, plan.message, engine, language, options=options)
            if i + 1 < plan.repeat:
                await asyncio.sleep(1.0)
        if plan.alert:
            await _finish_alert(hass, rt, call_id, plan)      # cảnh báo: đọc xong theo `after` (cúp / nối thiết bị / giữ máy), không hỏi
            return
        handed, reason, heard, choice = False, "", "", ""
        if plan.ai:
            res = await async_run_ai(hass, api, call_id, plan, rt.voice)
            handed, reason, heard, choice = bool(res.get("handover")), "ai", res.get("heard", ""), res.get("connect", "")
        if handed:
            # Chuyển trực tiếp: KHÔNG cúp máy, nối thiết bị đã cài (không cài = không nối), báo HA để automation xử lý.
            connect = plan.connect
            if plan.connect_ai:                              # AI chọn: CAMERA = cả camera, còn lại (hoặc không chọn) = chỉ loa + mic
                connect = devices_for_mode(plan.connect, "always" if choice == "camera" else "voice")
            connected = await async_connect_devices(rt, call_id, connect, plan.video, plan.connect_now, plan.echo_guard)
            hass.bus.async_fire("zalo_call_ai_handover", {"call_id": call_id, "heard": heard, "reason": reason, "connected": connected,
                                                          "camera": bool(connected and connect.get("camera"))})
        elif plan.hangup:
            await api.call_end(call_id)
        elif not plan.ai:
            await _restore_mic(api, call_id)
    except asyncio.CancelledError:
        raise
    except ZaloCallError as err:
        _LOGGER.warning("Giọng nói trong cuộc gọi %s: %s", call_id, err)
    except Exception:  # noqa: BLE001 - lỗi TTS/pipeline không được làm sập webhook
        _LOGGER.exception("Giọng nói trong cuộc gọi %s lỗi", call_id)
    finally:
        rt.voice.tasks.pop(call_id, None)


async def async_accept_video(rt: Any, call_id: Any, camera: str = "") -> bool:
    """Đồng ý lời xin chuyển cuộc thoại sang video của người gọi. camera (thực thể) trống = chỉ đồng ý, KHÔNG gửi hình trong nhà."""
    body: dict[str, Any] = {"call_id": call_id, "action": "start", "target": "accept_video"}
    if camera:
        fields = await rt.bridge.async_call_fields({"camera": camera})
        if fields.get("camera_url") and fields["camera_url"] != "off":
            body["camera_url"] = fields["camera_url"]
    res = await rt.api.media_control(body)
    if res and res.get("ok") is False:
        raise ZaloCallError("App không đồng ý được lời xin video: %s" % res.get("error", "lỗi"))
    return True


async def async_handle_video_request(hass: HomeAssistant, rt: Any, payload: dict[str, Any]) -> bool:
    """Người gọi xin chuyển sang video: làm theo `video_request` của quy tắc (ask = chỉ báo sự kiện, accept, accept_camera). Không có quy tắc = KHÔNG
    làm gì (người gọi chỉ thấy không ai đồng ý). Lỗi không được làm hỏng webhook."""
    rule = rule_for_call(rt, payload)
    mode = video_policy.decide(rule, rt.defaults.get(CONF_VIDEO_CONSENT))      # công tắc tổng TẮT (mặc định) = mọi người chỉ bị báo
    if mode == "ask":
        return False
    try:
        return await async_accept_video(rt, payload.get("call_id"), rule.get("camera") or "" if mode == "accept_camera" else "")
    except ZaloCallError as err:
        _LOGGER.warning("Đồng ý chuyển sang video lỗi: %s", err)
        return False


async def async_take_over(rt: Any, call_id: str | None = None) -> dict[str, Any]:
    """Người thật tiếp quản cuộc gọi đang do AI/thông báo giữ: dừng kế hoạch giọng nói, trả mic về nguồn mặc định (loa/mic nhà
    đã nối từ lúc nghe máy). call_id trống = cuộc gọi đang nói. Không có AI đang chạy thì cuộc gọi vốn đã trực tiếp."""
    api: ZaloCallApi = rt.api
    if call_id in (None, ""):
        active = (await api.calls() or {}).get("active")
        if not active or active.get("status") != "answered":
            raise ZaloCallError("Không có cuộc gọi nào đang nói")
        call_id = active["call_id"]
    cid = str(call_id)
    plan = rt.voice.plans.pop(cid, None)
    task = rt.voice.tasks.pop(cid, None)
    was_ai = task is not None
    if task is not None:
        task.cancel()
    manual = devices_for_mode(plan.connect, "voice") if plan is not None and plan.connect_ai else (plan.connect if plan is not None else {})
    if not (plan is not None and await async_connect_devices(rt, cid, manual, plan.video, plan.connect_now, plan.echo_guard)):
        await _restore_mic(api, cid)                  # không có thiết bị cài sẵn cho cuộc này: người bấm tiếp quản thì dùng mic mặc định
    return {"call_id": cid, "ai_was_running": was_ai}


def _devices_override(rule: dict[str, Any]) -> dict[str, Any]:
    """Thiết bị nhà lúc NGHE MÁY của cuộc gọi do quy tắc. AI giữ máy: tất cả TẮT (chỉ nối khi chuyển máy). Không AI: chỉ thiết bị
    quy tắc đã cài theo `connect`, không rơi về mặc định; không cài = tắt hết."""
    if rule.get("ai"):                                   # AI giữ máy: chỉ nối ngay thứ quy tắc cho phép nối ngay (connect_now), còn lại chờ
        now = rule_devices_now(rule)
        return explicit_devices(now) if now else dict(HELD_DEVICES)
    connect = rule_devices({**rule, "connect": "voice"} if "ai" in parse_connect(rule.get("connect")) else rule)     # không có AI để chọn: "ai" = chỉ mic + loa
    # Không AI thì người nghe máy chính là người dùng: thứ quy tắc đặt «nối ngay» (connect_now, blueprint: noi_ngay) phải được nối LÚC NGHE MÁY, hợp với
    # thứ «khi chuyển máy» (connect). Trước đây chỉ đọc connect: quy tắc có noi_ngay=[mic, speaker, camera] và noi_khi_chuyen=[] bị tắt hết thiết bị, cuộc
    # gọi nghe máy được nhưng im lặng (đo 12/11 07:06: app nhận mic/speaker/camera = «off»).
    connect = {**connect, **rule_devices_now(rule)}
    return explicit_devices(connect) if connect else dict(HELD_DEVICES)


def outgoing_devices(rule: dict[str, Any]) -> dict[str, Any]:
    """Thiết bị nhà nối cho cuộc gọi ĐI do người dùng tự gọi (WebUI): người gọi chính là người nói nên không có AI giữ máy; lấy cả thứ «nối ngay»
    lẫn thứ «khi chuyển máy» của quy tắc (như nhánh không-AI của cuộc gọi đến)."""
    base = rule_devices({**rule, "connect": "voice"} if "ai" in parse_connect(rule.get("connect")) else rule)
    return {**base, **rule_devices_now(rule)}


def outgoing_should_apply(call_type: Any, call_id: str, already_started: bool) -> bool:
    """Áp quy tắc cho cuộc gọi 1-1 được nghe/gọi mà HA CHƯA nối thiết bị nào: người dùng gọi từ WebUI, hoặc nghe máy tay trên WebUI (nghe máy theo quy tắc,
    hay HA tự gọi bằng make_call kèm thiết bị, thì đã có thiết bị nên bỏ qua). Nhóm: không (phải qua máy chủ nhóm)."""
    return str(call_type) != "group" and not already_started


async def _connect_outgoing_by_rule(hass: HomeAssistant, rt: Any, call_id: str, payload: dict[str, Any]) -> None:
    """Cuộc gọi được nghe/gọi mà HA không tự nối thiết bị (người dùng gọi từ WebUI hoặc nghe máy tay trên WebUI): áp quy tắc của người đó (theo threadID)
    để camera/mic/loa vào cuộc gọi theo automation, không phải bấm tay. Không có quy tắc hoặc quy tắc không cài thiết bị thì KHÔNG làm gì."""
    rule = rule_for_call(rt, payload)
    if not rule:
        return
    connect = outgoing_devices(rule)
    if not connect:
        return
    try:
        await async_connect_devices(rt, call_id, connect, video=str(payload.get("call_type")) == "video", echo_mode=rule.get("echo_guard"))
    except ZaloCallError as err:
        _LOGGER.warning("Cuộc gọi đi %s: không nối được thiết bị theo quy tắc: %s", call_id, err)
    except Exception:  # noqa: BLE001
        _LOGGER.exception("Cuộc gọi đi %s: nối thiết bị theo quy tắc lỗi", call_id)


def _plan_from_rule(rule: dict[str, Any], video: bool = False) -> Plan | None:
    if not rule.get("ai"):
        return None
    return Plan(ai=True, hangup=True, pipeline_id=rule.get("pipeline") or None, agent_id=rule.get("agent") or "",
                greeting=rule.get("greeting") or DEFAULT_GREETING, handover_rule=rule.get("handover_rule") or "",
                trusted=bool(rule.get("trusted")), video=video, connect=rule_devices(rule), echo_guard=str(rule.get("echo_guard") or ""),
                connect_now=rule_devices_now(rule), connect_ai="ai" in parse_connect(rule.get("connect")))


def _unresolved_reasons(hass: HomeAssistant, rule: dict[str, Any], fields: dict[str, str]) -> dict[str, str]:
    """Thiết bị quy tắc ĐÃ chọn mà HA không dựng được đường nối (fields rỗng/«off»): trả lý do bằng lời, gửi app (`standby_loi_*`) để nút tay trên WebUI nói đúng
    «đã chọn X nhưng HA không lấy được luồng» thay vì «chưa chọn», và ghi cảnh báo vào nhật ký. Trước đây thất bại này im lặng: người dùng thấy app nhận «off»."""
    out: dict[str, str] = {}
    for key, field, kind in (("camera", "camera_url", "camera"), ("mic", "mic_url", "mic"), ("speaker", "speaker_url", "speaker")):
        sel = str(rule.get(key) or "").strip()
        if not sel or (key == "mic" and rule.get("mic_url")) or fields.get(field) not in (None, "", "off"):
            continue
        entity = sel.split(",")[0].split("|")[0].strip()
        if hass.states.get(entity) is None and "." in entity and "://" not in entity:
            why = f"đã chọn {entity} nhưng thực thể này KHÔNG tồn tại trong HA"
        elif kind == "camera":
            why = f"đã chọn {entity} nhưng HA không lấy được luồng xem (camera cần có RTSP/go2rtc/stream)"
        elif kind == "mic":
            why = f"đã chọn {entity} nhưng HA không dựng được nguồn mic (cần loa R1 có tích hợp r1_speaker mới, hoặc camera có RTSP)"
        else:
            why = f"đã chọn {entity} nhưng loa này không có cổng bộ đàm (R1 cần tích hợp r1_speaker mới)"
        out[f"standby_loi_{kind}"] = why[:110]
        _LOGGER.warning("Quy tắc cuộc gọi: %s", why)
    return out


async def _standby_fields(rt: Any, rule: dict[str, Any]) -> dict[str, str]:
    """Loa / mic / camera mà QUY TẮC đã chọn, đổi thành URL gửi kèm lúc nghe máy dưới tên ``standby_*``: app KHÔNG tự nối (chỉ nối thứ ``connect_now``), nhưng nút
    bấm TAY Mic / Loa / Camera trên WebUI dùng chúng. Không có thì {} (app báo «chưa có camera…» như cũ)."""
    src: dict[str, Any] = {k: rule[k] for k in _DEVICE_KEYS if rule.get(k)}
    if not src:
        return {}
    src.update({k: rule[k] for k in ("volume", "mic_gain", "speaker_gain") if rule.get(k)})
    try:
        fields = await rt.bridge.async_call_fields(explicit_devices(src), rule.get("echo_guard"))
    except Exception:  # noqa: BLE001 — thiết bị chờ chỉ là tiện ích: hỏng thì thôi, không làm hỏng việc nghe máy
        _LOGGER.debug("Không dựng được thiết bị chờ của quy tắc", exc_info=True)
        return {}
    out = {f"standby_{k}": v for k, v in fields.items() if k in ("mic_url", "mic_format", "camera_url", "speaker_url") and v not in ("", "off", None)}
    try:
        out.update(_unresolved_reasons(rt.bridge.hass, rule, fields))
    except Exception:  # noqa: BLE001 — giải thích lỗi chỉ là tiện ích: không bao giờ làm hỏng việc nghe máy
        _LOGGER.debug("Không dựng được lý do thiết bị chưa nối", exc_info=True)
    return out


async def _answer_by_rule(hass: HomeAssistant, rt: Any, call_id: str, rule: dict[str, Any], video: bool = False) -> None:
    """Hết giờ chờ: hỏi lại app xem cuộc gọi còn đang đổ chuông không (người gọi có thể đã cúp), rồi nghe máy theo quy tắc."""
    api: ZaloCallApi = rt.api
    voice: CallVoice = rt.voice
    try:
        ev = voice.announce_done.get(call_id)
        if ev is not None and not ev.is_set():                    # báo tên người gọi xong rồi mới nghe máy (không đè lên cuộc gọi)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(ev.wait(), timeout=ANNOUNCE_WAIT_MAX)
        if await _call_status(api, call_id) != "incoming":
            return
        override = _devices_override(rule)
        rt.bridge.remember(call_id, override)
        fields = await rt.bridge.async_call_fields(override, rule.get("echo_guard"))      # chống vọng của QUY TẮC (trước đây bị bỏ qua)
        fields = {**fields, **await _standby_fields(rt, rule)}
        plan = _plan_from_rule(rule, video)
        if plan is not None:
            voice.plans[call_id] = plan               # đăng ký TRƯỚC khi nghe: webhook «đã nghe máy» có thể đến ngay
        try:
            res = await api.call_answer(call_id, fields)
        except ZaloCallError:
            voice.plans.pop(call_id, None)
            raise
        if not (res or {}).get("ok", True):
            voice.plans.pop(call_id, None)
            _LOGGER.warning("Không nghe máy được cuộc gọi %s: %s", call_id, (res or {}).get("error"))
    except asyncio.CancelledError:
        raise
    except ZaloCallError as err:
        _LOGGER.warning("Nghe máy theo quy tắc cuộc gọi %s lỗi: %s", call_id, err)
    except Exception:  # noqa: BLE001
        _LOGGER.exception("Nghe máy theo quy tắc cuộc gọi %s lỗi", call_id)


async def async_push_standby(rt: Any, cid: str, payload: dict[str, Any]) -> None:
    """Đẩy loa/mic/camera mà QUY TẮC của người này đã chọn xuống app cho cuộc gọi `cid` (đến HOẶC đi, nghe tự động hay bằng tay) để nút bấm tay Mic / Loa /
    Camera trên WebUI dùng đúng chúng; app KHÔNG tự nối. Mỗi cuộc một lần. Lỗi chỉ ghi nhật ký."""
    voice: CallVoice = rt.voice
    if cid in voice.standby_sent or str(payload.get("call_type")) == "group":
        return
    rule = rule_for_call(rt, payload)
    if not rule:
        return
    fields = await _standby_fields(rt, rule)
    if not fields:
        return
    voice.standby_sent.add(cid)
    try:
        await rt.api.media_control({"call_id": cid, "action": "standby", **fields})
    except Exception:  # noqa: BLE001 — tiện ích: hỏng thì nút tay báo thiếu thiết bị như cũ, không làm hỏng cuộc gọi
        _LOGGER.debug("Không đẩy được thiết bị chờ cho cuộc gọi %s", cid, exc_info=True)


def schedule_answer(hass: HomeAssistant, rt: Any, payload: dict[str, Any], rule: dict[str, Any] | None = None) -> bool:
    """Cuộc gọi 1-1 ĐẾN: áp quy tắc (theo threadID, rồi danh bạ/người lạ). Không có quy tắc, hoặc quy tắc không cho nghe loại cuộc gọi
    này, hoặc không biết người gọi quen hay lạ → KHÔNG làm gì (chỉ đổ chuông). True nếu đã lên lịch nghe."""
    cid = str(payload.get("call_id"))
    if str(payload.get("call_type")) == "group":
        return False                                     # cuộc gọi NHÓM: app chỉ báo, chưa nghe được (quy tắc 1-1 không áp)
    rule = rule if rule is not None else rule_for_call(rt, payload)
    if not rule or not rule.get("answer"):
        return False
    ctype = "video" if str(payload.get("call_type_raw")) == "1" else "voice"        # 0 thoại / 1 video theo zca-js (chưa kiểm cuộc thật)
    if ctype not in rule.get("call_types", ()):
        return False
    voice: CallVoice = rt.voice
    if cid in voice.scheduled:
        return False                                     # webhook «incoming» đến hai lần: chỉ một lịch
    voice.scheduled.add(cid)

    async def _go(_now: Any) -> None:
        voice.timers.pop(cid, None)
        await _answer_by_rule(hass, rt, cid, rule, ctype == "video")

    voice.timers[cid] = async_call_later(hass, int(rule.get("answer_delay", 0)), _go)
    return True


def _execute_rules(hass: HomeAssistant, rt: Any, cid: str, payload: dict[str, Any], claims: list[Claim]) -> None:
    """Làm theo các quy tắc đã chốt của cuộc gọi ĐẾN: báo ra loa (mọi quy tắc), đẩy thiết bị chờ xuống app, lên lịch nghe máy (quy tắc làm chính)."""
    voice: CallVoice = rt.voice
    main = primary_claim(claims)
    if main is not None:
        voice.rule_of[cid] = main.rule
        if len(claims) > 1:
            _LOGGER.info("Cuộc gọi %s khớp %d quy tắc cùng bậc (%s): %s làm chính (nghe máy, thiết bị); báo ra loa theo từng quy tắc",
                         cid, len(claims), ", ".join(c.source or "?" for c in claims), main.source or "?")
    rules = [c.rule for c in claims]
    voice.announce_tasks[cid] = hass.async_create_task(async_announce_incoming(hass, rt, payload, rules))
    if cid not in voice.standby_sent:
        hass.async_create_task(async_push_standby(rt, cid, payload))
    schedule_answer(hass, rt, payload, main.rule if main is not None else None)


def _decide(hass: HomeAssistant, rt: Any, cid: str) -> None:
    """Hết giờ chờ automation: chốt quy tắc cho cuộc gọi đến. Có quy tắc do automation áp thì chỉ dùng chúng (cụ thể nhất); không có thì dùng quy tắc lưu sẵn
    (set_rule — cách dùng cũ). Không có gì = chỉ đổ chuông, không làm gì."""
    voice: CallVoice = rt.voice
    voice.decide_timers.pop(cid, None)
    if cid in voice.decided or cid not in voice.incoming:
        return
    payload = voice.call_payload.get(cid) or {"call_id": cid}
    claims = best_claims(voice.claims.get(cid, []))
    if not claims:
        stored = resolve_rule(get_rules(rt.defaults), payload.get("thread_id"), "user", payload.get("contact_status"))
        claims = [Claim(stored, len(SCOPES), "kho quy tắc (set_rule)")] if stored else []
    voice.decided[cid] = claims
    _execute_rules(hass, rt, cid, payload, claims)


def _begin_decide(hass: HomeAssistant, rt: Any, cid: str, payload: dict[str, Any]) -> None:
    voice: CallVoice = rt.voice
    voice.incoming.add(cid)
    if cid in voice.announced:                                  # webhook «incoming» có thể đến hai lần: quyết một lần
        return
    voice.announced.add(cid)
    voice.announce_done[cid] = asyncio.Event()
    voice.call_payload[cid] = payload
    grace = DECIDE_GRACE if voice.decide_grace is None else voice.decide_grace
    if grace <= 0:
        _decide(hass, rt, cid)
        return
    # Các automation «áp quy tắc» bắn từ cùng sự kiện nên tới sau vài mili giây; chờ một nhịp ngắn để chọn quy tắc cụ thể nhất thay vì chạy cái tới trước.

    @callback
    def _fire(_now: Any) -> None:                               # @callback: chạy trong vòng lặp sự kiện (hàm thường bị HA đẩy sang luồng khác)
        _decide(hass, rt, cid)
    voice.decide_timers[cid] = async_call_later(hass, grace, _fire)


async def async_apply_claim(hass: HomeAssistant, rt: Any, call_id: Any, claim: Claim) -> str:
    """Dịch vụ apply_call_rule: một automation áp quy tắc của nó cho cuộc gọi `call_id`. Trả về: queued (chờ chốt), applied (cuộc gọi đã chốt mà chưa có quy tắc nào
    nên làm ngay), late (đến sau khi đã chốt bằng quy tắc khác: bỏ, có ghi nhật ký), duplicate (automation này đã áp cho cuộc gọi này), recorded (đến trước khi
    tích hợp đánh dấu cuộc gọi đến — _decide vẫn dùng — hoặc cuộc gọi không phải cuộc đến: ghi nhận cho các bước sau như nối thiết bị khi nghe máy tay)."""
    voice: CallVoice = rt.voice
    cid = str(call_id)
    if claim.source and any(c.source == claim.source for c in voice.claims.get(cid, [])):
        return "duplicate"
    voice.claim_seq += 1
    claim.seq = voice.claim_seq
    voice.claims.setdefault(cid, []).append(claim)
    if cid in voice.decide_timers:
        return "queued"
    if cid in voice.decided:
        if voice.decided[cid]:
            _LOGGER.warning("Quy tắc của %s đến sau khi cuộc gọi %s đã chốt theo %s: bỏ qua (nới thời gian chờ nếu automation chậm)",
                            claim.source or "?", cid, voice.decided[cid][0].source or "?")
            return "late"
        voice.decided[cid] = [claim]
        _execute_rules(hass, rt, cid, voice.call_payload.get(cid) or {"call_id": cid}, [claim])
        return "applied"
    return "recorded"


def _stop_announce(voice: "CallVoice", cid: str) -> None:
    task = voice.announce_tasks.pop(cid, None)
    if task is not None and not task.done():
        task.cancel()


def on_call_state(hass: HomeAssistant, rt: Any, state: str, call_id: Any, terminal: tuple, payload: dict[str, Any] | None = None) -> None:
    """Gọi từ webhook cuộc gọi: «incoming» → áp quy tắc nghe máy; «answered» → chạy kế hoạch (thông báo/AI); kết thúc → dọn lịch/kế hoạch."""
    cid = str(call_id)
    voice: CallVoice = rt.voice
    if state in ("ringing", "answered") and payload and cid not in voice.standby_sent:       # cuộc đến: đẩy sau khi chốt quy tắc (_execute_rules)
        hass.async_create_task(async_push_standby(rt, cid, payload))
    if state == "incoming":
        _begin_decide(hass, rt, cid, payload or {"call_id": cid})
    elif state == "video_request":
        hass.async_create_task(async_handle_video_request(hass, rt, payload or {"call_id": cid}))
    elif state == "answered":
        voice.answered_ids.add(cid)
        _stop_announce(voice, cid)                              # đã có người nghe: thôi báo ra loa
        plan = voice.plans.pop(cid, None)
        if plan is not None and cid not in voice.tasks:
            voice.tasks[cid] = hass.async_create_task(_run_plan(hass, rt, cid, plan))
        elif plan is None and payload and outgoing_should_apply(payload.get("call_type"), cid, rt.bridge.has_devices(cid)):
            hass.async_create_task(_connect_outgoing_by_rule(hass, rt, cid, payload))
    elif state in terminal:
        _stop_announce(voice, cid)
        voice.answered_ids.discard(cid)
        cancel = voice.timers.pop(cid, None)
        if cancel:
            cancel()
        cancel = voice.decide_timers.pop(cid, None)
        if cancel:
            cancel()
        voice.claims.pop(cid, None)
        voice.decided.pop(cid, None)
        voice.rule_of.pop(cid, None)
        voice.call_payload.pop(cid, None)
        voice.scheduled.discard(cid)
        voice.standby_sent.discard(cid)
        voice.plans.pop(cid, None)
        voice.incoming.discard(cid)
        voice.announced.discard(cid)
        voice.announce_done.pop(cid, None)
        task = voice.tasks.pop(cid, None)
        if task:
            task.cancel()
