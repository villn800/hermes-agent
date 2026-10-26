"""Photon transport diagnostics, typing breaker, and read-receipt tests.

These tests exercise the REAL sidecar (``plugins/platforms/photon/sidecar/index.mjs``)
against a stub ``spectrum-ts`` package, so the transport handling under test is
the shipped code path — not a re-implementation. The stub only encodes the
SDK-side condition:

* the ``/typing`` send rejects with the exact upstream ``ValidationError`` text
  Apple's shared-line failure produces (``Typing requires iMessage, but that
  service is not currently available for this recipient.``) while
  ``FAKE_TYPING_UNAVAILABLE=1``, and succeeds after a ``{"control":"typing_ok"}``
  line — never because the sidecar changed its own state, so the breaker
  assertions cannot be circular;
* every attempt is announced on stderr as ``fake-sdk: typing send <state>``, so
  "no wire call was made" is directly observable.

Inbound events are injected over the sidecar's stdin and read back from the real
``GET /inbound`` NDJSON stream.

The adapter half (``stop_typing`` de-duplication) stubs ``_sidecar_call`` and
asserts the wire request count, following test_overflow_recovery.py.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.photon.adapter import PhotonAdapter

SIDECAR_DIR = Path("plugins/platforms/photon/sidecar").resolve()
TOKEN = "test-token"
PHONE = "+15550000709"
SPACE_ID = f"any;-;{PHONE}"

_FAKE_SDK_CORE = '''
import readline from "node:readline";

export const state = {
  typingAvailable: process.env.FAKE_TYPING_UNAVAILABLE !== "1",
};
const sendErrorCode = Number(process.env.FAKE_SEND_ERROR_CODE || "0") || 0;

function log(line) {
  process.stderr.write("fake-sdk: " + line + "\\n");
}

const queue = [];
const waiters = [];
let closed = false;
const rl = readline.createInterface({ input: process.stdin });
rl.on("line", (line) => {
  const waiter = waiters.shift();
  if (waiter) waiter(line);
  else queue.push(line);
});
rl.on("close", () => {
  closed = true;
  while (waiters.length) waiters.shift()(null);
});

async function nextLine() {
  if (queue.length) return queue.shift();
  if (closed) return null;
  return new Promise((resolve) => waiters.push(resolve));
}

export function makeSpace(id, phone, type) {
  return {
    id,
    type: type || "dm",
    phone: phone || null,
    async send(builder) {
      if (builder && builder.type === "typing") {
        log("typing send " + String(builder.state));
        if (!state.typingAvailable) {
          const err = new Error(
            "[upstream] Typing requires iMessage, but that service is not " +
              "currently available for this recipient."
          );
          err.name = "ValidationError";
          throw err;
        }
        return { id: "fake-typing-message", sendErrorCode: 0 };
      }
      log("message send");
      return { id: "fake-outbound-message", sendErrorCode };
    },
    async getMessage() {
      const err = new Error("5 NOT_FOUND: no such message");
      err.code = 5;
      throw err;
    },
  };
}

function messageFor(spec) {
  return {
    id: spec.messageId || "fake-inbound-message",
    platform: "iMessage",
    direction: "inbound",
    space: { id: spec.spaceId, type: spec.type || "dm", phone: spec.phone || null },
    sender: { id: spec.senderId || spec.phone || "peer", service: spec.service },
    content: { type: "text", text: spec.text || "hello" },
    timestamp: new Date(),
    async read() {
      log("read attempt service=" + String(spec.service));
      if (spec.service && spec.service !== "iMessage") {
        const err = new Error("[upstream] Read receipts require iMessage.");
        err.name = "ValidationError";
        throw err;
      }
    },
  };
}

export function inbound() {
  return {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          for (;;) {
            const line = await nextLine();
            if (!line) return { value: undefined, done: true };
            let spec;
            try {
              spec = JSON.parse(line);
            } catch {
              continue;
            }
            if (spec && spec.control) {
              if (spec.control === "typing_ok") state.typingAvailable = true;
              if (spec.control === "typing_fail") state.typingAvailable = false;
              log("control " + spec.control);
              continue;
            }
            return {
              value: [makeSpace(spec.spaceId, spec.phone, spec.type), messageFor(spec)],
              done: false,
            };
          }
        },
      };
    },
  };
}
'''

_FAKE_SDK_INDEX = '''
import { inbound } from "./fake-sdk-core.js";

export const attachment = (value) => ({ type: "attachment", ...value });
export const voice = (value) => ({ type: "voice", ...value });
export const text = (value) => ({ type: "text", text: value });
export const markdown = (value) => ({ type: "markdown", text: value });
export const richlink = (url) => ({ type: "richlink", url });
export const poll = (title, options) => ({ type: "poll", title, options });
export const typing = (state) => ({ type: "typing", state });

export async function Spectrum() {
  return { messages: inbound(), async stop() {} };
}
'''

_FAKE_SDK_IMESSAGE = '''
import { makeSpace } from "../fake-sdk-core.js";

export const effect = { message: {} };

export function imessage() {
  return {
    space: {
      async get(id) {
        return makeSpace(id, null, null);
      },
      async create(phone) {
        return makeSpace("any;-;" + phone, phone, "dm");
      },
    },
  };
}

// The real SDK exposes `imessage` as a callable object: index.mjs reads both
// `imessage.config()` (providers) and `imessage.effect.message` (effects).
imessage.config = () => ({ provider: "imessage" });
imessage.effect = effect;
'''

_FAKE_SDK_PACKAGE = {
    "name": "spectrum-ts",
    "version": "0.0.0-test",
    "type": "module",
    "exports": {
        ".": "./index.js",
        "./providers/imessage": "./providers/imessage.js",
    },
}


def _sidecar_fixture(tmp_path: Path) -> Path:
    """Copy the real sidecar next to a stub spectrum-ts package."""
    sidecar = tmp_path / "sidecar"
    sidecar.mkdir()
    for helper in SIDECAR_DIR.glob("*.mjs"):
        shutil.copyfile(helper, sidecar / helper.name)
    sdk = sidecar / "node_modules" / "spectrum-ts"
    (sdk / "providers").mkdir(parents=True)
    (sdk / "package.json").write_text(json.dumps(_FAKE_SDK_PACKAGE), encoding="utf-8")
    (sdk / "index.js").write_text(_FAKE_SDK_INDEX, encoding="utf-8")
    (sdk / "fake-sdk-core.js").write_text(_FAKE_SDK_CORE, encoding="utf-8")
    (sdk / "providers" / "imessage.js").write_text(_FAKE_SDK_IMESSAGE, encoding="utf-8")
    return sidecar


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class SidecarHarness:
    """A running sidecar plus its captured stderr and stdin injection channel."""

    def __init__(self, proc: subprocess.Popen, port: int, lines: List[str], lock: threading.Lock):
        self.proc = proc
        self.port = port
        self._lines = lines
        self._lock = lock
        self._inbound: Optional[socket.socket] = None
        self._inbound_file = None

    # -- process observation -------------------------------------------------
    @property
    def stderr(self) -> str:
        with self._lock:
            return "".join(self._lines)

    def occurrences(self, needle: str) -> int:
        return self.stderr.count(needle)

    def wait_for_stderr(self, needle: str, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if needle in self.stderr:
                return
            time.sleep(0.05)
        raise AssertionError(f"stderr never contained {needle!r}; saw:\n{self.stderr}")

    def wait_for_port(self, timeout: float = 15.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with socket.socket() as probe:
                probe.settimeout(0.5)
                if probe.connect_ex(("127.0.0.1", self.port)) == 0:
                    return
            time.sleep(0.05)
        raise AssertionError(f"sidecar never listened on {self.port}; stderr:\n{self.stderr}")

    # -- HTTP ---------------------------------------------------------------
    def post(self, path: str, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "X-Hermes-Sidecar-Token": TOKEN},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def open_inbound(self) -> None:
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=15)
        sock.sendall(
            b"GET /inbound HTTP/1.1\r\nHost: 127.0.0.1\r\n"
            b"X-Hermes-Sidecar-Token: " + TOKEN.encode() + b"\r\n\r\n"
        )
        self._inbound = sock
        self._inbound_file = sock.makefile("rb")
        # Node sends the response head with the first NDJSON write, so the status
        # line is read lazily by next_event() rather than here.

    def inject(self, spec: Dict[str, Any]) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write((json.dumps(spec) + "\n").encode())
        self.proc.stdin.flush()

    def next_event(self, timeout: float = 20.0) -> Dict[str, Any]:
        assert self._inbound_file is not None and self._inbound is not None
        self._inbound.settimeout(timeout)
        while True:
            line = self._inbound_file.readline()
            if not line:
                raise AssertionError("inbound stream closed")
            stripped = line.strip()
            # Skip the HTTP status/headers (sent with the first chunk) and heartbeats.
            if not stripped or not stripped.startswith(b"{"):
                continue
            return json.loads(stripped)

    def close(self) -> None:
        for handle in (self._inbound_file, self._inbound):
            try:
                if handle is not None:
                    handle.close()
            except OSError:
                pass
        try:
            if self.proc.stdin:
                self.proc.stdin.close()
        except OSError:
            pass
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()


@pytest.fixture
def sidecar(tmp_path, monkeypatch):
    """Boot the real sidecar with the stub SDK; yields a SidecarHarness."""
    monkeypatch.delenv("PHOTON_SIDECAR_WATCH_STDIN", raising=False)
    monkeypatch.delenv("PHOTON_PROBE_SPACE_ID", raising=False)
    sidecar_dir = _sidecar_fixture(tmp_path)
    port = _free_port()
    env = {
        **os.environ,
        "PHOTON_PROJECT_ID": "test-project-id",
        "PHOTON_PROJECT_SECRET": "test-project-secret",
        "PHOTON_SIDECAR_TOKEN": TOKEN,
        "PHOTON_SIDECAR_PORT": str(port),
        "PHOTON_SIDECAR_BIND": "127.0.0.1",
        "FAKE_TYPING_UNAVAILABLE": "1",
        # The zombie watchdog is unrelated here and would add probe noise.
        "PHOTON_STREAM_SILENCE_PROBE_MS": "0",
        "PHOTON_READ_RECEIPTS": "true",
    }
    proc = subprocess.Popen(
        ["node", "index.mjs"],
        cwd=sidecar_dir,
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    lines: List[str] = []
    lock = threading.Lock()

    def pump() -> None:
        assert proc.stderr is not None
        for raw in proc.stderr:
            with lock:
                lines.append(raw.decode("utf-8", "replace"))

    reader = threading.Thread(target=pump, daemon=True)
    reader.start()
    harness = SidecarHarness(proc, port, lines, lock)
    harness.wait_for_port()
    try:
        yield harness
    finally:
        harness.close()


# ---------------------------------------------------------------------------
# Sidecar: typing breaker


def test_typing_breaker_skips_without_touching_the_wire(sidecar):
    sidecar.open_inbound()

    status, payload = sidecar.post("/typing", {"spaceId": SPACE_ID, "state": "start"})
    assert status == 200
    assert payload == {"ok": True, "skipped": True, "reason": "imessage_unavailable"}
    sidecar.wait_for_stderr("typing unavailable for " + PHONE)
    # Exactly one transition line, and no stack trace from the top-level handler.
    assert sidecar.occurrences("typing unavailable for ") == 1
    assert "handler error" not in sidecar.stderr
    assert "ValidationError" not in sidecar.stderr
    assert sidecar.occurrences("fake-sdk: typing send") == 1

    # Every later call inside the TTL is a no-op: no gRPC round trip, no log line.
    for state in ("start", "stop", "start", "stop"):
        status, payload = sidecar.post("/typing", {"spaceId": SPACE_ID, "state": state})
        assert status == 200
        assert payload["skipped"] is True and payload["reason"] == "imessage_unavailable"
    assert sidecar.occurrences("fake-sdk: typing send") == 1
    assert sidecar.occurrences("typing unavailable for ") == 1


def test_inbound_imessage_clears_the_breaker_and_lifts_the_skip(sidecar):
    sidecar.open_inbound()
    status, payload = sidecar.post("/typing", {"spaceId": SPACE_ID, "state": "start"})
    assert status == 200 and payload["skipped"] is True
    sidecar.wait_for_stderr("typing unavailable for ")
    assert sidecar.occurrences("fake-sdk: typing send") == 1

    # The recipient's transport works again (and the SDK now accepts typing).
    sidecar.inject({"control": "typing_ok"})
    sidecar.inject(
        {
            "spaceId": SPACE_ID,
            "phone": PHONE,
            "service": "iMessage",
            "messageId": "m-imessage",
            "text": "are you there?",
        }
    )
    event = sidecar.next_event()
    # The normalized event carries the transport the message actually arrived on.
    assert event["sender"]["service"] == "iMessage"
    assert event["service"] == "iMessage"

    status, payload = sidecar.post("/typing", {"spaceId": PHONE, "state": "start"})
    assert status == 200
    assert "skipped" not in payload
    assert sidecar.occurrences("fake-sdk: typing send") == 2


def test_rcs_inbound_is_labelled_and_read_receipts_are_not_attempted(sidecar):
    sidecar.open_inbound()
    sidecar.inject(
        {
            "spaceId": SPACE_ID,
            "phone": PHONE,
            "service": "RCS",
            "messageId": "m-rcs",
            "text": "sent over rcs",
        }
    )
    event = sidecar.next_event()
    assert event["sender"]["service"] == "RCS"
    assert event["service"] == "RCS"
    time.sleep(0.3)
    # No receipt attempt at all on a non-iMessage thread — and therefore no
    # "read-receipt failed" warning either.
    assert "fake-sdk: read attempt" not in sidecar.stderr
    assert "read-receipt failed" not in sidecar.stderr

    sidecar.inject(
        {
            "spaceId": SPACE_ID,
            "phone": PHONE,
            "service": "iMessage",
            "messageId": "m-imessage",
            "text": "sent over imessage",
        }
    )
    event = sidecar.next_event()
    assert event["sender"]["service"] == "iMessage"
    sidecar.wait_for_stderr("fake-sdk: read attempt service=iMessage")
    assert "read-receipt failed" not in sidecar.stderr


def test_send_surfaces_the_upstream_send_error_code(tmp_path, monkeypatch):
    """A non-zero sendErrorCode is returned to the adapter, not silently dropped."""
    monkeypatch.delenv("PHOTON_SIDECAR_WATCH_STDIN", raising=False)
    sidecar_dir = _sidecar_fixture(tmp_path)
    port = _free_port()
    env = {
        **os.environ,
        "PHOTON_PROJECT_ID": "test-project-id",
        "PHOTON_PROJECT_SECRET": "test-project-secret",
        "PHOTON_SIDECAR_TOKEN": TOKEN,
        "PHOTON_SIDECAR_PORT": str(port),
        "FAKE_TYPING_UNAVAILABLE": "0",
        "FAKE_SEND_ERROR_CODE": "22",
        "PHOTON_STREAM_SILENCE_PROBE_MS": "0",
    }
    proc = subprocess.Popen(
        ["node", "index.mjs"],
        cwd=sidecar_dir,
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    lines: List[str] = []
    lock = threading.Lock()

    def pump() -> None:
        assert proc.stderr is not None
        for raw in proc.stderr:
            with lock:
                lines.append(raw.decode("utf-8", "replace"))

    threading.Thread(target=pump, daemon=True).start()
    harness = SidecarHarness(proc, port, lines, lock)
    harness.wait_for_port()
    try:
        status, payload = harness.post("/send", {"spaceId": SPACE_ID, "text": "hello"})
        assert status == 200
        assert payload["messageId"] == "fake-outbound-message"
        assert payload["sendErrorCode"] == 22
        harness.wait_for_stderr("sendErrorCode=22")
    finally:
        harness.close()


# ---------------------------------------------------------------------------
# Adapter: stop_typing de-duplication


def _make_adapter(monkeypatch: pytest.MonkeyPatch, **extra: Any) -> PhotonAdapter:
    monkeypatch.setenv("PHOTON_PROJECT_ID", "test-project-id")
    monkeypatch.setenv("PHOTON_PROJECT_SECRET", "test-project-secret")
    return PhotonAdapter(PlatformConfig(enabled=True, token="", extra=extra))


def _capture_typing(adapter: PhotonAdapter, *, skipped: bool) -> List[Dict[str, Any]]:
    calls: List[Dict[str, Any]] = []

    async def _fake_call(path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        calls.append({"path": path, **body})
        return {"ok": True, "skipped": True} if skipped and body.get("state") == "start" else {"ok": True}

    adapter._sidecar_call = _fake_call  # type: ignore[assignment]
    return calls


@pytest.mark.asyncio
async def test_stop_typing_issues_at_most_one_wire_call_per_indicator(monkeypatch) -> None:
    adapter = _make_adapter(monkeypatch)
    calls = _capture_typing(adapter, skipped=False)

    await adapter.send_typing(SPACE_ID)
    assert [c["state"] for c in calls] == ["start"]

    # base.py's turn teardown stops typing up to four times.
    for _ in range(4):
        await adapter.stop_typing(SPACE_ID)
    assert [c["state"] for c in calls] == ["start", "stop"]

    # A second turn starts a fresh indicator and gets exactly one more stop.
    await adapter.send_typing(SPACE_ID)
    await adapter.stop_typing(SPACE_ID)
    await adapter.stop_typing(SPACE_ID)
    assert [c["state"] for c in calls] == ["start", "stop", "start", "stop"]


@pytest.mark.asyncio
async def test_stop_typing_is_a_noop_when_the_start_was_skipped(monkeypatch) -> None:
    adapter = _make_adapter(monkeypatch)
    calls = _capture_typing(adapter, skipped=True)

    await adapter.send_typing(SPACE_ID)
    assert [c["state"] for c in calls] == ["start"]

    for _ in range(4):
        await adapter.stop_typing(SPACE_ID)
    # The start never reached the wire, so no stop may either.
    assert [c["state"] for c in calls] == ["start"]


@pytest.mark.asyncio
async def test_stop_typing_without_any_start_never_calls_the_sidecar(monkeypatch) -> None:
    adapter = _make_adapter(monkeypatch)
    calls = _capture_typing(adapter, skipped=False)

    await adapter.stop_typing(SPACE_ID)
    assert calls == []


@pytest.mark.asyncio
async def test_typing_state_is_shared_across_the_guid_and_phone_forms(monkeypatch) -> None:
    """The adapter may address a DM by GUID or bare phone; both mean one peer."""
    adapter = _make_adapter(monkeypatch)
    calls = _capture_typing(adapter, skipped=False)

    await adapter.send_typing(SPACE_ID)
    await adapter.stop_typing(PHONE)  # normalized to the same key
    await adapter.stop_typing(SPACE_ID)
    assert [c["state"] for c in calls] == ["start", "stop"]


# ---------------------------------------------------------------------------
# Adapter: inbound transport logging


@pytest.mark.asyncio
async def test_inbound_transport_transitions_are_logged_once(monkeypatch, caplog) -> None:
    adapter = _make_adapter(monkeypatch)

    seen: List[str] = []

    async def _record(text: str, mtype=None, **kwargs: Any) -> None:
        seen.append(text)

    adapter.handle_message = _record  # type: ignore[assignment]

    def _event(service: Optional[str]) -> Dict[str, Any]:
        sender: Dict[str, Any] = {"id": PHONE}
        if service:
            sender["service"] = service
        payload: Dict[str, Any] = {
            "messageId": f"m-{service}",
            "space": {"id": SPACE_ID, "type": "dm", "phone": PHONE},
            "sender": sender,
            "content": {"type": "text", "text": "hi"},
            "timestamp": "2026-10-22T23:41:10Z",
        }
        if service:
            payload["service"] = service
        return payload

    import logging

    with caplog.at_level(logging.INFO, logger="plugins.platforms.photon.adapter"):
        await adapter._dispatch_inbound(_event("iMessage"))
        await adapter._dispatch_inbound(_event("iMessage"))  # unchanged -> no second line
        await adapter._dispatch_inbound(_event("RCS"))

    lines = [r.getMessage() for r in caplog.records if "inbound transport service=" in r.getMessage()]
    assert lines == [
        f"[photon] chat {SPACE_ID} inbound transport service=iMessage",
        f"[photon] chat {SPACE_ID} inbound transport service=RCS",
    ]
    assert len(seen) == 3


@pytest.mark.asyncio
async def test_inbound_without_a_declared_service_logs_nothing(monkeypatch, caplog) -> None:
    adapter = _make_adapter(monkeypatch)

    async def _record(text: str, mtype=None, **kwargs: Any) -> None:
        return None

    adapter.handle_message = _record  # type: ignore[assignment]

    import logging

    with caplog.at_level(logging.INFO, logger="plugins.platforms.photon.adapter"):
        await adapter._dispatch_inbound(
            {
                "messageId": "m-plain",
                "space": {"id": SPACE_ID, "type": "dm", "phone": PHONE},
                "sender": {"id": PHONE},
                "content": {"type": "text", "text": "hi"},
                "timestamp": "2026-10-22T23:41:10Z",
            }
        )
    assert not [r for r in caplog.records if "inbound transport service=" in r.getMessage()]
