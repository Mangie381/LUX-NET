#!/usr/bin/env python3
"""
Headless libretro host used by the LUX-NET Discord bot for NES / SNES games.

One worker process = one running game. The bot spawns it on demand and kills it when the
session ends or goes idle, so the OS gives all of its memory back.

Two ways of running a game:

  LIVE   {"op": "live", "on": True}   The worker runs the core on its own clock (real time,
         ~60 Hz) and pushes a JPEG frame to the bot ~30 times a second. The bot relays those to
         browsers over a WebSocket. While nobody is watching, or nobody has pressed a key for a
         while, the loop sleeps and uses no CPU at all.

  BURST  "step" / "steps" / "snap" / "clip"   The core runs as fast as the CPU allows for a few
         frames, then one picture comes back. Used for the Discord button / chat controls.

Wire protocol (stdin/stdout, both ends are our own code):
  bot -> worker : 4-byte little-endian length + pickle
  worker -> bot : 4-byte little-endian length + 1 kind byte + payload
                  kind b"R" = pickled reply to the last request op
                  kind b"J" = JPEG video frame
                  kind b"E" = pickled event (stats / paused / resumed)
Ops that never get a reply: input, seq, live.
"""
import io
import os
import pickle
import queue
import struct
import sys
import threading
import time
from collections import deque
from ctypes import (CDLL, CFUNCTYPE, POINTER, Structure, byref, c_bool, c_char_p, c_double,
                    c_float, c_int, c_int16, c_size_t, c_uint, c_void_p, cast,
                    create_string_buffer, string_at)

from PIL import Image

# --- libretro structures -------------------------------------------------------------------
class GameInfo(Structure):
    _fields_ = [("path", c_char_p), ("data", c_void_p), ("size", c_size_t), ("meta", c_char_p)]


class SystemInfo(Structure):
    _fields_ = [("library_name", c_char_p), ("library_version", c_char_p),
                ("valid_extensions", c_char_p), ("need_fullpath", c_bool), ("block_extract", c_bool)]


class GameGeometry(Structure):
    _fields_ = [("base_width", c_uint), ("base_height", c_uint),
                ("max_width", c_uint), ("max_height", c_uint), ("aspect_ratio", c_float)]


class SystemTiming(Structure):
    _fields_ = [("fps", c_double), ("sample_rate", c_double)]


class AVInfo(Structure):
    _fields_ = [("geometry", GameGeometry), ("timing", SystemTiming)]


ENV_CB = CFUNCTYPE(c_bool, c_uint, c_void_p)
VIDEO_CB = CFUNCTYPE(None, c_void_p, c_uint, c_uint, c_size_t)
AUDIO_CB = CFUNCTYPE(None, c_int16, c_int16)
AUDIO_BATCH_CB = CFUNCTYPE(c_size_t, c_void_p, c_size_t)
POLL_CB = CFUNCTYPE(None)
INPUT_CB = CFUNCTYPE(c_int16, c_uint, c_uint, c_uint, c_uint)

RETRO_DEVICE_JOYPAD = 1
RETRO_DEVICE_ID_JOYPAD_MASK = 256

# libretro pixel format -> Pillow raw decoder
RAW_MODES = {0: "BGR;15", 1: "BGRX", 2: "BGR;16"}

try:
    _QUANT = Image.Quantize.FASTOCTREE
except AttributeError:  # very old Pillow
    _QUANT = 2


class Core:
    def __init__(self, core_path: str, rom_path: str, system_dir: str, save_dir: str):
        os.makedirs(system_dir, exist_ok=True)
        os.makedirs(save_dir, exist_ok=True)
        self._sys = system_dir.encode()
        self._save = save_dir.encode()
        self.fmt = 0            # libretro default is 0RGB1555 until the core says otherwise
        self.frame = None       # (raw bytes, w, h, pitch) of the latest *copied* video frame
        self.fresh = False      # True when the last retro_run delivered (and we copied) new pixels
        self.want_video = True  # False => we do not need this frame: skip the copy, tell the core
        self.mask = 0           # currently held buttons (libretro joypad bitmask)

        self.lib = lib = CDLL(core_path)
        lib.retro_set_environment.argtypes = [ENV_CB]
        lib.retro_set_video_refresh.argtypes = [VIDEO_CB]
        lib.retro_set_audio_sample.argtypes = [AUDIO_CB]
        lib.retro_set_audio_sample_batch.argtypes = [AUDIO_BATCH_CB]
        lib.retro_set_input_poll.argtypes = [POLL_CB]
        lib.retro_set_input_state.argtypes = [INPUT_CB]
        lib.retro_get_system_info.argtypes = [POINTER(SystemInfo)]
        lib.retro_get_system_av_info.argtypes = [POINTER(AVInfo)]
        lib.retro_load_game.argtypes = [POINTER(GameInfo)]
        lib.retro_load_game.restype = c_bool
        lib.retro_serialize_size.restype = c_size_t
        lib.retro_serialize.argtypes = [c_void_p, c_size_t]
        lib.retro_serialize.restype = c_bool
        lib.retro_unserialize.argtypes = [c_void_p, c_size_t]
        lib.retro_unserialize.restype = c_bool

        # Keep references: ctypes callbacks are freed if the Python object is collected.
        self._cbs = (
            ENV_CB(self._env), VIDEO_CB(self._video), AUDIO_CB(lambda l, r: None),
            AUDIO_BATCH_CB(lambda d, n: n),   # audio is discarded: nobody can hear a Discord frame
            POLL_CB(lambda: None), INPUT_CB(self._input),
        )
        lib.retro_set_environment(self._cbs[0])
        lib.retro_init()
        lib.retro_set_video_refresh(self._cbs[1])
        lib.retro_set_audio_sample(self._cbs[2])
        lib.retro_set_audio_sample_batch(self._cbs[3])
        lib.retro_set_input_poll(self._cbs[4])
        lib.retro_set_input_state(self._cbs[5])

        info = SystemInfo()
        lib.retro_get_system_info(byref(info))
        self.name = (info.library_name or b"core").decode(errors="replace")

        gi = GameInfo()
        gi.path = rom_path.encode()
        gi.meta = None
        self._rom = None
        if info.need_fullpath:
            gi.data, gi.size = None, 0
        else:
            with open(rom_path, "rb") as f:
                self._rom = f.read()                    # must outlive the game
            gi.data = cast(c_char_p(self._rom), c_void_p)
            gi.size = len(self._rom)
        self._gi = gi
        if not lib.retro_load_game(byref(gi)):
            raise RuntimeError("core refused to load this ROM")

        av = AVInfo()
        lib.retro_get_system_av_info(byref(av))
        self.fps = av.timing.fps or 60.0
        for _ in range(5):                  # get a first picture (a core may start with a dupe)
            self.run_frames(1, 0)
            if self.frame is not None:
                break

    # -- callbacks ---------------------------------------------------------------------------
    def _env(self, cmd, data):
        cmd &= 0xFFFF   # strip EXPERIMENTAL / PRIVATE flags
        if cmd == 3:                                    # GET_CAN_DUPE
            cast(data, POINTER(c_bool))[0] = True
            return True
        if cmd in (9, 31):                              # GET_SYSTEM_DIRECTORY / GET_SAVE_DIRECTORY
            cast(data, POINTER(c_char_p))[0] = self._sys if cmd == 9 else self._save
            return True
        if cmd == 10:                                   # SET_PIXEL_FORMAT
            fmt = cast(data, POINTER(c_int))[0]
            if fmt in RAW_MODES:
                self.fmt = fmt
                return True
            return False
        if cmd == 17:                                   # GET_VARIABLE_UPDATE
            cast(data, POINTER(c_bool))[0] = False
            return True
        if cmd == 47:                                   # GET_AUDIO_VIDEO_ENABLE
            # bit0 = render video, bit3 = hard-disable audio. Cores that honour this skip the
            # expensive PPU/blitter work on frames we are going to throw away.
            cast(data, POINTER(c_int))[0] = (1 if self.want_video else 0) | 8
            return True
        if cmd == 51:                                   # GET_INPUT_BITMASK: one call instead of 16
            return True
        if cmd in (11, 16, 32, 37):                     # input descriptors, variables, av info, geometry
            return True
        return False

    def _video(self, data, w, h, pitch):
        # NULL means "same picture as the previous frame". Copying costs a lot at 60 Hz, so we
        # only do it for frames somebody will actually look at.
        if data and self.want_video:
            self.frame = (string_at(data, pitch * h), w, h, pitch)
            self.fresh = True

    def _input(self, port, device, index, id_):
        if port != 0 or device != RETRO_DEVICE_JOYPAD:
            return 0
        if id_ == RETRO_DEVICE_ID_JOYPAD_MASK:
            return self.mask & 0xFFFF
        return 1 if (self.mask >> id_) & 1 else 0

    # -- emulation ---------------------------------------------------------------------------
    def run_frames(self, n: int, mask: int, show_last: bool = True):
        """Run n frames at full speed. Only the last one is copied (unless show_last is False)."""
        self.mask = mask
        run = self.lib.retro_run
        for i in range(n):
            last = i == n - 1
            self.want_video = last and show_last
            self.fresh = False
            run()
        self.want_video = True
        if n and show_last and not self.fresh:
            # The core answered the last frame with a NULL "duplicate" and we skipped the copy of
            # the real picture before it. Run a few more frames so self.frame is current.
            for _ in range(3):
                self.fresh = False
                run()
                if self.fresh:
                    break

    def image(self, scale: int = 1) -> Image.Image:
        raw, w, h, pitch = self.frame
        im = Image.frombuffer("RGB", (w, h), raw, "raw", RAW_MODES[self.fmt], pitch, 1)
        if scale > 1:
            im = im.resize((w * scale, h * scale), Image.NEAREST)
        return im

    def png(self, scale: int) -> bytes:
        buf = io.BytesIO()
        self.image(scale).save(buf, "PNG", compress_level=1)   # level 1: fastest, still small
        return buf.getvalue()

    def jpeg(self, quality: int) -> bytes:
        buf = io.BytesIO()
        # 4:4:4 keeps pixel-art colour edges clean; the frames are tiny anyway.
        self.image(1).save(buf, "JPEG", quality=quality, subsampling=0)
        return buf.getvalue()

    def gif(self, mask, hold, total, every, scale) -> bytes:
        frames = []
        self.mask = mask
        for i in range(total):
            if i == hold:
                self.mask = 0
            sample = i % every == every - 1
            self.want_video = sample
            self.fresh = False
            self.lib.retro_run()
            if sample and self.fresh:
                frames.append(self.image(scale).quantize(colors=64, method=_QUANT))
        self.want_video = True
        self.mask = 0
        if not frames:
            self.run_frames(1, 0)
            frames.append(self.image(scale).quantize(colors=64, method=_QUANT))
        buf = io.BytesIO()
        frames[0].save(buf, "GIF", save_all=True, append_images=frames[1:], loop=0,
                       duration=max(20, int(1000 * every / self.fps)), disposal=1, optimize=False)
        return buf.getvalue()

    def save_state(self, path: str):
        size = self.lib.retro_serialize_size()
        buf = create_string_buffer(size)
        if not self.lib.retro_serialize(buf, size):
            raise RuntimeError("this core cannot save states")
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(buf.raw)
        os.replace(tmp, path)

    def load_state(self, path: str):
        with open(path, "rb") as f:
            data = f.read()
        buf = create_string_buffer(data, len(data))
        if not self.lib.retro_unserialize(buf, len(data)):
            raise RuntimeError("core rejected the save state")


def _clamp(v, lo, hi):
    return max(lo, min(hi, int(v)))


class Host:
    """Everything the worker process does besides running the core: I/O, ops, the live loop."""

    def __init__(self, out):
        self.out = out
        self.core = None
        self.cmds = queue.Queue()
        # live-mode state
        self.live = False
        self.paused = False
        self.live_mask = 0
        self.seq = deque()          # [mask, frames_left] steps queued by chat/button presses
        self.every = 2              # send every Nth emulated frame (2 => ~30 fps from a 60 Hz core)
        self.quality = 72
        self.afk = 60.0
        self.last_input = 0.0
        self.next_t = 0.0
        self.frame_i = 0
        self.last_raw = None
        self.stat_t = 0.0
        self.stat_emu = 0
        self.stat_sent = 0
        self.stat_busy = 0.0

    # -- output ------------------------------------------------------------------------------
    def _write(self, kind: bytes, payload: bytes):
        self.out.write(struct.pack("<I", len(payload) + 1) + kind + payload)
        self.out.flush()

    def reply(self, obj):
        self._write(b"R", pickle.dumps(obj, protocol=4))

    def event(self, obj):
        self._write(b"E", pickle.dumps(obj, protocol=4))

    # -- input thread ------------------------------------------------------------------------
    def reader(self, inp):
        try:
            while True:
                hdr = inp.read(4)
                if len(hdr) < 4:
                    break
                (n,) = struct.unpack("<I", hdr)
                self.cmds.put(pickle.loads(inp.read(n)))
        finally:
            self.cmds.put(None)         # bot went away

    # -- ops ---------------------------------------------------------------------------------
    def dispatch(self, msg):
        op = msg.get("op")
        silent = op in ("input", "seq", "live")
        try:
            res = self.run_op(op, msg)
        except Exception as e:
            res = {"ok": False, "err": f"{type(e).__name__}: {e}"}
            if silent:
                self.event({"ev": "error", "err": res["err"]})
                return
        if not silent:
            self.reply(res)
        if op == "quit":
            os._exit(0)     # skip retro_deinit: some cores crash on teardown, the OS reclaims everything

    def run_op(self, op, msg):
        core = self.core
        if op == "load":
            self.core = core = Core(msg["core"], msg["rom"], msg["sysdir"], msg["savedir"])
            _, w, h, _ = core.frame
            return {"ok": True, "w": w, "h": h, "fps": core.fps, "name": core.name}
        if op == "quit":
            return {"ok": True}
        if core is None:
            return {"ok": False, "err": "no game is loaded"}

        if op == "step":
            core.run_frames(_clamp(msg["hold"], 0, 240), msg["mask"], show_last=False)
            core.run_frames(max(1, _clamp(msg["settle"], 0, 240)), 0)
            return {"ok": True, "png": core.png(_clamp(msg.get("scale", 2), 1, 4))}
        if op == "steps":
            budget = 900
            for mask, frames in msg["steps"]:
                frames = min(_clamp(frames, 0, 240), budget)
                budget -= frames
                core.run_frames(frames, int(mask) & 0xFFFF, show_last=False)
                if budget <= 0:
                    break
            core.run_frames(max(1, _clamp(msg.get("settle", 4), 0, 240)), 0)
            return {"ok": True}
        if op == "snap":
            if core.frame is None:
                return {"ok": False, "err": "no frame yet"}
            return {"ok": True, "png": core.png(_clamp(msg.get("scale", 2), 1, 4))}
        if op == "clip":
            gif = core.gif(msg["mask"], _clamp(msg["hold"], 0, 240), _clamp(msg["frames"], 6, 240),
                           _clamp(msg.get("every", 3), 1, 10), _clamp(msg.get("scale", 1), 1, 3))
            return {"ok": True, "gif": gif}
        if op == "save":
            core.save_state(msg["path"])
            return {"ok": True}
        if op == "loadstate":
            core.load_state(msg["path"])
            core.run_frames(1, 0)
            self.last_raw = None
            return {"ok": True}

        # ---- live-mode ops (no reply) ----
        if op == "live":
            self.set_live(bool(msg["on"]), msg)
            return None
        if op == "input":
            mask = int(msg["mask"]) & 0xFFFF
            if mask != self.live_mask:
                self.live_mask = mask
                if mask:
                    self.wake()
            return None
        if op == "seq":
            for mask, frames in msg["steps"]:
                self.seq.append([int(mask) & 0xFFFF, _clamp(frames, 1, 240)])
            while len(self.seq) > 200:      # never let a flood build an endless backlog
                self.seq.pop()
            self.wake()
            return None
        return {"ok": False, "err": f"unknown op {op!r}"}

    # -- live loop ---------------------------------------------------------------------------
    def wake(self):
        now = time.perf_counter()
        self.last_input = now
        if self.live and self.paused:
            self.paused = False
            self.next_t = now
            self.event({"ev": "paused", "on": False})

    def set_live(self, on: bool, msg):
        core = self.core
        if core is None:
            return
        if self.paused:                 # the bot's idea of "paused" must not go stale
            self.paused = False
            self.event({"ev": "paused", "on": False})
        if not on:
            self.live = False
            self.live_mask = 0
            self.seq.clear()
            return
        stream_fps = max(5.0, min(60.0, float(msg.get("fps", 30))))
        self.every = max(1, round(core.fps / stream_fps))
        self.quality = _clamp(msg.get("quality", 72), 30, 95)
        self.afk = max(5.0, float(msg.get("afk", 60)))
        now = time.perf_counter()
        was_live = self.live
        self.live, self.paused = True, False
        self.next_t = now
        self.last_input = now
        self.stat_t, self.stat_emu, self.stat_sent, self.stat_busy = now, 0, 0, 0.0
        if not was_live and core.frame is not None:
            self.last_raw = None
            self.send_current()

    def send_current(self):
        core = self.core
        if core.frame is None:
            return
        raw = core.frame[0]
        if raw == self.last_raw:
            return
        self.last_raw = raw
        self._write(b"J", core.jpeg(self.quality))
        self.stat_sent += 1

    def live_frame(self, now):
        core = self.core
        period = 1.0 / core.fps
        lag = now - self.next_t
        if lag > period * 8:        # cannot keep up: run in slow motion instead of spiralling
            self.next_t, lag = now, 0.0
        self.next_t += period

        mask = self.live_mask
        if self.seq:
            cur = self.seq[0]
            mask |= cur[0]
            cur[1] -= 1
            if cur[1] <= 0:
                self.seq.popleft()

        # Only encode a picture when we are on time; when behind, skipping the encode is what
        # lets the emulation catch up.
        send = (self.frame_i % self.every == 0) and lag < period * 1.5
        self.frame_i += 1
        core.mask = mask
        core.want_video = send
        core.fresh = False
        core.lib.retro_run()
        core.want_video = True
        self.stat_emu += 1
        if send and core.fresh:
            self.send_current()

        if not mask and not self.seq and now - self.last_input > self.afk:
            self.paused = True
            self.event({"ev": "paused", "on": True})

    def emit_stats(self, now):
        dt = now - self.stat_t
        if dt >= 2.0:
            self.event({"ev": "stats", "emu": round(self.stat_emu / dt, 1),
                        "sent": round(self.stat_sent / dt, 1),
                        "load": round(100 * self.stat_busy / dt)})
            self.stat_t, self.stat_emu, self.stat_sent, self.stat_busy = now, 0, 0, 0.0

    def run(self):
        cmds = self.cmds
        while True:
            if not self.live or self.paused:
                msg = cmds.get()                  # block: zero CPU while idle
                if msg is None:
                    return
                self.dispatch(msg)
                continue
            now = time.perf_counter()
            if now < self.next_t:
                try:
                    msg = cmds.get(timeout=self.next_t - now)
                except queue.Empty:
                    continue
                if msg is None:
                    return
                self.dispatch(msg)
                continue
            t0 = time.perf_counter()
            self.live_frame(now)
            t1 = time.perf_counter()
            self.stat_busy += t1 - t0
            self.emit_stats(t1)
            # a slow command (clip, state load) may have eaten time; do not "catch up" 100 frames
            while True:
                try:
                    msg = cmds.get_nowait()
                except queue.Empty:
                    break
                if msg is None:
                    return
                self.dispatch(msg)


def main():
    import faulthandler
    faulthandler.enable()   # a crash inside a core prints a Python traceback to stderr (shown in the bot log)
    # Cores sometimes printf. Keep the real stdout private for the protocol and send
    # everything else (including C-level prints) to stderr.
    out = os.fdopen(os.dup(1), "wb")
    os.dup2(2, 1)
    host = Host(out)
    threading.Thread(target=host.reader, args=(sys.stdin.buffer,), daemon=True).start()
    try:
        host.run()
    finally:
        os._exit(0)   # skip retro_deinit: some cores crash on teardown and the OS reclaims everything anyway


if __name__ == "__main__":
    main()
