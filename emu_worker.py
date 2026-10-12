#!/usr/bin/env python3
"""
Headless libretro host used by the LUX-NET Discord bot for NES / SNES games.

One worker process = one running game. The bot spawns it on demand and kills it when the
session ends or goes idle, so the OS gives all of its memory back.

Two ways of running a game:

  LIVE   {"op": "live", "on": True}   The worker runs the core on its own clock (the core's
         native rate, ~60 Hz) and pushes EVERY frame as a JPEG plus the matching audio to the
         bot. The bot relays both over a WebSocket to browsers. While nobody is watching, or
         nobody has pressed a key for a while, the loop sleeps and uses no CPU at all.

  BURST  "steps" / "snap" / "clip"    The core runs as fast as the CPU allows for a few
         frames, then one picture comes back. Used for the Discord button / chat controls.

Wire protocol (stdin/stdout, both ends are our own code):
  bot -> worker : 4-byte little-endian length + pickle
  worker -> bot : 4-byte little-endian length + 1 kind byte + payload
                    kind b"R" = pickled reply to the last request op
                    kind b"J" = video frame:  1 pad byte + JPEG
                    kind b"A" = audio chunk:  1 pad byte + int16 PCM (interleaved if stereo)
                    kind b"E" = pickled event (stats / paused / error)
Ops that never get a reply: input, seq, live, audio.

The pad byte makes the PCM start on an even offset once the bot forwards "kind + pad + data"
unchanged to the browser, so the page can view it as an Int16Array without copying.

Performance notes
  * Pictures are never converted in Python: Pillow unpacks XRGB8888 / RGB565 / 0RGB1555
    straight from the core's buffer (handles the pitch too).
  * JPEG encoding and pipe writes run in a separate thread. Pillow and ctypes both release
    the GIL, so the core is already running the next frame while the last one is encoded.
  * The encoder only ever sees the NEWEST frame (latest wins). A slow pipe or a slow CPU
    drops pictures, never grows memory. Audio has a hard cap too.
  * No numpy, no per-frame allocation in the emulation thread beyond the audio bytes.
"""
import collections
import ctypes
import gc
import io
import json
import os
import pickle
import queue
import struct
import sys
import threading
import time

from PIL import Image

try:                                  # stdlib <=3.12, `audioop-lts` on 3.13+. Optional.
    import audioop
except ImportError:
    audioop = None

# ---------------------------------------------------------------------------------------------
# libretro ctypes bindings
# ---------------------------------------------------------------------------------------------
c = ctypes


class GameInfo(c.Structure):
    _fields_ = [("path", c.c_char_p), ("data", c.c_void_p), ("size", c.c_size_t), ("meta", c.c_char_p)]


class SystemInfo(c.Structure):
    _fields_ = [("library_name", c.c_char_p), ("library_version", c.c_char_p),
                ("valid_extensions", c.c_char_p), ("need_fullpath", c.c_bool), ("block_extract", c.c_bool)]


class GameGeometry(c.Structure):
    _fields_ = [("base_width", c.c_uint), ("base_height", c.c_uint),
                ("max_width", c.c_uint), ("max_height", c.c_uint), ("aspect_ratio", c.c_float)]


class SystemTiming(c.Structure):
    _fields_ = [("fps", c.c_double), ("sample_rate", c.c_double)]


class AVInfo(c.Structure):
    _fields_ = [("geometry", GameGeometry), ("timing", SystemTiming)]


class Variable(c.Structure):
    _fields_ = [("key", c.c_char_p), ("value", c.c_char_p)]


ENV_CB = c.CFUNCTYPE(c.c_bool, c.c_uint, c.c_void_p)
VIDEO_CB = c.CFUNCTYPE(None, c.c_void_p, c.c_uint, c.c_uint, c.c_size_t)
AUDIO_CB = c.CFUNCTYPE(None, c.c_int16, c.c_int16)
AUDIO_BATCH_CB = c.CFUNCTYPE(c.c_size_t, c.c_void_p, c.c_size_t)
POLL_CB = c.CFUNCTYPE(None)
INPUT_CB = c.CFUNCTYPE(c.c_int16, c.c_uint, c.c_uint, c.c_uint, c.c_uint)

ENV_SET_ROTATION = 1
ENV_GET_CAN_DUPE = 3
ENV_SET_PERFORMANCE_LEVEL = 8
ENV_GET_SYSTEM_DIRECTORY = 9
ENV_SET_PIXEL_FORMAT = 10
ENV_SET_INPUT_DESCRIPTORS = 11
ENV_GET_VARIABLE = 15
ENV_SET_VARIABLES = 16
ENV_GET_VARIABLE_UPDATE = 17
ENV_GET_LOG_INTERFACE = 27
ENV_GET_SAVE_DIRECTORY = 31
ENV_SET_SYSTEM_AV_INFO = 32
ENV_SET_CONTROLLER_INFO = 35
ENV_SET_GEOMETRY = 37
ENV_GET_CORE_OPTIONS_VERSION = 52

JOYPAD = 1
JOYPAD_MASK_ID = 256
MEM_SAVE_RAM = 0

# libretro pixel format id -> (Pillow raw mode, bytes per pixel)
PIXFMT = {0: ("BGR;15", 2), 1: ("BGRX", 4), 2: ("BGR;16", 2)}

MAX_BURST_FRAMES = 900          # safety cap for one steps/clip request
AUDIO_QUEUE_CHUNKS = 90         # ~1.5 s of audio at most waiting for the pipe
IDLE_STATS_EVERY = 1.0


class Live:
    last = b""       # last picture sent (identical pictures are not sent again)


def log(*a):
    print("[worker]", *a, file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------------------------
# Output: one writer thread owns stdout, encodes video and keeps every queue bounded
# ---------------------------------------------------------------------------------------------
class Out:
    def __init__(self):
        self.cv = threading.Condition()
        self.reliable = collections.deque()                       # replies + events, never dropped
        self.audio = collections.deque(maxlen=AUDIO_QUEUE_CHUNKS)  # oldest dropped if the pipe stalls
        self.slot = None            # bytearray: newest frame waiting for the encoder
        self.slot_len = 0
        self.slot_meta = None       # (w, h, pitch, rawmode)
        self.pending = False
        self.encbuf = bytearray(0)
        self.quality = 72
        self.sent = 0               # frames written (for the stats event)
        self.bio = io.BytesIO()
        self._wlock = threading.Lock()
        # Some cores printf() to stdout, which would corrupt the binary protocol. Keep a private
        # copy of the real stdout for ourselves and point fd 1 at stderr (the bot logs that).
        proto_fd = os.dup(1)
        os.dup2(2, 1)
        self._stdout = os.fdopen(proto_fd, "wb")
        threading.Thread(target=self._run, daemon=True, name="writer").start()
        threading.Thread(target=self._run_video, daemon=True, name="video").start()

    # ---- called from the emulation thread ----
    def reply(self, obj):
        self._post(b"R", pickle.dumps(obj, protocol=4))

    def event(self, obj):
        self._post(b"E", pickle.dumps(obj, protocol=4))

    def _post(self, kind, payload):
        with self.cv:
            self.reliable.append(kind + payload)
            self.cv.notify_all()

    def audio_chunk(self, pcm):
        with self.cv:
            self.audio.append(pcm)
            self.cv.notify_all()

    def publish(self, src: bytearray, n: int, meta):
        """Copy the newest frame into the slot (latest wins). ~10 microseconds."""
        with self.cv:
            if self.slot is None or len(self.slot) < n:
                self.slot = bytearray(n)
            self.slot[:n] = memoryview(src)[:n]
            self.slot_len, self.slot_meta, self.pending = n, meta, True
            self.cv.notify_all()

    # ---- writer thread ----
    def _write(self, body: bytes):
        try:
            with self._wlock:
                self._stdout.write(struct.pack("<I", len(body)) + body)
                self._stdout.flush()
        except (BrokenPipeError, ValueError, OSError):
            os._exit(0)

    def _run(self):
        """Replies, events and audio: small messages that must never wait for a JPEG."""
        while True:
            with self.cv:
                while not (self.reliable or self.audio):
                    self.cv.wait()
                if self.reliable:
                    body = self.reliable.popleft()
                else:
                    body = b"A\x00" + b"".join(self.audio)
                    self.audio.clear()
            self._write(body)

    def _run_video(self):
        while True:
            with self.cv:
                while not self.pending:
                    self.cv.wait()
                n, meta = self.slot_len, self.slot_meta
                if len(self.encbuf) < n:
                    self.encbuf = bytearray(n)
                self.encbuf[:n] = memoryview(self.slot)[:n]
                self.pending = False
            body = self._encode(meta, n)
            if body is None:
                continue
            self.sent += 1
            self._write(body)

    def _encode(self, meta, n):
        w, h, pitch, raw = meta
        try:
            im = Image.frombuffer("RGB", (w, h), self.encbuf, "raw", raw, pitch, 1)
            self.bio.seek(0)
            self.bio.truncate()
            im.save(self.bio, "JPEG", quality=self.quality, subsampling=2)
            return b"J\x00" + self.bio.getvalue()
        except Exception as e:
            self.event({"ev": "error", "err": f"encode failed: {e}"})
            return None


# ---------------------------------------------------------------------------------------------
# The emulator
# ---------------------------------------------------------------------------------------------
class Emu:
    def __init__(self, out: Out):
        self.out = out
        self.lib = None
        self.keep = []                       # keeps ctypes callbacks / strings alive
        self.pixfmt = 0
        self.options = {}
        try:
            self.options = json.loads(os.environ.get("EMU_CORE_OPTIONS", "{}"))
        except Exception:
            pass
        self.opt_bufs = {}
        self.sysdir = b"."
        self.savedir = b"."
        # video
        self.cur = bytearray(0)              # newest frame the core produced
        self.cur_addr = 0
        self.cur_meta = None
        self.cur_len = 0
        self.frame_new = False
        # audio
        self.audio_buf = bytearray()
        self.audio_on = False                # only produce chunks while a browser listens
        self.capture_audio = False
        self.mono = os.environ.get("EMU_AUDIO_MONO", "0") == "1" and audioop is not None
        # input
        self.mask = 0
        self.rom_name = "game"
        self.srm_path = None
        self.state_buf = None
        # timing
        self.fps = 60.0
        self.rate = 32000.0
        self.w = self.h = 0

    # ---- callbacks (kept tiny: they run inside retro_run) ----
    def _env(self, cmd, data):
        try:
            if cmd == ENV_SET_PIXEL_FORMAT:
                fmt = c.cast(data, c.POINTER(c.c_int)).contents.value
                if fmt in PIXFMT:
                    self.pixfmt = fmt
                    return True
                return False
            if cmd == ENV_GET_CAN_DUPE:
                c.cast(data, c.POINTER(c.c_bool)).contents.value = True
                return True
            if cmd == ENV_GET_SYSTEM_DIRECTORY:
                c.cast(data, c.POINTER(c.c_char_p)).contents.value = self.sysdir
                return True
            if cmd == ENV_GET_SAVE_DIRECTORY:
                c.cast(data, c.POINTER(c.c_char_p)).contents.value = self.savedir
                return True
            if cmd == ENV_GET_VARIABLE:
                var = c.cast(data, c.POINTER(Variable)).contents
                key = var.key.decode() if var.key else ""
                if key in self.options:
                    buf = self.opt_bufs.setdefault(key, str(self.options[key]).encode())
                    var.value = buf
                    return True
                return False
            if cmd == ENV_GET_VARIABLE_UPDATE:
                c.cast(data, c.POINTER(c.c_bool)).contents.value = False
                return True
            if cmd == ENV_GET_CORE_OPTIONS_VERSION:
                c.cast(data, c.POINTER(c.c_uint)).contents.value = 0
                return True
            if cmd in (ENV_SET_ROTATION, ENV_SET_PERFORMANCE_LEVEL, ENV_SET_INPUT_DESCRIPTORS,
                       ENV_SET_VARIABLES, ENV_SET_CONTROLLER_INFO, ENV_SET_GEOMETRY,
                       ENV_SET_SYSTEM_AV_INFO):
                return True
        except Exception as e:
            log("env error", cmd, e)
        return False                         # includes GET_LOG_INTERFACE: the core logs to stderr itself

    def _video(self, data, w, h, pitch):
        if not data or data == 0xFFFFFFFFFFFFFFFF:      # duplicate frame / hardware frame
            return
        n = pitch * h
        if n > len(self.cur):
            self.cur = bytearray(n)
            self.cur_addr = c.addressof((c.c_char * n).from_buffer(self.cur))
        c.memmove(self.cur_addr, data, n)
        self.cur_len = n
        self.cur_meta = (w, h, pitch, PIXFMT[self.pixfmt][0])
        self.w, self.h = w, h
        self.frame_new = True

    def _audio_batch(self, data, frames):
        if self.capture_audio and data:
            self.audio_buf += c.string_at(data, frames * 4)
        return frames

    def _audio_one(self, left, right):
        if self.capture_audio:
            self.audio_buf += struct.pack("<hh", left, right)

    def _poll(self):
        pass

    def _input(self, port, device, index, id_):
        if port != 0 or device != JOYPAD:
            return 0
        if id_ == JOYPAD_MASK_ID:
            return self.mask
        return (self.mask >> id_) & 1 if id_ < 16 else 0

    # ---- load / frames ----
    def load(self, core, rom, sysdir, savedir):
        os.makedirs(sysdir, exist_ok=True)
        os.makedirs(savedir, exist_ok=True)
        self.sysdir, self.savedir = sysdir.encode(), savedir.encode()
        self.lib = lib = c.CDLL(core)
        lib.retro_get_memory_data.restype = c.c_void_p
        lib.retro_get_memory_size.restype = c.c_size_t
        lib.retro_serialize_size.restype = c.c_size_t
        lib.retro_serialize.restype = c.c_bool
        lib.retro_unserialize.restype = c.c_bool
        lib.retro_load_game.restype = c.c_bool

        self.cb_env, self.cb_video = ENV_CB(self._env), VIDEO_CB(self._video)
        self.cb_audio, self.cb_batch = AUDIO_CB(self._audio_one), AUDIO_BATCH_CB(self._audio_batch)
        self.cb_poll, self.cb_input = POLL_CB(self._poll), INPUT_CB(self._input)
        lib.retro_set_environment(self.cb_env)
        lib.retro_init()
        lib.retro_set_video_refresh(self.cb_video)
        lib.retro_set_audio_sample(self.cb_audio)
        lib.retro_set_audio_sample_batch(self.cb_batch)
        lib.retro_set_input_poll(self.cb_poll)
        lib.retro_set_input_state(self.cb_input)

        sysinfo = SystemInfo()
        lib.retro_get_system_info(c.byref(sysinfo))
        with open(rom, "rb") as f:
            data = f.read()
        self.rom_data = c.create_string_buffer(data, len(data))        # must outlive the core
        info = GameInfo(rom.encode(), c.cast(self.rom_data, c.c_void_p), len(data), None)
        if not lib.retro_load_game(c.byref(info)):
            raise RuntimeError("the core refused this game file")
        if not sysinfo.need_fullpath:
            pass                                                        # data pointer already given
        av = AVInfo()
        lib.retro_get_system_av_info(c.byref(av))
        self.fps = av.timing.fps if av.timing.fps > 1 else 60.0
        self.rate = av.timing.sample_rate if av.timing.sample_rate > 1000 else 32000.0
        lib.retro_set_controller_port_device(0, JOYPAD)

        self.rom_name = os.path.splitext(os.path.basename(rom))[0]
        self.srm_path = os.path.join(savedir, self.rom_name + ".srm")
        self._load_sram()

        size = lib.retro_serialize_size()
        if size:
            self.state_buf = bytearray(size)
            self.state_addr = c.addressof((c.c_char * size).from_buffer(self.state_buf))

        # first frame so w/h are known; 0 mask, no audio
        self.run_frames(1)
        gc.collect()
        gc.freeze()                                    # core + python objects never need scanning again
        gc.set_threshold(20000, 50, 50)
        return {"ok": True, "w": self.w, "h": self.h, "fps": self.fps, "rate": int(self.rate),
                "ch": 1 if self.mono else 2}

    def run_frames(self, n, mask=None, audio=False):
        if mask is not None:
            self.mask = mask
        self.capture_audio = audio
        run = self.lib.retro_run
        for _ in range(n):
            run()
        if not audio:
            self.audio_buf.clear()

    # ---- cartridge RAM + save states ----
    def _load_sram(self):
        size = self.lib.retro_get_memory_size(MEM_SAVE_RAM)
        ptr = self.lib.retro_get_memory_data(MEM_SAVE_RAM)
        if size and ptr and os.path.exists(self.srm_path):
            try:
                with open(self.srm_path, "rb") as f:
                    data = f.read(size)
                c.memmove(ptr, data, len(data))
            except OSError as e:
                log("could not load sram:", e)

    def save_sram(self):
        try:
            size = self.lib.retro_get_memory_size(MEM_SAVE_RAM)
            ptr = self.lib.retro_get_memory_data(MEM_SAVE_RAM)
            if size and ptr:
                _atomic_write(self.srm_path, c.string_at(ptr, size))
        except Exception as e:
            log("could not save sram:", e)

    def save_state(self, path):
        if not self.state_buf:
            raise RuntimeError("this core cannot save states")
        if not self.lib.retro_serialize(c.c_void_p(self.state_addr), len(self.state_buf)):
            raise RuntimeError("save state failed")
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        _atomic_write(path, memoryview(self.state_buf))
        self.save_sram()

    def load_state(self, path):
        if not self.state_buf:
            raise RuntimeError("this core cannot load states")
        with open(path, "rb") as f:
            data = f.read()
        if len(data) > len(self.state_buf):
            raise RuntimeError("that save belongs to a different game version")
        c.memmove(self.state_addr, data, len(data))
        if not self.lib.retro_unserialize(c.c_void_p(self.state_addr), len(data)):
            raise RuntimeError("load state failed")
        self.run_frames(1, 0)                      # refresh the picture

    # ---- pictures ----
    def _image(self):
        if self.cur_meta is None:
            self.run_frames(1, 0)
        w, h, pitch, raw = self.cur_meta
        return Image.frombuffer("RGB", (w, h), self.cur, "raw", raw, pitch, 1)

    def snap_png(self, scale):
        im = self._image()
        if scale > 1:
            im = im.resize((im.width * scale, im.height * scale), Image.NEAREST)
        bio = io.BytesIO()
        im.save(bio, "PNG", compress_level=1)
        return bio.getvalue()

    def clip_gif(self, mask, hold, frames, every, scale):
        frames = max(1, min(int(frames), MAX_BURST_FRAMES))
        every = max(1, int(every))
        imgs = []
        for i in range(frames):
            self.run_frames(1, mask if i < hold else 0)
            if i % every == 0:
                im = self._image().copy()
                if scale > 1:
                    im = im.resize((im.width * scale, im.height * scale), Image.NEAREST)
                imgs.append(im)
        bio = io.BytesIO()
        imgs[0].save(bio, "GIF", save_all=True, append_images=imgs[1:], loop=0,
                     duration=max(20, int(1000 * every / self.fps)), optimize=False)
        return bio.getvalue()


def snap_async(emu, out, scale):
    """PNG encoding runs on a helper thread so a Discord picture update never stalls the game."""
    if emu.cur_meta is None:
        emu.run_frames(1, 0)
    w, h, pitch, raw = emu.cur_meta
    data = bytes(memoryview(emu.cur)[:emu.cur_len])

    def job():
        try:
            im = Image.frombuffer("RGB", (w, h), data, "raw", raw, pitch, 1)
            if scale > 1:
                im = im.resize((im.width * scale, im.height * scale), Image.NEAREST)
            bio = io.BytesIO()
            im.save(bio, "PNG", compress_level=1)
            out.reply({"ok": True, "png": bio.getvalue()})
        except Exception as e:
            out.reply({"ok": False, "err": f"snapshot failed: {e}"})

    threading.Thread(target=job, daemon=True, name="snap").start()


def _atomic_write(path, data):
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------------------------
def stdin_reader(q: "queue.SimpleQueue"):
    stream = sys.stdin.buffer
    try:
        while True:
            head = stream.read(4)
            if len(head) < 4:
                break
            (n,) = struct.unpack("<I", head)
            body = stream.read(n)
            if len(body) < n:
                break
            q.put(pickle.loads(body))
    except Exception as e:
        log("stdin reader:", e)
    q.put(None)


def main():
    out = Out()
    emu = Emu(out)
    q: "queue.SimpleQueue" = queue.SimpleQueue()
    threading.Thread(target=stdin_reader, args=(q,), daemon=True, name="stdin").start()

    live = False
    paused = False
    stream_fps = 60.0
    afk = 60.0
    last_input = time.monotonic()
    seq = collections.deque()                    # per-frame masks queued by "seq"
    input_mask = 0
    send_acc = 0.0
    # stats
    st_t = time.monotonic()
    st_frames = 0
    st_busy = 0.0
    st_sent0 = 0
    next_t = 0.0

    def set_paused(on):
        nonlocal paused, next_t
        if on != paused:
            paused = on
            out.event({"ev": "paused", "on": on})
            next_t = 0.0

    def handle(m):
        """Returns False when the worker should exit."""
        nonlocal live, stream_fps, afk, last_input, input_mask, next_t, send_acc
        if m is None:
            return False
        op = m.get("op")
        try:
            if op == "input":
                input_mask = int(m["mask"]) & 0x0FFF
                last_input = time.monotonic()
                set_paused(False)
            elif op == "seq":
                for mask, frames in m["steps"]:
                    seq.extend([int(mask) & 0x0FFF] * int(frames))
                last_input = time.monotonic()
                set_paused(False)
            elif op == "live":
                live = bool(m["on"])
                stream_fps = float(m.get("fps") or 60)
                out.quality = int(m.get("quality") or 72)
                afk = float(m.get("afk") or 0)
                last_input = time.monotonic()
                send_acc = 0.0
                next_t = 0.0
                Live.last = b""
                if live:
                    set_paused(False)
                else:
                    seq.clear()
                    input_mask = 0
                    emu.audio_on = False
            elif op == "audio":
                emu.audio_on = bool(m["on"])
            elif op == "load":
                res = emu.load(m["core"], m["rom"], m["sysdir"], m["savedir"])
                out.reply(res)
            elif op == "steps":
                total = 0
                for mask, frames in m["steps"]:
                    n = min(int(frames), MAX_BURST_FRAMES - total)
                    emu.run_frames(max(0, n), int(mask) & 0x0FFF)
                    total += n
                emu.run_frames(max(1, int(m.get("settle", 1))), 0)
                out.reply({"ok": True})
                next_t = 0.0
            elif op == "snap":
                snap_async(emu, out, max(1, int(m.get("scale", 1))))
            elif op == "clip":
                out.reply({"ok": True, "gif": emu.clip_gif(int(m.get("mask", 0)) & 0x0FFF, int(m.get("hold", 8)),
                                                             m.get("frames", 90), m.get("every", 3),
                                                             max(1, int(m.get("scale", 1))))})
                next_t = 0.0
            elif op == "save":
                emu.save_state(m["path"])
                out.reply({"ok": True})
            elif op == "loadstate":
                emu.load_state(m["path"])
                out.reply({"ok": True})
            else:
                out.reply({"ok": False, "err": f"unknown op {op!r}"})
        except Exception as e:
            log(f"op {op} failed: {e}")
            if op in ("input", "seq", "live", "audio"):
                out.event({"ev": "error", "err": f"{op}: {e}"})
            else:
                out.reply({"ok": False, "err": str(e)})
        return True

    while True:
        # ---- sleeping: nothing to run, block until the bot says something ----
        if not (live and not paused and emu.lib is not None):
            if not handle(q.get()):
                break
            continue

        # ---- running in real time ----
        loop_start = time.perf_counter()
        exit_now = False
        while True:
            try:
                msg = q.get_nowait()
            except queue.Empty:
                break
            if not handle(msg):
                exit_now = True
                break
        if exit_now:
            break
        if not (live and not paused):
            continue

        now = time.perf_counter()
        frame_dt = 1.0 / emu.fps
        if next_t == 0.0 or now - next_t > 0.25:       # first frame, or fell far behind: re-sync, never "catch up"
            if next_t == 0.0:                          # (re)starting: stats should not include the sleep
                st_t, st_frames, st_busy, st_sent0 = time.monotonic(), 0, 0.0, out.sent
            next_t = now

        mask = seq.popleft() if seq else input_mask
        emu.run_frames(1, mask, audio=emu.audio_on)
        st_frames += 1

        if emu.audio_on and emu.audio_buf:
            pcm = bytes(emu.audio_buf)
            emu.audio_buf.clear()
            if emu.mono:
                pcm = audioop.tomono(pcm, 2, 0.5, 0.5)
            out.audio_chunk(pcm)

        # honour EMU_STREAM_FPS when it is below the core rate (e.g. 30 on a weak CPU)
        send_acc += min(stream_fps, emu.fps)
        if send_acc >= emu.fps - 1e-6:
            send_acc -= emu.fps
            if emu.frame_new and emu.cur_meta and time.perf_counter() - next_t < 0.03:
                snapshot = bytes(memoryview(emu.cur)[:emu.cur_len])
                if snapshot != Live.last:         # unchanged picture (menus, pauses): nothing to encode or send
                    Live.last = snapshot
                    out.publish(emu.cur, emu.cur_len, emu.cur_meta)
        emu.frame_new = False

        # AFK: nobody touched a key for a while and nothing is held -> sleep with 0 CPU
        t = time.monotonic()
        if afk and not seq and input_mask == 0 and t - last_input > afk:
            set_paused(True)

        st_busy += time.perf_counter() - loop_start
        if t - st_t >= IDLE_STATS_EVERY:
            span = t - st_t
            out.event({"ev": "stats", "emu": round(st_frames / span), "sent": round((out.sent - st_sent0) / span),
                       "load": min(100, round(100 * st_busy / span))})
            st_t, st_frames, st_busy, st_sent0 = t, 0, 0.0, out.sent

        next_t += frame_dt
        delay = next_t - time.perf_counter()
        if delay > 0:
            time.sleep(delay)

    # stdin closed: persist cartridge RAM and leave immediately (daemon threads die with us)
    try:
        if emu.lib is not None:
            emu.save_sram()
    finally:
        try:
            out._stdout.flush()
        finally:
            os._exit(0)


if __name__ == "__main__":
    main()
