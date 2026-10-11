#!/usr/bin/env python3
"""
Headless libretro host used by the LUX-NET Discord bot for NES / SNES games.

One worker process = one running game. The bot spawns it on demand and kills it when the
session ends or goes idle, so the OS gives all of its memory back. Nothing is held in the
bot process itself.

Protocol (stdin/stdout): 4-byte little-endian length + pickle, request then reply.
Both ends are our own code, so pickle is fine here.
"""
import io
import os
import pickle
import struct
import sys
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

# Pixel format -> Pillow raw decoder + bytes per pixel
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
        self.frame = None       # (raw bytes, w, h, pitch) of the latest video frame
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
            self._rom = open(rom_path, "rb").read()      # must outlive the game
            gi.data = cast(c_char_p(self._rom), c_void_p)
            gi.size = len(self._rom)
        self._gi = gi
        if not lib.retro_load_game(byref(gi)):
            raise RuntimeError("core refused to load this ROM")

        av = AVInfo()
        lib.retro_get_system_av_info(byref(av))
        self.fps = av.timing.fps or 60.0
        self.run_frames(1, 0)

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
        if cmd in (11, 16, 32, 37):                     # input descriptors, variables, av info, geometry
            return True
        return False

    def _video(self, data, w, h, pitch):
        if data:                                        # NULL means "same frame as before"
            self.frame = (string_at(data, pitch * h), w, h, pitch)

    def _input(self, port, device, index, id_):
        if port != 0 or device != RETRO_DEVICE_JOYPAD:
            return 0
        if id_ == RETRO_DEVICE_ID_JOYPAD_MASK:
            return self.mask & 0xFFFF
        return 1 if (self.mask >> id_) & 1 else 0

    # -- emulation ---------------------------------------------------------------------------
    def run_frames(self, n: int, mask: int):
        self.mask = mask
        run = self.lib.retro_run
        for _ in range(n):
            run()

    def image(self, scale: int) -> Image.Image:
        raw, w, h, pitch = self.frame
        im = Image.frombuffer("RGB", (w, h), raw, "raw", RAW_MODES[self.fmt], pitch, 1)
        if scale > 1:
            im = im.resize((w * scale, h * scale), Image.NEAREST)
        return im

    def png(self, scale: int) -> bytes:
        buf = io.BytesIO()
        self.image(scale).save(buf, "PNG", compress_level=1)   # level 1: fastest, still small
        return buf.getvalue()

    def gif(self, mask, hold, total, every, scale) -> bytes:
        frames = []
        self.mask = mask
        for i in range(total):
            if i == hold:
                self.mask = 0
            self.lib.retro_run()
            if i % every == every - 1:
                frames.append(self.image(scale).quantize(colors=64, method=_QUANT))
        self.mask = 0
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
        data = open(path, "rb").read()
        buf = create_string_buffer(data, len(data))
        if not self.lib.retro_unserialize(buf, len(data)):
            raise RuntimeError("core rejected the save state")


def _clamp(v, lo, hi):
    return max(lo, min(hi, int(v)))


def main():
    import faulthandler
    faulthandler.enable()   # a crash inside a core prints a Python traceback to stderr (shown in the bot log)
    # Cores sometimes printf. Keep the real stdout private for the protocol and send
    # everything else (including C-level prints) to stderr.
    out = os.fdopen(os.dup(1), "wb", buffering=0)
    os.dup2(2, 1)
    inp = sys.stdin.buffer
    core = None

    while True:
        hdr = inp.read(4)
        if len(hdr) < 4:
            break                     # bot went away: exit so we never linger as an orphan
        (n,) = struct.unpack("<I", hdr)
        msg = pickle.loads(inp.read(n))
        op = msg.get("op")
        try:
            if op == "load":
                core = Core(msg["core"], msg["rom"], msg["sysdir"], msg["savedir"])
                _, w, h, _ = core.frame
                res = {"ok": True, "w": w, "h": h, "fps": core.fps, "name": core.name}
            elif op == "step":
                core.run_frames(_clamp(msg["hold"], 0, 240), msg["mask"])
                core.run_frames(_clamp(msg["settle"], 0, 240), 0)
                res = {"ok": True, "png": core.png(_clamp(msg.get("scale", 2), 1, 4))}
            elif op == "clip":
                gif = core.gif(msg["mask"], _clamp(msg["hold"], 0, 240), _clamp(msg["frames"], 6, 240),
                               _clamp(msg.get("every", 3), 1, 10), _clamp(msg.get("scale", 1), 1, 3))
                res = {"ok": True, "gif": gif}
            elif op == "save":
                core.save_state(msg["path"])
                res = {"ok": True}
            elif op == "loadstate":
                core.load_state(msg["path"])
                core.run_frames(1, 0)
                res = {"ok": True, "png": core.png(_clamp(msg.get("scale", 2), 1, 4))}
            elif op == "quit":
                res = {"ok": True}
            else:
                res = {"ok": False, "err": f"unknown op {op!r}"}
        except Exception as e:
            res = {"ok": False, "err": f"{type(e).__name__}: {e}"}

        data = pickle.dumps(res, protocol=4)
        out.write(struct.pack("<I", len(data)) + data)
        if op == "quit":
            break
    os._exit(0)   # skip retro_deinit: some cores crash on teardown and the OS reclaims everything anyway


if __name__ == "__main__":
    main()
