# Copyright (C) 2026 by xcentaurix
# License: GNU General Public License v3.0
#
#   TSRemap - gives an MPEG transport stream segment the PIDs of another one, and nothing else.
#
#   A muxed HLS stream whose pieces have different PID layouts (an ad clip next to the programme) used to be remuxed with ffmpeg
#   (stream copy) to get one layout. The remux also changes what the box's decoders care about: the PCR was every 0.1 s instead of
#   per frame and ran 0.7 s instead of 1.0 s behind the PTS, and picture and sound were out of sync for the rest of the channel.
#
#   Here only the PIDs are changed: the packets of the video and the audio stream (the first of each) get the PIDs of the layout
#   that the player has been fed so far, a PAT and a PMT of that layout take the place of those of the segment, and every other
#   packet (other streams, SDT, null packets ...) is dropped. The PES packets, the PCR, the PTS/DTS, the packet boundaries of the
#   source are not touched, so the segment is what the CDN made it except for its PIDs, like the segments that need no change.
#   The continuity counters are the business of the caller (_patch_continuity).

PACKET = 188
_SYNC = 0x47
_PAT_PID = 0


def crc32_mpeg(data: bytes) -> int:
    """CRC-32/MPEG-2 of PSI sections (polynomial 0x04C11DB7, start value 0xFFFFFFFF, not reflected, no final xor)."""
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte << 24
        for _ in range(8):
            crc = ((crc << 1) ^ 0x04C11DB7) & 0xFFFFFFFF if crc & 0x80000000 else (crc << 1) & 0xFFFFFFFF
    return crc


def _section_packet(pid: int, cc: int, section: bytes) -> bytes:
    """One packet that carries *section* (it has to fit in it: 183 bytes)."""
    body = b'\x00' + section   # pointer field
    if len(body) > PACKET - 4:
        raise ValueError('section does not fit in one packet')
    return bytes((_SYNC, 0x40 | (pid >> 8), pid & 0xFF, 0x10 | (cc & 0x0F))) + body + b'\xff' * (PACKET - 4 - len(body))


def _with_crc(section: bytes) -> bytes:
    return section + crc32_mpeg(section).to_bytes(4, 'big')


def _find_section(ts: bytes, pid: int, table_id: int, limit: int = 200) -> 'int | None':
    """Offset of the first section of table *table_id* that starts in a packet of *pid* within the first *limit* packets."""
    for off in range(0, min(len(ts) - PACKET + 1, PACKET * limit), PACKET):
        if ts[off] != _SYNC or not ts[off + 1] & 0x40 or (((ts[off + 1] & 0x1F) << 8) | ts[off + 2]) != pid:
            continue
        afc = (ts[off + 3] >> 4) & 3
        if not afc & 1:
            continue
        sec = off + 4 + (1 + ts[off + 4] if afc & 2 else 0)
        sec += 1 + ts[sec]   # pointer field
        if sec + 12 <= off + PACKET and ts[sec] == table_id:
            return sec
    return None


def _pmt_streams(ts: bytes, pmt_pid: int) -> 'tuple | None':
    """(program number, version, {pid: (stream type, es info bytes)}) of the PMT of *pmt_pid*, None if it is not in one packet."""
    sec = _find_section(ts, pmt_pid, 2)
    if sec is None:
        return None
    length = ((ts[sec + 1] & 0x0F) << 8) | ts[sec + 2]
    end = sec + 3 + length - 4
    if end > len(ts) or (sec // PACKET) != ((end + 3) // PACKET):
        return None
    program = (ts[sec + 3] << 8) | ts[sec + 4]
    version = (ts[sec + 5] >> 1) & 0x1F
    pos = sec + 12 + (((ts[sec + 10] & 0x0F) << 8) | ts[sec + 11])
    streams: dict = {}
    while pos + 5 <= end:
        info_len = ((ts[pos + 3] & 0x0F) << 8) | ts[pos + 4]
        pid = ((ts[pos + 1] & 0x1F) << 8) | ts[pos + 2]
        streams[pid] = (ts[pos], bytes(ts[pos + 5:pos + 5 + info_len]))
        pos += 5 + info_len
    return program, version, streams


def _pat_header(ts: bytes) -> 'tuple | None':
    """(transport stream id, version) of the PAT in *ts*."""
    sec = _find_section(ts, _PAT_PID, 0)
    if sec is None:
        return None
    return (ts[sec + 3] << 8) | ts[sec + 4], (ts[sec + 5] >> 1) & 0x1F


def remap_pids(ts: bytes, source: 'tuple | None', target: 'tuple | None', video_types: tuple, audio_types: tuple) -> 'bytes | None':
    """*ts* (a whole number of packets) with the PIDs of *target*, both layouts as (PMT pid, PCR pid, ((stream type, pid), ...)).

    Its first video stream and its first audio stream become the ones of *target*, a PAT and a PMT of *target*'s PMT PID
    replace those of *ts*, everything else is dropped. None if that can't be done without guessing (no video, an audio stream
    on one side only, the PCR on a stream that is dropped, a PMT in more than one packet, packets out of sync): the
    caller has to treat the segment in another way."""
    if source is None or target is None or not ts or len(ts) % PACKET:
        return None
    s_pmt, s_pcr, s_streams = source
    t_pmt, _, t_streams = target

    def first(streams, types):
        return next((pid for stream_type, pid in streams if stream_type in types), None)

    s_video, t_video = first(s_streams, video_types), first(t_streams, video_types)
    s_audio, t_audio = first(s_streams, audio_types), first(t_streams, audio_types)
    if s_video is None or t_video is None or (s_audio is None) != (t_audio is None):
        return None
    pidmap = {s_video: t_video}
    if s_audio is not None:
        pidmap[s_audio] = t_audio
    if s_pcr not in pidmap or len({t_pmt, *pidmap.values()}) != len(pidmap) + 1 or _PAT_PID in (t_pmt, *pidmap.values()):
        return None
    pmt = _pmt_streams(ts, s_pmt)
    pat = _pat_header(ts)
    if pmt is None or pat is None:
        return None
    program, version, infos = pmt
    pat_section = _with_crc(bytes((0x00, 0xB0, 13, pat[0] >> 8, pat[0] & 0xFF, 0xC1 | (pat[1] << 1), 0, 0,
                                   program >> 8, program & 0xFF, 0xE0 | (t_pmt >> 8), t_pmt & 0xFF)))
    loop = b''
    source_of = {new: pid for pid, new in pidmap.items()}
    for _, new in t_streams:   # the kept streams in the order of the target, with the type and the descriptors of the source
        pid = source_of.get(new)
        if pid is not None and pid in infos:
            stream_type, info = infos[pid]
            loop += bytes((stream_type, 0xE0 | (new >> 8), new & 0xFF, 0xF0 | (len(info) >> 8), len(info) & 0xFF)) + info
    new_pcr = pidmap[s_pcr]
    body = bytes((program >> 8, program & 0xFF, 0xC1 | (version << 1), 0, 0, 0xE0 | (new_pcr >> 8), new_pcr & 0xFF, 0xF0, 0)) + loop
    pmt_section = _with_crc(bytes((0x02, 0xB0 | ((len(body) + 4) >> 8), (len(body) + 4) & 0xFF)) + body)
    try:
        pat_packet = _section_packet(_PAT_PID, 0, pat_section)
        pmt_packet = _section_packet(t_pmt, 0, pmt_section)
    except ValueError:
        return None

    out = bytearray()
    for off in range(0, len(ts), PACKET):
        if ts[off] != _SYNC:
            return None
        pid = ((ts[off + 1] & 0x1F) << 8) | ts[off + 2]
        if pid == _PAT_PID or pid == s_pmt:
            if ts[off + 1] & 0x40:   # a section starts: the whole PAT/PMT of the target takes its place
                packet = bytearray(pat_packet if pid == _PAT_PID else pmt_packet)
                packet[3] = (packet[3] & 0xF0) | (ts[off + 3] & 0x0F)
                out += packet
            continue
        new = pidmap.get(pid)
        if new is None:
            continue
        out += ts[off:off + PACKET]
        out[-PACKET + 1] = (ts[off + 1] & 0xE0) | (new >> 8)
        out[-PACKET + 2] = new & 0xFF
    return bytes(out)
