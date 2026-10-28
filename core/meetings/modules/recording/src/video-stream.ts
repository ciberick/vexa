import { spawn, type ChildProcess } from 'child_process';
import { log } from './log';

/**
 * Continuous screen+audio capture for presentation-style meetings.
 *
 * ONE ffmpeg process grabs the Xvfb display (x11grab) and a dedicated PulseAudio monitor and writes a
 * single continuous WebM byte stream to stdout. The stream is cut BY BYTES (never by container
 * structure) into chunks that are handed to `onChunk`; the first chunk carries the EBML header and the
 * rest are plain continuations, so concatenating the chunks in seq order reproduces the stream exactly
 * — the same invariant the server's WebM byte-concat master already relies on for MediaRecorder parts.
 *
 * Tuned for slides / desktop sharing: few frames per second, a keyframe every ~10 s, screen-content
 * tuning and a hard bitrate ceiling. Browser-free and spawn-injectable, so it is provable offline.
 */

export interface VideoStreamConfig {
  /** X display to grab (e.g. ':99'). */
  display: string;
  /** Frames per second of the capture (5 default; 10 max). */
  fps: number;
  width?: number;
  height?: number;
  /** PulseAudio source carrying the meeting mix (e.g. 'meeting_sink.monitor'); null → video only. */
  audioSource?: string | null;
  /** Seconds between keyframes. */
  keyframeSeconds?: number;
  /** Video bitrate ceiling in kb/s. */
  maxVideoKbps?: number;
  /** Opus bitrate in kb/s (mono). */
  audioKbps?: number;
  /** libvpx-vp9 cpu-used (higher = cheaper CPU, larger output). */
  cpuUsed?: number;
  /** libvpx-vp9 CRF (lower = better quality). */
  crf?: number;
}

export const VIDEO_FPS_DEFAULT = 5;
export const VIDEO_FPS_MAX = 10;

/** Clamp a requested fps into the supported range; non-finite → default. */
export function normalizeFps(fps: number | undefined): number {
  if (!Number.isFinite(fps) || (fps as number) <= 0) return VIDEO_FPS_DEFAULT;
  return Math.min(VIDEO_FPS_MAX, Math.max(1, Math.round(fps as number)));
}

/** The ffmpeg argv for the continuous WebM stream on stdout. Pure. */
export function buildVideoStreamArgs(cfg: VideoStreamConfig): string[] {
  const fps = normalizeFps(cfg.fps);
  const width = cfg.width ?? 1920;
  const height = cfg.height ?? 1080;
  const gop = fps * (cfg.keyframeSeconds ?? 10);
  const args: string[] = ['-hide_banner', '-loglevel', 'error'];

  args.push(
    '-f', 'x11grab', '-draw_mouse', '1', '-framerate', String(fps),
    '-video_size', `${width}x${height}`, '-i', cfg.display,
  );
  if (cfg.audioSource) {
    args.push('-thread_queue_size', '1024', '-f', 'pulse', '-i', cfg.audioSource);
  }
  args.push('-map', '0:v:0');
  if (cfg.audioSource) args.push('-map', '1:a:0');

  args.push(
    '-c:v', 'libvpx-vp9', '-pix_fmt', 'yuv420p',
    '-deadline', 'realtime', '-cpu-used', String(cfg.cpuUsed ?? 6), '-row-mt', '1',
    '-tune-content', 'screen',
    '-crf', String(cfg.crf ?? 36), '-b:v', `${cfg.maxVideoKbps ?? 900}k`,
    '-g', String(gop), '-keyint_min', String(gop),
  );
  if (cfg.audioSource) {
    args.push('-c:a', 'libopus', '-ac', '1', '-ar', '48000', '-b:a', `${cfg.audioKbps ?? 40}k`);
  }
  // `-live 1` + short clusters: a stream with no seek table that is playable from any cluster boundary
  // and loses at most one short cluster when the process dies.
  args.push('-f', 'webm', '-live', '1', '-cluster_time_limit', '2000', 'pipe:1');
  return args;
}

export interface ByteChunkerOptions {
  /** Flush whatever is buffered at least this often (ms). */
  intervalMs: number;
  /** Flush as soon as this many bytes are buffered. */
  maxBytes: number;
  onChunk: (seq: number, bytes: Uint8Array) => void;
}

/** Cuts a byte stream into ordered chunks by time or size. No awareness of container structure. */
export class ByteChunker {
  private parts: Buffer[] = [];
  private size = 0;
  private seq = 0;
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(private opts: ByteChunkerOptions) {}

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => this.flush(), this.opts.intervalMs);
    this.timer.unref?.();
  }

  push(buf: Buffer): void {
    if (buf.length === 0) return;
    this.parts.push(buf);
    this.size += buf.length;
    if (this.size >= this.opts.maxBytes) this.flush();
  }

  /** Emit everything buffered as one chunk (no-op when empty). */
  flush(): void {
    if (this.size === 0) return;
    const out = Buffer.concat(this.parts, this.size);
    this.parts = [];
    this.size = 0;
    this.opts.onChunk(this.seq++, new Uint8Array(out.buffer, out.byteOffset, out.length));
  }

  /** Stop the timer, flush the remainder, and return the NEXT seq (where a final marker belongs). */
  end(): number {
    if (this.timer) { clearInterval(this.timer); this.timer = null; }
    this.flush();
    return this.seq;
  }
}

export type SpawnFn = (cmd: string, args: string[]) => ChildProcess;

export interface VideoStreamRecorderOptions {
  config: VideoStreamConfig;
  /** ~10 s per chunk by default. */
  chunkMs?: number;
  /** Hard per-chunk byte cap (4 MiB default). */
  maxChunkBytes?: number;
  /** Graceful-stop budget before SIGKILL (ms). */
  stopTimeoutMs?: number;
  /** One ordered chunk; the last call has isFinal=true and zero bytes. */
  onChunk: (seq: number, isFinal: boolean, bytes: Uint8Array) => void;
  /** ffmpeg ended without stop() — the take is partial. */
  onUnexpectedExit?: (code: number | null) => void;
  spawnFn?: SpawnFn;
  ffmpegPath?: string;
}

/** Supervises the ffmpeg stream and feeds the chunker. */
export class VideoStreamRecorder {
  private proc: ChildProcess | null = null;
  private chunker: ByteChunker;
  private stopping = false;
  private finished = false;
  private closed: Promise<void> = Promise.resolve();

  constructor(private opts: VideoStreamRecorderOptions) {
    this.chunker = new ByteChunker({
      intervalMs: opts.chunkMs ?? 10_000,
      maxBytes: opts.maxChunkBytes ?? 4 * 1024 * 1024,
      onChunk: (seq, bytes) => opts.onChunk(seq, false, bytes),
    });
  }

  start(): void {
    if (this.proc) return;
    const args = buildVideoStreamArgs(this.opts.config);
    log(`[VideoStream] ffmpeg ${args.join(' ')}`);
    const spawnFn: SpawnFn = this.opts.spawnFn ?? ((cmd, a) => spawn(cmd, a, { stdio: ['pipe', 'pipe', 'pipe'] }));
    const proc = spawnFn(this.opts.ffmpegPath ?? 'ffmpeg', args);
    this.proc = proc;
    this.chunker.start();

    proc.stdout?.on('data', (b: Buffer) => this.chunker.push(b));
    proc.stderr?.on('data', (b: Buffer) => {
      const t = b.toString().trim();
      if (t) log(`[VideoStream] ffmpeg: ${t.slice(0, 500)}`);
    });
    proc.on('error', (e) => { log(`[VideoStream] spawn error: ${e.message}`); this.finish(null); });

    this.closed = new Promise<void>((resolve) => {
      proc.on('close', (code) => { this.finish(code); resolve(); });
    });
  }

  private finish(code: number | null): void {
    if (this.finished) return;
    this.finished = true;
    const next = this.chunker.end();
    this.opts.onChunk(next, true, new Uint8Array(0));
    if (!this.stopping) {
      log(`[VideoStream] ffmpeg exited unexpectedly (code=${code}) — take is partial`);
      this.opts.onUnexpectedExit?.(code);
    }
  }

  /** Ask ffmpeg to finalize ('q' on stdin), escalate to SIGTERM then SIGKILL, flush the tail + final marker. */
  async stop(): Promise<void> {
    const proc = this.proc;
    if (!proc || this.finished) return;
    this.stopping = true;
    const budget = this.opts.stopTimeoutMs ?? 3000;
    try { proc.stdin?.write('q\n'); } catch { /* already closed */ }
    const term = setTimeout(() => proc.kill('SIGTERM'), budget);
    const kill = setTimeout(() => proc.kill('SIGKILL'), budget * 2);
    try { await this.closed; } finally { clearTimeout(term); clearTimeout(kill); }
  }
}
