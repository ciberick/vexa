/**
 * Video recording for the Google Meet bot — the opt-in screen+audio stream, delivered chunk by chunk.
 *
 * @vexa/recording's VideoStreamRecorder produces ONE continuous WebM byte stream cut into ordered
 * chunks. This file owns the DELIVER half for it: an upload queue that is
 *   • serialized (chunks land in seq order, the first one carrying the container header),
 *   • receipted (each chunk carries its sha256; an HTTP 200 whose body says `pending` is a retry,
 *     not a receipt),
 *   • BOUNDED (a spool cap in bytes — when the network cannot keep up the video stops and the take is
 *     declared partial; chunks are never dropped silently to keep up appearances),
 *   • drainable with a deadline (the worker's shutdown budget is short; the tail is uploaded within
 *     it or reported as lost, never awaited forever).
 * The transcript and the audio recording are independent of every failure here.
 */
import { createHash } from 'node:crypto';
import { RecordingService, VideoStreamRecorder, normalizeFps, type VideoStreamConfig } from '@vexa/recording';
import type { Invocation } from './config.js';

export const VIDEO_SPOOL_MAX_BYTES_DEFAULT = 64 * 1024 * 1024;
export const VIDEO_DRAIN_DEADLINE_MS_DEFAULT = 10_000;
/** The meeting's audio mix lives on a dedicated PulseAudio sink (entrypoint.sh). */
export const MEETING_SINK_MONITOR = 'meeting_sink.monitor';

/** One chunk as the uploader sees it. `partial` is set on the final marker of a degraded take. */
export interface VideoChunkUpload {
  seq: number;
  isFinal: boolean;
  bytes: Uint8Array;
  sha256: string;
  partial: boolean;
}
export type VideoChunkUploader = (chunk: VideoChunkUpload) => Promise<void>;

export interface VideoUploaderOptions {
  inv: Invocation;
  fps: number;
  log: (msg: string) => void;
}

/** The default uploader: RecordingService.uploadChunk as media_type=video, receipt required. */
export function defaultVideoUploader(o: VideoUploaderOptions): VideoChunkUploader {
  const url = o.inv.recordingUploadUrl;
  const svc = new RecordingService(o.inv.meeting_id ?? 0, o.inv.connectionId ?? '');
  const token = o.inv.internalSecret ?? '';
  return async (c) => {
    if (!url) { o.log(`video: no recordingUploadUrl — chunk ${c.seq} (${c.bytes.length}B) NOT uploaded`); return; }
    await svc.uploadChunk(url, token, Buffer.from(c.bytes), c.seq, c.isFinal, 'webm', {
      mediaType: 'video',
      expectedMediaTypes: ['audio', 'video'],
      checksumSha256: c.sha256,
      requireStored: true,
      extra: {
        assembly_strategy: 'stream_bytes_v1', width: 1920, height: 1080, fps: o.fps,
        contains_audio: true, ...(c.partial ? { coverage: 'partial' } : {}),
      },
    });
  };
}

export interface VideoRecordingSinkOptions {
  upload: VideoChunkUploader;
  spoolMaxBytes?: number;
  log?: (msg: string) => void;
  /** Called once when the spool cap is exceeded (the caller stops the stream). */
  onOverflow?: () => void;
}

export interface DrainResult {
  /** Chunks still queued/in flight when the deadline passed. */
  pending: number;
  /** True when the take is not complete (overflow, ffmpeg death, or an undrained tail). */
  partial: boolean;
}

/** The bounded, serialized, drainable upload queue. */
export class VideoRecordingSink {
  private queue: Promise<void> = Promise.resolve();
  private pendingBytes = 0;
  private pendingCount = 0;
  private overflowed = false;
  private degraded = false;
  private finalQueued = false;
  private readonly spoolMax: number;
  private readonly log: (m: string) => void;

  constructor(private opts: VideoRecordingSinkOptions) {
    this.spoolMax = opts.spoolMaxBytes ?? VIDEO_SPOOL_MAX_BYTES_DEFAULT;
    this.log = opts.log ?? (() => { /* silent */ });
  }

  /** The take is known to be incomplete. */
  markPartial(): void { this.degraded = true; }

  /** Feed one chunk from the recorder. After an overflow only the final marker is still sent. */
  chunk(seq: number, isFinal: boolean, bytes: Uint8Array): void {
    if (this.finalQueued) return;
    if (this.overflowed && !isFinal) return;
    if (!isFinal && this.pendingBytes + bytes.length > this.spoolMax) {
      this.overflowed = true;
      this.degraded = true;
      this.log(`video: spool cap ${this.spoolMax}B exceeded at chunk ${seq} — stopping video, take is partial`);
      this.opts.onOverflow?.();
      return;
    }
    if (isFinal) this.finalQueued = true;
    const partial = this.degraded && isFinal;
    const sha256 = createHash('sha256').update(bytes).digest('hex');
    this.pendingBytes += bytes.length;
    this.pendingCount++;
    this.queue = this.queue
      .then(() => this.opts.upload({ seq, isFinal, bytes, sha256, partial }))
      .catch((e) => {
        this.degraded = true;   // a chunk is missing from storage → the prefix is all that is contiguous
        this.log(`video: chunk ${seq} upload failed permanently — take is partial: ${String(e)}`);
      })
      .finally(() => { this.pendingBytes -= bytes.length; this.pendingCount--; });
  }

  /** Wait for the queue to empty, at most `deadlineMs`. */
  async drain(deadlineMs: number = VIDEO_DRAIN_DEADLINE_MS_DEFAULT): Promise<DrainResult> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = await Promise.race([
      this.queue.then(() => false),
      new Promise<boolean>((resolve) => { timer = setTimeout(() => resolve(true), deadlineMs); }),
    ]);
    if (timer) clearTimeout(timer);
    if (timedOut) {
      this.degraded = true;
      this.log(`video: drain deadline ${deadlineMs}ms hit with ${this.pendingCount} chunk(s) still pending`);
    }
    return { pending: this.pendingCount, partial: this.degraded };
  }
}

export interface VideoRecordingOptions {
  inv: Invocation;
  log: (msg: string) => void;
  /** Test seams. */
  upload?: VideoChunkUploader;
  recorderFactory?: (cfg: ConstructorParameters<typeof VideoStreamRecorder>[0]) => Pick<VideoStreamRecorder, 'start' | 'stop'>;
  display?: string;
  spoolMaxBytes?: number;
  drainDeadlineMs?: number;
  chunkMs?: number;
}

/** True when this invocation asked for video (and the platform supports it). */
export function videoRecordingRequested(inv: Invocation): boolean {
  return !!inv.recordingEnabled && inv.platform === 'google_meet' && inv.videoRecording?.enabled === true;
}

/**
 * Start the video stream; returns the teardown. Everything is best-effort: a start failure or an
 * ffmpeg death degrades video only (the live pipeline already treats a recording-start throw as
 * non-fatal and keeps the bot seated). Teardown stops ffmpeg, then drains the tail within the deadline.
 */
export async function startVideoRecording(o: VideoRecordingOptions): Promise<() => Promise<void>> {
  const fps = normalizeFps(o.inv.videoRecording?.fps);
  const upload = o.upload ?? defaultVideoUploader({ inv: o.inv, fps, log: o.log });
  let recorder: Pick<VideoStreamRecorder, 'start' | 'stop'> | null = null;
  const sink = new VideoRecordingSink({
    upload, spoolMaxBytes: o.spoolMaxBytes, log: o.log,
    onOverflow: () => { void recorder?.stop(); },
  });
  const config: VideoStreamConfig = {
    display: o.display ?? process.env.DISPLAY ?? ':99',
    fps,
    audioSource: MEETING_SINK_MONITOR,
  };
  const recOpts = {
    config,
    chunkMs: o.chunkMs,
    onChunk: (seq: number, isFinal: boolean, bytes: Uint8Array) => sink.chunk(seq, isFinal, bytes),
    onUnexpectedExit: () => sink.markPartial(),
  };
  recorder = o.recorderFactory ? o.recorderFactory(recOpts) : new VideoStreamRecorder(recOpts);
  recorder.start();
  o.log(`video: recording started (${fps} fps, audio from ${MEETING_SINK_MONITOR})`);

  return async () => {
    await recorder?.stop().catch((e) => o.log(`video: stop failed: ${String(e)}`));
    const r = await sink.drain(o.drainDeadlineMs);
    o.log(`video: closed (pending=${r.pending}, partial=${r.partial})`);
  };
}
