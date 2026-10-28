/**
 * L3 — video recording delivery (the bounded, receipted, drainable queue + the opt-in gate). OFFLINE:
 * a fake uploader and a fake recorder; no ffmpeg, no HTTP.
 * Run: npx tsx src/video-recording.test.ts
 */
import { createHash } from 'node:crypto';
import {
  VideoRecordingSink, startVideoRecording, videoRecordingRequested,
  type VideoChunkUpload,
} from './video-recording.js';
import type { Invocation } from './config.js';

let failed = 0;
const check = (name: string, cond: boolean, detail = '') => {
  console.log(`  ${cond ? '✅' : '❌'} ${name}${cond ? '' : '  — ' + detail}`);
  if (!cond) failed++;
};
const inv = (over: Partial<Invocation> = {}): Invocation => ({
  platform: 'google_meet', meetingUrl: 'https://meet.google.com/abc-defg-hij', botName: 'Vexa',
  redisUrl: 'redis://localhost:6379', recordingEnabled: true, connectionId: 'conn-xyz',
  videoRecording: { enabled: true, fps: 5 }, ...over,
});
const bytes = (n: number, v = 1) => new Uint8Array(n).fill(v);
const tick = () => new Promise((r) => setImmediate(r));

async function run() {
  // ── the opt-in gate ──
  check('requested only for google_meet + recordingEnabled + videoRecording.enabled',
    videoRecordingRequested(inv()) &&
    !videoRecordingRequested(inv({ videoRecording: undefined })) &&
    !videoRecordingRequested(inv({ videoRecording: { enabled: false } })) &&
    !videoRecordingRequested(inv({ recordingEnabled: false })) &&
    !videoRecordingRequested(inv({ platform: 'zoom' })));

  // ── ordered, hashed delivery ──
  {
    const seen: VideoChunkUpload[] = [];
    const sink = new VideoRecordingSink({ upload: async (c) => { seen.push(c); } });
    sink.chunk(0, false, bytes(3, 7)); sink.chunk(1, false, bytes(2, 9)); sink.chunk(2, true, new Uint8Array(0));
    const r = await sink.drain(1000);
    check('chunks are delivered in seq order with the final marker last', seen.map((c) => `${c.seq}${c.isFinal ? 'F' : ''}`).join() === '0,1,2F');
    check('each chunk carries the sha256 of its bytes', seen[0].sha256 === createHash('sha256').update(bytes(3, 7)).digest('hex'));
    check('a clean take drains with nothing pending and is not partial', r.pending === 0 && !r.partial && !seen[2].partial);
  }

  // ── a permanent failure marks the take partial; later chunks still flow ──
  {
    const seen: number[] = [];
    const sink = new VideoRecordingSink({ upload: async (c) => { if (c.seq === 1) throw new Error('boom'); seen.push(c.seq); } });
    sink.chunk(0, false, bytes(1)); sink.chunk(1, false, bytes(1)); sink.chunk(2, true, new Uint8Array(0));
    const r = await sink.drain(1000);
    check('failed chunk → partial; the queue keeps moving', r.partial && seen.join() === '0,2');
  }

  // ── spool cap: stop, mark partial, send ONLY the final (flagged partial) ──
  {
    const seen: VideoChunkUpload[] = []; let release!: () => void; let overflow = 0;
    const gate = new Promise<void>((r) => { release = r; });
    const sink = new VideoRecordingSink({
      spoolMaxBytes: 10, onOverflow: () => { overflow++; },
      upload: async (c) => { await gate; seen.push(c); },
    });
    sink.chunk(0, false, bytes(6)); sink.chunk(1, false, bytes(6));   // 12 > 10 → overflow, chunk 1 refused
    sink.chunk(2, false, bytes(1));                                    // refused too (no silent mid-stream gaps)
    sink.chunk(3, true, new Uint8Array(0));                            // the final still goes out
    release();
    const r = await sink.drain(1000);
    check('overflow fires once, drops later chunks, keeps the final', overflow === 1 && seen.map((c) => c.seq).join() === '0,3');
    check('the final of a degraded take is flagged partial', seen[1].isFinal && seen[1].partial && r.partial);
  }

  // ── drain deadline: a stuck uploader does not hold shutdown ──
  {
    const sink = new VideoRecordingSink({ upload: () => new Promise<void>(() => { /* never */ }) });
    sink.chunk(0, false, bytes(1));
    const t0 = Date.now();
    const r = await sink.drain(60);
    check('drain returns at the deadline with the tail reported pending + partial', Date.now() - t0 < 1000 && r.pending === 1 && r.partial);
  }

  // ── startVideoRecording: stop → recorder stopped first, then the tail is drained ──
  {
    const order: string[] = []; const seen: number[] = [];
    let emit!: (seq: number, fin: boolean, b: Uint8Array) => void;
    const stop = await startVideoRecording({
      inv: inv(), log: () => {}, upload: async (c) => { seen.push(c.seq); },
      recorderFactory: (o) => {
        emit = o.onChunk;
        return { start: () => { order.push('start'); }, stop: async () => { order.push('stop'); emit(1, true, new Uint8Array(0)); } };
      },
    });
    emit(0, false, bytes(4));
    await stop();
    check('teardown stops the recorder then uploads the tail + final', order.join() === 'start,stop' && seen.join() === '0,1');
  }

  // ── ffmpeg death mid-take: the final goes out flagged partial ──
  {
    const seen: VideoChunkUpload[] = [];
    let o!: any;
    const stop = await startVideoRecording({
      inv: inv(), log: () => {}, upload: async (c) => { seen.push(c); },
      recorderFactory: (opts) => { o = opts; return { start() {}, stop: async () => {} }; },
    });
    o.onChunk(0, false, bytes(4)); o.onUnexpectedExit(1); o.onChunk(1, true, new Uint8Array(0));
    await tick(); await stop();
    check('unexpected ffmpeg exit → final marker is partial', seen.length === 2 && seen[1].isFinal && seen[1].partial);
  }

  console.log(failed ? `\n${failed} FAILED` : '\nall passed');
  process.exit(failed ? 1 : 0);
}
void run();
