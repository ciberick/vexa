/**
 * L2 — continuous video stream: argv shape, byte chunker, supervisor lifecycle. Offline (a fake
 * ffmpeg process); a real-ffmpeg arm runs only when `ffmpeg` is on PATH.
 * Run: npx tsx src/video-stream.test.ts
 */
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import { spawnSync } from 'node:child_process';
import { ByteChunker, VideoStreamRecorder, buildVideoStreamArgs, normalizeFps } from './video-stream';

let failed = 0;
const check = (name: string, cond: boolean, detail = '') => {
  console.log(`  ${cond ? '✅' : '❌'} ${name}${cond ? '' : '  — ' + detail}`);
  if (!cond) failed++;
};
const flag = (a: string[], f: string) => a[a.indexOf(f) + 1];

// ── argv ──
const a = buildVideoStreamArgs({ display: ':99', fps: 5, audioSource: 'meeting_sink.monitor' });
check('fps clamps to 1..10, junk → 5', normalizeFps(99) === 10 && normalizeFps(0) === 5 && normalizeFps(NaN) === 5 && normalizeFps(7) === 7);
check('grabs the display at the requested fps', flag(a, '-framerate') === '5' && flag(a, '-video_size') === '1920x1080');
check('keyframe every 10 s (gop = fps*10)', flag(a, '-g') === '50');
check('screen-content VP9 realtime with a bitrate ceiling', flag(a, '-tune-content') === 'screen' && flag(a, '-deadline') === 'realtime' && flag(a, '-b:v') === '900k');
check('mono Opus voice track from the dedicated monitor', flag(a, '-c:a') === 'libopus' && flag(a, '-ac') === '1' && a.includes('meeting_sink.monitor'));
check('continuous webm on stdout', flag(a, '-f') === 'x11grab' && a[a.length - 1] === 'pipe:1');
const v = buildVideoStreamArgs({ display: ':99', fps: 10, audioSource: null });
check('video-only: no pulse input, no audio codec, gop 100', !v.includes('pulse') && !v.includes('-c:a') && flag(v, '-g') === '100');

// ── chunker ──
{
  const got: Array<[number, number]> = [];
  const c = new ByteChunker({ intervalMs: 3_600_000, maxBytes: 10, onChunk: (s, b) => got.push([s, b.length]) });
  c.push(Buffer.alloc(4)); c.push(Buffer.alloc(4));
  check('below the cap nothing is emitted', got.length === 0);
  c.push(Buffer.alloc(4));
  check('size cap flushes one ordered chunk', got.length === 1 && got[0][0] === 0 && got[0][1] === 12);
  c.push(Buffer.alloc(3));
  const next = c.end();
  check('end() flushes the tail and returns the next seq', got.length === 2 && got[1][1] === 3 && next === 2);
}

// ── supervisor with a fake ffmpeg ──
function fakeProc() {
  const p: any = new EventEmitter();
  p.stdout = new PassThrough(); p.stderr = new PassThrough(); p.stdin = new PassThrough();
  p.killed = [] as string[];
  p.kill = (sig: string) => { p.killed.push(sig); };
  return p;
}
async function run() {
  {
    const p = fakeProc();
    const seen: Array<{ seq: number; fin: boolean; len: number }> = [];
    const r = new VideoStreamRecorder({
      config: { display: ':99', fps: 5 }, chunkMs: 3_600_000, maxChunkBytes: 8,
      onChunk: (seq, fin, b) => seen.push({ seq, fin, len: b.length }),
      spawnFn: () => p,
    });
    r.start();
    p.stdout.write(Buffer.alloc(8)); p.stdout.write(Buffer.alloc(5));
    await new Promise((res) => setImmediate(res));
    let sawQ = false;
    p.stdin.on('data', (d: Buffer) => { if (d.toString().startsWith('q')) { sawQ = true; setImmediate(() => p.emit('close', 0)); } });
    await r.stop();
    check('stop() sends q, flushes the tail, ends with an empty final', sawQ && seen.length === 3 && seen[0].len === 8 && seen[1].len === 5 && seen[2].fin && seen[2].len === 0 && seen.map((s) => s.seq).join() === '0,1,2');
  }
  {
    const p = fakeProc();
    let exit: number | null | undefined; const seen: boolean[] = [];
    const r = new VideoStreamRecorder({
      config: { display: ':99', fps: 5 }, chunkMs: 3_600_000, onChunk: (_s, fin) => seen.push(fin),
      onUnexpectedExit: (c) => { exit = c; }, spawnFn: () => p,
    });
    r.start(); p.stdout.write(Buffer.alloc(3)); await new Promise((res) => setImmediate(res)); p.emit('close', 1);
    await new Promise((res) => setImmediate(res));
    check('unexpected exit flushes, sends a final and reports the partial take', exit === 1 && seen.join() === 'false,true');
  }
  {
    const p = fakeProc();
    const r = new VideoStreamRecorder({ config: { display: ':99', fps: 5 }, stopTimeoutMs: 20, onChunk: () => {}, spawnFn: () => p });
    r.start();
    const t = r.stop();
    await new Promise((res) => setTimeout(res, 60));
    check('a wedged ffmpeg is escalated SIGTERM → SIGKILL', p.killed[0] === 'SIGTERM' && p.killed.includes('SIGKILL'));
    p.emit('close', 137); await t;
  }

  // real-ffmpeg arm: lavfi sources through the SAME codec flags → chunks concatenate to a decodable WebM.
  const probe = spawnSync('ffmpeg', ['-version']);
  if (probe.status === 0) {
    const real = spawnSync('ffmpeg', ['-hide_banner', '-loglevel', 'error',
      '-f', 'lavfi', '-i', 'testsrc2=size=1280x720:rate=5', '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=48000',
      '-t', '4', '-c:v', 'libvpx-vp9', '-pix_fmt', 'yuv420p', '-deadline', 'realtime', '-cpu-used', '6', '-row-mt', '1',
      '-tune-content', 'screen', '-crf', '36', '-b:v', '900k', '-g', '50', '-c:a', 'libopus', '-ac', '1', '-b:a', '40k',
      '-f', 'webm', '-live', '1', '-cluster_time_limit', '2000', 'pipe:1'], { maxBuffer: 64 * 1024 * 1024 });
    const out = real.stdout as Buffer;
    check('real ffmpeg accepts the encoder flags and emits a WebM stream', real.status === 0 && out.length > 0 && out.subarray(0, 4).toString('hex') === '1a45dfa3',
      String(real.stderr).slice(0, 300));
    // cut into arbitrary byte chunks, rejoin, and ffprobe the result
    const chunks: Buffer[] = [];
    for (let i = 0; i < out.length; i += 7001) chunks.push(out.subarray(i, i + 7001));
    const joined = Buffer.concat(chunks);
    const pr = spawnSync('ffprobe', ['-v', 'error', '-show_entries', 'stream=codec_name', '-of', 'csv=p=0', '-'], { input: joined });
    const codecs = String(pr.stdout).trim().split(/\s+/).sort().join(',');
    check('byte-cut chunks rejoin into a stream ffprobe reads as vp9 + opus', codecs === 'opus,vp9', `got ${codecs} ${String(pr.stderr).slice(0, 200)}`);
  } else {
    console.log('  ⏭  real-ffmpeg arm skipped (ffmpeg not on PATH)');
  }

  console.log(failed ? `\n${failed} FAILED` : '\nall passed');
  process.exit(failed ? 1 : 0);
}
void run();
