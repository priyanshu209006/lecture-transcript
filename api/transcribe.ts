import type { VercelRequest, VercelResponse } from '@vercel/node';
import { createClient } from '@supabase/supabase-js';
import FormData from 'form-data';
import { createHash, randomUUID } from 'node:crypto';
import { createReadStream, createWriteStream, promises as fs } from 'node:fs';
import { request as httpsRequest } from 'node:https';
import { pipeline } from 'node:stream/promises';

export const config = { api: { bodyParser: false } };
const MAX_AUDIO_BYTES = 180_000; // 2 s of 16 kHz mono 16-bit WAV is ~64 KB
const timeoutMs = 45_000;
type ChunkClaim = { claimed: boolean; status: 'processing' | 'completed' | 'failed'; transcript: string | null };

function env(name: string): string { const value = process.env[name]; if (!value) throw new Error(`Missing ${name}`); return value; }
function json(res: VercelResponse, code: number, body: object) { return res.status(code).setHeader('Cache-Control', 'no-store').json(body); }
function authenticated(req: VercelRequest) {
  const token = req.headers.authorization?.replace(/^Bearer\s+/i, '');
  return token && token === env('DEVICE_SHARED_SECRET');
}
function singleHeader(req: VercelRequest, name: string): string | undefined {
  const value = req.headers[name]; return Array.isArray(value) ? value[0] : value;
}
async function readAudioToTmp(req: VercelRequest, path: string) {
  const declared = Number(singleHeader(req, 'content-length'));
  if (!Number.isFinite(declared) || declared < 45 || declared > MAX_AUDIO_BYTES) throw Object.assign(new Error('invalid audio size'), { status: 413 });
  let bytes = 0; const hash = createHash('sha256');
  req.on('data', (part: Buffer) => { bytes += part.length; hash.update(part); if (bytes > MAX_AUDIO_BYTES) req.destroy(Object.assign(new Error('audio too large'), { status: 413 })); });
  await pipeline(req, createWriteStream(path, { flags: 'wx' }));
  if (bytes !== declared) throw Object.assign(new Error('incomplete request body'), { status: 400 });
  return { bytes, sha256: hash.digest('hex') };
}
async function groqTranscribe(path: string) {
  const form = new FormData();
  form.append('file', createReadStream(path), { filename: 'chunk.wav', contentType: 'audio/wav' });
  form.append('model', process.env.GROQ_MODEL || 'whisper-large-v3-turbo');
  form.append('response_format', 'json');
  form.append('temperature', '0');
  // Intentionally no language field: Whisper detects Hindi, English, and Hinglish itself.
  const response = await new Promise<{ statusCode: number; body: string }>((resolve, reject) => {
    const upstream = httpsRequest('https://api.groq.com/openai/v1/audio/transcriptions', {
      method: 'POST', headers: { Authorization: `Bearer ${env('GROQ_API_KEY')}`, ...form.getHeaders() }
    }, (upstreamResponse) => {
      const parts: Buffer[] = [];
      upstreamResponse.on('data', (part: Buffer) => parts.push(part));
      upstreamResponse.on('end', () => resolve({ statusCode: upstreamResponse.statusCode || 502, body: Buffer.concat(parts).toString('utf8') }));
    });
    upstream.setTimeout(timeoutMs, () => upstream.destroy(new Error('Groq request timed out')));
    upstream.on('error', reject); form.on('error', reject); form.pipe(upstream);
  });
  if (response.statusCode < 200 || response.statusCode >= 300) throw Object.assign(new Error(response.body.slice(0, 500)), { status: response.statusCode });
  const parsed = JSON.parse(response.body) as { text?: string; x_groq?: { id?: string } };
  if (typeof parsed.text !== 'string') throw new Error('Groq response omitted text');
  return { text: parsed.text.trim(), requestId: parsed.x_groq?.id || null };
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  try {
    if (req.method !== 'POST') return json(res, 405, { error: 'POST only' });
    if (!authenticated(req)) return json(res, 401, { error: 'unauthorized' });
    const action = singleHeader(req, 'x-lecture-action');
    const supabase = createClient(env('SUPABASE_URL'), env('SUPABASE_SERVICE_ROLE_KEY'), { auth: { persistSession: false } });
    if (action === 'start') {
      const deviceId = singleHeader(req, 'x-device-id');
      if (!deviceId || deviceId.length > 80) return json(res, 400, { error: 'invalid device id' });
      const sessionId = randomUUID();
      const { error } = await supabase.from('lecture_sessions').insert({ id: sessionId, device_id: deviceId });
      if (error) throw Object.assign(new Error(error.message), { status: 502 });
      return json(res, 201, { sessionId, status: 'recording' });
    }
    const sessionId = singleHeader(req, 'x-session-id');
    if (!sessionId) return json(res, 400, { error: 'x-session-id is required' });
    if (action === 'finish') {
      const { data: pending, error: pendingError } = await supabase.from('lecture_chunks').select('sequence_no').eq('session_id', sessionId).eq('status', 'processing').limit(1);
      if (pendingError) throw Object.assign(new Error(pendingError.message), { status: 502 });
      if (pending?.length) return json(res, 409, { error: 'chunk still processing', sequenceNo: pending[0].sequence_no });
      const { data: transcript, error } = await supabase.rpc('lecture_transcript', { p_session_id: sessionId });
      if (error) throw Object.assign(new Error(error.message), { status: 502 });
      const { error: updateError } = await supabase.from('lecture_sessions').update({ status: 'finished', finished_at: new Date().toISOString() }).eq('id', sessionId);
      if (updateError) throw Object.assign(new Error(updateError.message), { status: 502 });
      return json(res, 200, { sessionId, status: 'finished', transcript });
    }
    if (action !== 'chunk') return json(res, 400, { error: 'invalid x-lecture-action' });
    const sequenceNo = Number(singleHeader(req, 'x-chunk-sequence'));
    const capturedAt = singleHeader(req, 'x-captured-at');
    if (!Number.isInteger(sequenceNo) || sequenceNo < 0 || !capturedAt || Number.isNaN(Date.parse(capturedAt))) return json(res, 400, { error: 'invalid chunk headers' });
    const { data: session, error: sessionError } = await supabase.from('lecture_sessions').select('status').eq('id', sessionId).maybeSingle();
    if (sessionError) throw Object.assign(new Error(sessionError.message), { status: 502 });
    if (!session) return json(res, 404, { error: 'unknown session' });
    if (session.status !== 'recording') return json(res, 409, { error: 'session is not accepting chunks' });
    const tempPath = `/tmp/lecture-${randomUUID()}.wav`;
    try {
      const audio = await readAudioToTmp(req, tempPath);
      const { data: claimedData, error: claimError } = await supabase.rpc('claim_lecture_chunk', { p_session_id: sessionId, p_sequence_no: sequenceNo, p_captured_at: capturedAt, p_audio_sha256: audio.sha256 }).single();
      const claimed = claimedData as ChunkClaim | null;
      if (claimError || !claimed) throw Object.assign(new Error(claimError?.message || 'unable to claim chunk'), { status: 502 });
      if (!claimed.claimed) {
        if (claimed.status === 'completed') return json(res, 200, { sessionId, sequenceNo, duplicate: true, status: 'completed', transcript: claimed.transcript });
        // Never resend to Groq after an ambiguous timeout/crash; operator can inspect this row.
        return json(res, 409, { sessionId, sequenceNo, duplicate: true, status: claimed.status, retryAfterSeconds: 10 });
      }
      const result = await groqTranscribe(tempPath);
      const { data: completed, error: completeError } = await supabase.rpc('complete_lecture_chunk', { p_session_id: sessionId, p_sequence_no: sequenceNo, p_transcript: result.text, p_groq_request_id: result.requestId });
      if (completeError || !completed) throw Object.assign(new Error(completeError?.message || 'chunk completion conflict'), { status: 502 });
      return json(res, 200, { sessionId, sequenceNo, status: 'completed', transcript: result.text });
    } finally { await fs.unlink(tempPath).catch(() => undefined); }
  } catch (error: any) {
    const status = error?.name === 'AbortError' ? 504 : (Number.isInteger(error?.status) ? error.status : 500);
    console.error('transcribe failed', { status, message: error?.message });
    return json(res, status, { error: status === 500 ? 'internal error' : error.message });
  }
}
