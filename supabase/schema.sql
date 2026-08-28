-- Run once in Supabase SQL Editor. The service-role key is only used in Vercel.
create extension if not exists pgcrypto;

create table if not exists public.lecture_sessions (
  id uuid primary key,
  device_id text not null,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null default 'recording' check (status in ('recording','finishing','finished')),
  created_at timestamptz not null default now()
);

create table if not exists public.lecture_chunks (
  session_id uuid not null references public.lecture_sessions(id) on delete cascade,
  sequence_no integer not null check (sequence_no >= 0),
  captured_at timestamptz not null,
  audio_sha256 text not null,
  status text not null default 'processing' check (status in ('processing','completed','failed')),
  transcript text,
  groq_request_id text,
  error_code text,
  created_at timestamptz not null default now(),
  completed_at timestamptz,
  primary key (session_id, sequence_no)
);
create index if not exists lecture_chunks_session_sequence_idx on public.lecture_chunks(session_id, sequence_no);

-- Atomic claim. A conflict means this exact sequence was already accepted.
create or replace function public.claim_lecture_chunk(
  p_session_id uuid, p_sequence_no integer, p_captured_at timestamptz, p_audio_sha256 text
) returns table(claimed boolean, status text, transcript text) language plpgsql security definer as $$
begin
  insert into public.lecture_chunks(session_id, sequence_no, captured_at, audio_sha256, status)
  values (p_session_id, p_sequence_no, p_captured_at, p_audio_sha256, 'processing')
  on conflict (session_id, sequence_no) do nothing;
  if found then return query select true, 'processing'::text, null::text; return; end if;
  return query select false, c.status, c.transcript
  from public.lecture_chunks c where c.session_id = p_session_id and c.sequence_no = p_sequence_no;
end $$;

create or replace function public.complete_lecture_chunk(
  p_session_id uuid, p_sequence_no integer, p_transcript text, p_groq_request_id text
) returns boolean language plpgsql security definer as $$
begin
  update public.lecture_chunks set status='completed', transcript=p_transcript,
    groq_request_id=p_groq_request_id, completed_at=now(), error_code=null
  where session_id=p_session_id and sequence_no=p_sequence_no and status='processing';
  return found;
end $$;

create or replace function public.lecture_transcript(p_session_id uuid)
returns text language sql stable security definer as $$
  select coalesce(string_agg(transcript, E'\n' order by sequence_no), '')
  from public.lecture_chunks where session_id=p_session_id and status='completed';
$$;
