# WhatsApp Voice Notes

`frappe_whatsapp` can send WhatsApp voice notes through the
`WhatsApp Message` DocType. For reliable playback on iPhone WhatsApp, client
apps must create voice notes as local Ogg/Opus files and let
`frappe_whatsapp` upload that file to Meta before the message is sent.

## Requirements

- Store the audio as a local Frappe `File`; do not pass a remote URL for voice
  notes.
- Use an Ogg container with the Opus codec.
- Use `.ogg`, `.oga`, or `.opus` as the file extension.
- Keep the file 16 MB or smaller.
- Create a `WhatsApp Message` with `content_type = "audio"` and
  `is_voice_note = 1`.
- Keep `message` empty unless you intentionally want a caption.

For browser or mobile recordings that are WebM, MP4, or another container,
convert before creating the `WhatsApp Message`. This ffmpeg profile matches the
format used by the WhatsApp Chat recorder:

```bash
ffmpeg -y -i input.webm -vn -map_metadata -1 -ac 1 -ar 48000 \
  -c:a libopus -b:a 64k -application voip -f ogg voice-note.ogg
```

## Creating a Voice Note From a Client App

```python
import frappe


def send_voice_note(to_number: str, ogg_opus_bytes: bytes):
    file_doc = frappe.get_doc({
        "doctype": "File",
        "file_name": "voice-note.ogg",
        "content": ogg_opus_bytes,
        "is_private": 0,
    }).insert(ignore_permissions=True)

    return frappe.get_doc({
        "doctype": "WhatsApp Message",
        "type": "Outgoing",
        "to": to_number,
        "content_type": "audio",
        "is_voice_note": 1,
        "attach": file_doc.file_url,
        "message": "",
    }).insert(ignore_permissions=True)
```

When the document is inserted, `frappe_whatsapp` uploads the local file to
Meta's media endpoint with `audio/ogg; codecs=opus`, then sends the message
using the returned media ID and `voice: true`. That media-ID path is the safest
way to avoid the iPhone WhatsApp error: "This audio is no longer available."

Generic audio files can still be sent with `content_type = "audio"` and
`is_voice_note = 0`, but they are not treated as WhatsApp push-to-talk voice
notes.

## NextJS Client App Using the Frappe REST API

Use a NextJS API route as the bridge between the browser and Frappe:

1. The browser records audio with `MediaRecorder`.
2. The browser sends the recording to a NextJS API route.
3. The NextJS API route converts the recording to Ogg/Opus.
4. The NextJS API route uploads the `.ogg` file to Frappe with
   `/api/method/upload_file`.
5. The NextJS API route creates a `WhatsApp Message` through
   `/api/resource/WhatsApp%20Message`.

Do not call Frappe directly from browser code with an API key and secret. Keep
the Frappe credentials in the NextJS server environment.

### Frappe Requirements

- Create a Frappe API user with permission to upload `File` records and create
  `WhatsApp Message` records.
- Use a Desk/API user for audio uploads. Frappe's default guest/non-desk upload
  restrictions may reject audio MIME types.
- Make sure the target number is inside the allowed WhatsApp service window, or
  send an approved template first according to your compliance settings.
- Install `ffmpeg` in the NextJS runtime with `libopus` support.

Example `.env.local`:

```env
FRAPPE_BASE_URL=https://erp.example.com
FRAPPE_API_KEY=your_api_key
FRAPPE_API_SECRET=your_api_secret
FFMPEG_PATH=/usr/bin/ffmpeg
```

### Browser Recorder Example

This component records audio and posts it to your NextJS API route. The server
still converts the file before sending, even when the browser reports a
supported MIME type.

```tsx
"use client";

import { useRef, useState } from "react";

const MIME_TYPES = [
  "audio/ogg;codecs=opus",
  "audio/mp4",
  "audio/webm;codecs=opus",
];

function getRecorderMimeType() {
  if (typeof MediaRecorder === "undefined") {
    return "";
  }

  return MIME_TYPES.find((type) => MediaRecorder.isTypeSupported(type)) || "";
}

export function VoiceNoteRecorder({ to }: { to: string }) {
  const recorderRef = useRef<MediaRecorder | null>(null);
  const streamRef = useRef<MediaStream | null>(null);
  const sendOnStopRef = useRef(false);
  const chunksRef = useRef<BlobPart[]>([]);
  const [recording, setRecording] = useState(false);

  async function start() {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const mimeType = getRecorderMimeType();
    const recorder = new MediaRecorder(
      stream,
      mimeType ? { mimeType } : undefined
    );

    chunksRef.current = [];
    streamRef.current = stream;
    recorderRef.current = recorder;

    recorder.ondataavailable = (event) => {
      if (event.data.size) {
        chunksRef.current.push(event.data);
      }
    };

    recorder.onstop = async () => {
      const shouldSend = sendOnStopRef.current;
      sendOnStopRef.current = false;
      setRecording(false);
      stream.getTracks().forEach((track) => track.stop());

      if (!shouldSend || !chunksRef.current.length) {
        chunksRef.current = [];
        return;
      }

      const blob = new Blob(chunksRef.current, {
        type: recorder.mimeType || mimeType || "audio/webm",
      });
      chunksRef.current = [];

      const form = new FormData();
      form.append("to", to);
      form.append("audio", blob, `voice-note-${Date.now()}.webm`);

      const response = await fetch("/api/send-whatsapp-voice-note", {
        method: "POST",
        body: form,
      });

      if (!response.ok) {
        const error = await response.json().catch(() => ({}));
        throw new Error(error.error || "Could not send voice note");
      }
    };

    recorder.start();
    setRecording(true);
  }

  function stopAndSend() {
    sendOnStopRef.current = true;
    recorderRef.current?.stop();
  }

  function cancel() {
    sendOnStopRef.current = false;
    recorderRef.current?.stop();
  }

  return (
    <div>
      {!recording ? (
        <button type="button" onClick={start}>Record</button>
      ) : (
        <>
          <button type="button" onClick={stopAndSend}>Send</button>
          <button type="button" onClick={cancel}>Cancel</button>
        </>
      )}
    </div>
  );
}
```

### NextJS API Route Example

Create `app/api/send-whatsapp-voice-note/route.ts`.

```ts
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

export const runtime = "nodejs";

const execFileAsync = promisify(execFile);
const MAX_AUDIO_BYTES = 16 * 1024 * 1024;

function requiredEnv(name: string) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function frappeAuthHeader() {
  return `token ${requiredEnv("FRAPPE_API_KEY")}:` +
    requiredEnv("FRAPPE_API_SECRET");
}

function extensionFromFile(file: File) {
  const nameExtension = file.name.split(".").pop()?.toLowerCase();
  if (nameExtension && /^[a-z0-9]+$/.test(nameExtension)) {
    return nameExtension;
  }

  const mime = file.type.split(";")[0].toLowerCase();
  if (mime === "audio/mp4") return "m4a";
  if (mime === "audio/ogg") return "ogg";
  return "webm";
}

async function transcodeToOggOpus(input: Buffer, extension: string) {
  const dir = await mkdtemp(join(tmpdir(), "whatsapp-voice-"));
  const inputPath = join(dir, `input.${extension}`);
  const outputPath = join(dir, "voice-note.ogg");
  const ffmpeg = process.env.FFMPEG_PATH || "ffmpeg";

  try {
    await writeFile(inputPath, input);
    await execFileAsync(
      ffmpeg,
      [
        "-y",
        "-i",
        inputPath,
        "-vn",
        "-map_metadata",
        "-1",
        "-ac",
        "1",
        "-ar",
        "48000",
        "-c:a",
        "libopus",
        "-b:a",
        "64k",
        "-application",
        "voip",
        "-f",
        "ogg",
        outputPath,
      ],
      { timeout: 60_000 }
    );

    return await readFile(outputPath);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

async function uploadVoiceFileToFrappe(ogg: Buffer) {
  const frappeBaseUrl = requiredEnv("FRAPPE_BASE_URL").replace(/\/$/, "");
  const fileName = `voice-note-${Date.now()}.ogg`;

  const form = new FormData();
  form.append(
    "file",
    new Blob([new Uint8Array(ogg)], { type: "audio/ogg; codecs=opus" }),
    fileName
  );
  form.append("is_private", "1");
  form.append("folder", "Home/Attachments");

  const response = await fetch(`${frappeBaseUrl}/api/method/upload_file`, {
    method: "POST",
    headers: {
      Authorization: frappeAuthHeader(),
    },
    body: form,
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.exception || payload.message || "Upload failed");
  }

  const fileUrl = payload.message?.file_url;
  if (!fileUrl) {
    throw new Error("Frappe did not return a file_url");
  }

  return fileUrl as string;
}

async function createWhatsAppVoiceMessage(to: string, fileUrl: string) {
  const frappeBaseUrl = requiredEnv("FRAPPE_BASE_URL").replace(/\/$/, "");

  const response = await fetch(
    `${frappeBaseUrl}/api/resource/WhatsApp%20Message`,
    {
      method: "POST",
      headers: {
        Authorization: frappeAuthHeader(),
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        type: "Outgoing",
        to,
        content_type: "audio",
        is_voice_note: 1,
        attach: fileUrl,
        message: "",
      }),
    }
  );

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.exception || payload.message || "Send failed");
  }

  return payload.data;
}

export async function POST(request: Request) {
  try {
    const form = await request.formData();
    const to = String(form.get("to") || "");
    const audio = form.get("audio");

    if (!to) {
      return Response.json({ error: "Missing recipient number" }, { status: 400 });
    }
    if (!(audio instanceof File)) {
      return Response.json({ error: "Missing audio file" }, { status: 400 });
    }
    if (audio.size > MAX_AUDIO_BYTES) {
      return Response.json({ error: "Voice note is too large" }, { status: 413 });
    }

    const input = Buffer.from(await audio.arrayBuffer());
    const ogg = await transcodeToOggOpus(input, extensionFromFile(audio));

    if (ogg.length > MAX_AUDIO_BYTES) {
      return Response.json(
        { error: "Converted voice note is too large" },
        { status: 413 }
      );
    }

    const fileUrl = await uploadVoiceFileToFrappe(ogg);
    const message = await createWhatsAppVoiceMessage(to, fileUrl);

    return Response.json({
      ok: true,
      file_url: fileUrl,
      message,
    });
  } catch (error) {
    console.error(error);
    return Response.json(
      { error: error instanceof Error ? error.message : "Unexpected error" },
      { status: 500 }
    );
  }
}
```

### REST Request Summary

Upload the converted Ogg/Opus file:

```http
POST /api/method/upload_file
Authorization: token <api_key>:<api_secret>
Content-Type: multipart/form-data

file=<voice-note.ogg>
is_private=1
folder=Home/Attachments
```

Create and send the WhatsApp voice note:

```http
POST /api/resource/WhatsApp%20Message
Authorization: token <api_key>:<api_secret>
Content-Type: application/json

{
  "type": "Outgoing",
  "to": "15551234567",
  "content_type": "audio",
  "is_voice_note": 1,
  "attach": "/private/files/voice-note-1710000000000.ogg",
  "message": ""
}
```

The `POST /api/resource/WhatsApp%20Message` call inserts the document and sends
the WhatsApp message immediately. The response contains the created document in
`data`. If the request fails, do not retry blindly; inspect the error response
and avoid creating duplicate voice notes for the same user action.

### Common Mistakes

- Sending `.m4a`, `.mp4`, or `.webm` with `is_voice_note = 1`. Convert to
  Ogg/Opus first.
- Passing a remote URL in `attach` for a voice note. Upload a local Frappe
  `File` and use its `file_url`.
- Wrapping the document body in `{ "data": { ... } }`. This Frappe REST route
  expects the document fields directly in the JSON body.
- Exposing `FRAPPE_API_KEY` or `FRAPPE_API_SECRET` in browser JavaScript.
