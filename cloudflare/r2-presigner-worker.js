// Cloudflare Worker: R2 presigner for direct browser uploads.
// Ops: presign_put, presign_get, multipart_init, multipart_sign_part, multipart_complete, multipart_abort
//
// Required secrets:
// - R2_ACCOUNT_ID
// - R2_ACCESS_KEY_ID
// - R2_SECRET_ACCESS_KEY
// - R2_BUCKET
// Optional:
// - R2_PRESIGNER_TOKEN

const enc = new TextEncoder();

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" }
  });
}

function hex(bytes) {
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function sha256Hex(str) {
  const digest = await crypto.subtle.digest("SHA-256", enc.encode(str));
  return hex(new Uint8Array(digest));
}

async function hmacRaw(keyBytes, msg) {
  const key = await crypto.subtle.importKey(
    "raw",
    keyBytes,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(msg));
  return new Uint8Array(sig);
}

function rfc3986(str) {
  return encodeURIComponent(str).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

function pathEncode(key) {
  return key.split("/").map((p) => rfc3986(p)).join("/");
}

function nowAmz() {
  const iso = new Date().toISOString();
  const amzDate = iso.replace(/[:-]|\.\d{3}/g, "");
  return { amzDate, dateStamp: amzDate.slice(0, 8) };
}

function host(env) {
  return `${env.R2_BUCKET}.${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;
}

async function signingKey(secret, dateStamp, region = "auto", service = "s3") {
  const kDate = await hmacRaw(enc.encode(`AWS4${secret}`), dateStamp);
  const kRegion = await hmacRaw(kDate, region);
  const kService = await hmacRaw(kRegion, service);
  return hmacRaw(kService, "aws4_request");
}

function canonicalQuery(params) {
  return Object.keys(params)
    .sort()
    .map((k) => `${rfc3986(k)}=${rfc3986(String(params[k]))}`)
    .join("&");
}

async function signQueryV4({ method, env, objectKey, extraQuery = {}, expires = 900 }) {
  const key = String(objectKey || "").replace(/^\/+/, "");
  if (!key) throw new Error("Missing object_key");

  const { amzDate, dateStamp } = nowAmz();
  const scope = `${dateStamp}/auto/s3/aws4_request`;
  const h = host(env);
  const uri = `/${pathEncode(key)}`;
  const q = {
    ...extraQuery,
    "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
    "X-Amz-Credential": `${env.R2_ACCESS_KEY_ID}/${scope}`,
    "X-Amz-Date": amzDate,
    "X-Amz-Expires": Math.min(Math.max(Number(expires || 900), 60), 86400),
    "X-Amz-SignedHeaders": "host"
  };
  const cq = canonicalQuery(q);
  const ch = `host:${h}\n`;
  const signedHeaders = "host";
  const payloadHash = "UNSIGNED-PAYLOAD";

  const creq = [method, uri, cq, ch, signedHeaders, payloadHash].join("\n");
  const sts = ["AWS4-HMAC-SHA256", amzDate, scope, await sha256Hex(creq)].join("\n");
  const sk = await signingKey(env.R2_SECRET_ACCESS_KEY, dateStamp, "auto", "s3");
  const sig = hex(await hmacRaw(sk, sts));

  return `https://${h}${uri}?${cq}&X-Amz-Signature=${sig}`;
}

function authOk(req, env) {
  if (!env.R2_PRESIGNER_TOKEN) return true;
  const auth = req.headers.get("authorization") || "";
  return auth === `Bearer ${env.R2_PRESIGNER_TOKEN}`;
}

function safePublicUrl(base, key) {
  const b = String(base || "").replace(/\/+$/, "");
  if (!b) return "";
  return `${b}/${String(key || "").replace(/^\/+/, "")}`;
}

function parseTag(xml, tag) {
  const m = String(xml || "").match(new RegExp(`<${tag}>([^<]+)</${tag}>`, "i"));
  return m && m[1] ? m[1] : "";
}

async function opPresignPut(body, env) {
  const key = String(body.object_key || "").replace(/^\/+/, "");
  const url = await signQueryV4({
    method: "PUT",
    env,
    objectKey: key,
    extraQuery: {},
    expires: Number(body.expires || 900)
  });
  return json({
    upload_url: url,
    object_key: key,
    public_url: safePublicUrl(body.public_base_url, key)
  });
}

async function opPresignGet(body, env) {
  const key = String(body.object_key || "").replace(/^\/+/, "");
  const url = await signQueryV4({
    method: "GET",
    env,
    objectKey: key,
    extraQuery: {},
    expires: Number(body.expires || 86400)
  });
  return json({ download_url: url, object_key: key });
}

async function opMultipartInit(body, env) {
  const key = String(body.object_key || "").replace(/^\/+/, "");
  const url = await signQueryV4({
    method: "POST",
    env,
    objectKey: key,
    extraQuery: { uploads: "" },
    expires: Number(body.expires || 3600)
  });
  const resp = await fetch(url, {
    method: "POST",
    headers: { "content-type": String(body.content_type || "application/octet-stream") }
  });
  if (!resp.ok) return json({ error: `multipart_init failed (${resp.status})` }, 400);
  const xml = await resp.text();
  const uploadId = parseTag(xml, "UploadId");
  if (!uploadId) return json({ error: "multipart_init missing UploadId" }, 400);
  const partSize = Math.max(5 * 1024 * 1024, Number(body.part_size || 64 * 1024 * 1024));
  return json({
    upload_id: uploadId,
    object_key: key,
    public_url: safePublicUrl(body.public_base_url, key),
    part_size: partSize,
    expires: Math.min(Math.max(Number(body.expires || 3600), 60), 86400)
  });
}

async function opMultipartSignPart(body, env) {
  const key = String(body.object_key || "").replace(/^\/+/, "");
  const uploadId = String(body.upload_id || "");
  const partNumber = Number(body.part_number || 0);
  if (!key || !uploadId || !partNumber) return json({ error: "Missing object_key/upload_id/part_number" }, 400);
  const url = await signQueryV4({
    method: "PUT",
    env,
    objectKey: key,
    extraQuery: { partNumber, uploadId },
    expires: Number(body.expires || 3600)
  });
  return json({ upload_url: url, part_number: partNumber });
}

async function opMultipartComplete(body, env) {
  const key = String(body.object_key || "").replace(/^\/+/, "");
  const uploadId = String(body.upload_id || "");
  const partsIn = Array.isArray(body.parts) ? body.parts : [];
  if (!key || !uploadId || !partsIn.length) return json({ error: "Missing object_key/upload_id/parts" }, 400);

  const parts = partsIn
    .map((p) => ({
      partNumber: Number(p.part_number || p.PartNumber || 0),
      etag: String(p.etag || p.ETag || "").replace(/^\"|\"$/g, "")
    }))
    .filter((p) => p.partNumber > 0 && p.etag)
    .sort((a, b) => a.partNumber - b.partNumber);
  if (!parts.length) return json({ error: "No valid parts" }, 400);

  const payload = `<CompleteMultipartUpload>${parts
    .map((p) => `<Part><PartNumber>${p.partNumber}</PartNumber><ETag>\"${p.etag}\"</ETag></Part>`)
    .join("")}</CompleteMultipartUpload>`;

  const url = await signQueryV4({
    method: "POST",
    env,
    objectKey: key,
    extraQuery: { uploadId },
    expires: 3600
  });
  const resp = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/xml" },
    body: payload
  });
  if (!resp.ok) return json({ error: `multipart_complete failed (${resp.status})` }, 400);
  const out = await resp.text();
  return json({ ok: true, etag: parseTag(out, "ETag").replace(/^\"|\"$/g, "") });
}

async function opMultipartAbort(body, env) {
  const key = String(body.object_key || "").replace(/^\/+/, "");
  const uploadId = String(body.upload_id || "");
  if (!key || !uploadId) return json({ error: "Missing object_key/upload_id" }, 400);
  const url = await signQueryV4({
    method: "DELETE",
    env,
    objectKey: key,
    extraQuery: { uploadId },
    expires: 3600
  });
  const resp = await fetch(url, { method: "DELETE" });
  if (!resp.ok && resp.status !== 404) return json({ error: `multipart_abort failed (${resp.status})` }, 400);
  return json({ ok: true });
}

export default {
  async fetch(req, env) {
    if (req.method !== "POST") return json({ error: "Method Not Allowed" }, 405);
    if (!authOk(req, env)) return json({ error: "Unauthorized" }, 401);

    let body;
    try {
      body = await req.json();
    } catch {
      return json({ error: "Invalid JSON payload" }, 400);
    }
    const op = String(body.op || "");

    try {
      if (op === "presign_put") return opPresignPut(body, env);
      if (op === "presign_get") return opPresignGet(body, env);
      if (op === "multipart_init") return opMultipartInit(body, env);
      if (op === "multipart_sign_part") return opMultipartSignPart(body, env);
      if (op === "multipart_complete") return opMultipartComplete(body, env);
      if (op === "multipart_abort") return opMultipartAbort(body, env);
      return json({ error: "Unsupported op" }, 400);
    } catch (err) {
      return json({ error: err && err.message ? err.message : "Worker error" }, 500);
    }
  }
};
