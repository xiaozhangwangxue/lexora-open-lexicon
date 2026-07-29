const allowedPaths = new Set([
  "/health",
  "/manifest",
  "/oxford-manifest",
  "/v1/lookup",
  "/v1/suggest",
]);
const bootstrapObjects = new Set([
  "lexora-open-oxford-scope.sqlite.gz.part-00-0",
  "lexora-open-oxford-scope.sqlite.gz.part-00-1",
  "lexora-open-oxford-scope.sqlite.gz.part-01",
]);

function withCors(response, originName, cacheStatus) {
  const headers = new Headers(response.headers);
  headers.set("Access-Control-Allow-Origin", "*");
  headers.set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
  headers.set("Access-Control-Allow-Headers", "Content-Type");
  headers.set("X-Content-Type-Options", "nosniff");
  headers.set("X-Lexora-Origin", originName);
  headers.set("X-Lexora-Cache", cacheStatus);
  headers.delete("server");
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

async function fetchOrigin(origin, originName, request, env) {
  const incoming = new URL(request.url);
  const target = new URL(incoming.pathname + incoming.search, origin);
  if (
    (incoming.pathname === "/v1/lookup" ||
      incoming.pathname === "/v1/suggest") &&
    !target.searchParams.has("dataset")
  ) {
    target.searchParams.set("dataset", "oxford");
  }
  const headers = new Headers();
  headers.set("Accept", "application/json");
  headers.set("X-Lexora-Origin-Token", env.ORIGIN_TOKEN);
  const timeoutSetting =
    originName === "primary"
      ? env.PRIMARY_TIMEOUT_MS
      : env.SECONDARY_TIMEOUT_MS;
  const configuredTimeout = Number.parseInt(timeoutSetting || "8000", 10);
  const timeoutMs =
    Number.isFinite(configuredTimeout) && configuredTimeout >= 500
      ? configuredTimeout
      : 8000;
  const response = await fetch(target, {
    method: request.method,
    headers,
    redirect: "follow",
    signal: AbortSignal.timeout(timeoutMs),
    cf: { cacheTtl: 0 },
  });
  return { response, originName };
}

async function fetchWithFailover(request, env) {
  const origins = [
    [env.PRIMARY_ORIGIN, "primary"],
    [env.SECONDARY_ORIGIN, "secondary"],
  ];
  let lastResponse;
  for (const [origin, originName] of origins) {
    if (!origin) continue;
    try {
      const result = await fetchOrigin(origin, originName, request, env);
      lastResponse = result;
      if (result.response.status < 500) return result;
    } catch {
      // Try the next free OCI origin.
    }
  }
  return (
    lastResponse || {
      response: Response.json(
        { detail: "dictionary relay temporarily unavailable" },
        { status: 503 },
      ),
      originName: "unavailable",
    }
  );
}

function normalizeTerm(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function isValidTerm(term) {
  return /^[a-z][a-z' -]{0,79}$/.test(term);
}

async function enrichmentTerm(term, ctx) {
  const cache = caches.default;
  const cacheKey = new Request(
    `https://lexora-enrichment-cache.invalid/v1/${encodeURIComponent(term)}`,
  );
  const cached = await cache.match(cacheKey);
  if (cached) return cached;

  const requests = {
    dictionary: `https://api.dictionaryapi.dev/api/v2/entries/en/${encodeURIComponent(term)}`,
    related: `https://api.datamuse.com/words?${new URLSearchParams({
      ml: term,
      md: "dfr",
      ipa: "1",
      max: "30",
    })}`,
    exact: `https://api.datamuse.com/words?${new URLSearchParams({
      sp: term,
      md: "dfrp",
      ipa: "1",
      max: "8",
    })}`,
    synonyms: `https://api.datamuse.com/words?${new URLSearchParams({
      rel_syn: term,
      md: "f",
      max: "12",
    })}`,
    antonyms: `https://api.datamuse.com/words?${new URLSearchParams({
      rel_ant: term,
      max: "12",
    })}`,
  };
  const jsonOrNull = async (input) => {
    try {
      const response = await fetch(input, {
        headers: { Accept: "application/json" },
        signal: AbortSignal.timeout(2200),
      });
      if (!response.ok) return null;
      return response.json();
    } catch {
      return null;
    }
  };
  const values = await Promise.all(Object.values(requests).map(jsonOrNull));
  if (values.every((value) => value === null)) {
    return Response.json(
      { error: "Dictionary providers are temporarily unavailable" },
      { status: 504, headers: { "Cache-Control": "no-store" } },
    );
  }
  const result = Object.fromEntries(
    Object.keys(requests).map((key, index) => [key, values[index]]),
  );
  const response = Response.json(result, {
    headers: { "Cache-Control": "public, max-age=604800" },
  });
  ctx.waitUntil(cache.put(cacheKey, response.clone()));
  return response;
}

async function enrichmentDictionaryBatch(request, ctx) {
  let terms;
  try {
    const payload = await request.json();
    const seen = new Set();
    terms = Array.isArray(payload.terms)
      ? payload.terms
          .map(normalizeTerm)
          .filter((term) => {
            if (!isValidTerm(term) || seen.has(term)) return false;
            seen.add(term);
            return true;
          })
          .slice(0, 8)
      : [];
  } catch {
    return Response.json({ error: "Invalid JSON" }, { status: 400 });
  }
  if (terms.length === 0) return Response.json({ results: {} });

  const entries = await Promise.all(
    terms.map(async (term) => {
      const response = await enrichmentTerm(term, ctx);
      let data;
      try {
        data = await response.json();
      } catch {
        data = { error: "Invalid upstream response" };
      }
      return [term, { status: response.status, data }];
    }),
  );
  return Response.json(
    { results: Object.fromEntries(entries) },
    { headers: { "Cache-Control": "no-store" } },
  );
}

async function translationCacheKey(text) {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(text),
  );
  const key = Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
  return new Request(`https://lexora-enrichment-translation.invalid/v1/${key}`);
}

async function enrichmentTranslationBatch(request, ctx) {
  let texts;
  try {
    const payload = await request.json();
    texts = Array.isArray(payload.texts)
      ? payload.texts
          .map((value) => String(value).trim())
          .filter((value) => value.length > 0 && value.length <= 480)
          .slice(0, 32)
      : [];
  } catch {
    return Response.json({ error: "Invalid JSON" }, { status: 400 });
  }
  if (texts.length === 0) return Response.json({ translations: [] });

  const cache = caches.default;
  const keys = await Promise.all(texts.map(translationCacheKey));
  const cached = await Promise.all(keys.map((key) => cache.match(key)));
  const translations = await Promise.all(
    cached.map((response) => response?.text() ?? ""),
  );
  const missingIndexes = translations
    .map((value, index) => (value ? -1 : index))
    .filter((index) => index >= 0);

  if (missingIndexes.length > 0) {
    const marker = (index) => `[[[${index}]]]`;
    const payload = missingIndexes
      .map((index) => `${marker(index)} ${texts[index]}`)
      .join("\n");
    try {
      const endpoint = new URL(
        "https://translate.googleapis.com/translate_a/single",
      );
      endpoint.search = new URLSearchParams({
        client: "gtx",
        sl: "en",
        tl: "zh-CN",
        dt: "t",
        q: payload,
      }).toString();
      const response = await fetch(endpoint, {
        headers: { Accept: "application/json" },
        signal: AbortSignal.timeout(3500),
      });
      if (response.ok) {
        const body = await response.json();
        const chunks = Array.isArray(body?.[0]) ? body[0] : [];
        const joined = chunks.map((chunk) => String(chunk?.[0] ?? "")).join("");
        for (let position = 0; position < missingIndexes.length; position++) {
          const index = missingIndexes[position];
          const startMarker = marker(index);
          const start = joined.indexOf(startMarker);
          if (start < 0) continue;
          const contentStart = start + startMarker.length;
          const nextIndex = missingIndexes[position + 1];
          const end =
            nextIndex === undefined
              ? joined.length
              : joined.indexOf(marker(nextIndex), contentStart);
          const translated = joined
            .slice(contentStart, end < 0 ? joined.length : end)
            .trim();
          if (translated) translations[index] = translated;
        }
      }
    } catch {
      // Individual fallback below handles providers that reject a batch.
    }
  }

  const stillMissing = translations
    .map((value, index) => (value ? -1 : index))
    .filter((index) => index >= 0);
  await Promise.all(
    stillMissing.map(async (index) => {
      try {
        const endpoint = new URL("https://api.mymemory.translated.net/get");
        endpoint.search = new URLSearchParams({
          q: texts[index],
          langpair: "en|zh-CN",
        }).toString();
        const response = await fetch(endpoint, {
          headers: { Accept: "application/json" },
          signal: AbortSignal.timeout(3000),
        });
        if (!response.ok) return;
        const body = await response.json();
        translations[index] = String(
          body?.responseData?.translatedText ?? "",
        ).trim();
      } catch {
        // Missing translations remain empty and can be retried later.
      }
    }),
  );

  translations.forEach((translation, index) => {
    if (translation) {
      ctx.waitUntil(
        cache.put(
          keys[index],
          new Response(translation, {
            headers: { "Cache-Control": "public, max-age=2592000" },
          }),
        ),
      );
    }
  });
  return Response.json(
    { translations },
    { headers: { "Cache-Control": "no-store" } },
  );
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/bootstrap/")) {
      const key = decodeURIComponent(url.pathname.slice("/bootstrap/".length));
      if (
        !["GET", "HEAD"].includes(request.method) ||
        !bootstrapObjects.has(key) ||
        request.headers.get("X-Lexora-Origin-Token") !== env.ORIGIN_TOKEN
      ) {
        return Response.json({ detail: "not found" }, { status: 404 });
      }
      const object = await env.DOWNLOADS?.get(key);
      if (!object) {
        return Response.json({ detail: "bootstrap object unavailable" }, { status: 503 });
      }
      const headers = new Headers();
      object.writeHttpMetadata(headers);
      headers.set("ETag", object.httpEtag);
      headers.set("Content-Length", String(object.size));
      headers.set("Cache-Control", "private, no-store");
      return new Response(request.method === "HEAD" ? null : object.body, { headers });
    }
    if (url.pathname.startsWith("/internal/api/")) {
      if (request.headers.get("X-Lexora-Origin-Token") !== env.ORIGIN_TOKEN) {
        return Response.json({ detail: "not found" }, { status: 404 });
      }
      if (
        url.pathname === "/internal/api/dictionary/batch" &&
        request.method === "POST"
      ) {
        return enrichmentDictionaryBatch(request, ctx);
      }
      if (
        url.pathname === "/internal/api/translate/batch" &&
        request.method === "POST"
      ) {
        return enrichmentTranslationBatch(request, ctx);
      }
      return Response.json({ detail: "not found" }, { status: 404 });
    }
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type",
        },
      });
    }
    if (!["GET", "HEAD"].includes(request.method) || !allowedPaths.has(url.pathname)) {
      return Response.json({ detail: "not found" }, { status: 404 });
    }

    const cache = caches.default;
    const cacheKey = new Request(url.toString(), { method: "GET" });
    if (request.method === "GET" && url.pathname !== "/health") {
      const cached = await cache.match(cacheKey);
      if (cached) return withCors(cached, "cache", "HIT");
    }

    const { response, originName } = await fetchWithFailover(request, env);
    const ttl =
      response.ok && url.pathname === "/v1/lookup"
        ? 86400
        : response.ok && url.pathname === "/v1/suggest"
          ? 300
          : response.ok
            ? 60
            : 20;
    const headers = new Headers(response.headers);
    headers.set("Cache-Control", `public, max-age=${ttl}`);
    const relayResponse = new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
    if (request.method === "GET" && response.status < 500 && url.pathname !== "/health") {
      ctx.waitUntil(cache.put(cacheKey, relayResponse.clone()));
    }
    return withCors(relayResponse, originName, "MISS");
  },
};
