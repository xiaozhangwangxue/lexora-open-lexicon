const allowedPaths = new Set([
  "/health",
  "/manifest",
  "/oxford-manifest",
  "/v1/lookup",
  "/v1/progress",
  "/v1/suggest",
  "/v1/web/quota",
  "/v1/web/lookup",
  "/v1/web/suggest",
  "/v1/web/import",
  "/v1/web/generate",
]);
const bootstrapObjects = new Set([
  "lexora-open-oxford-scope.sqlite.gz.part-00-0",
  "lexora-open-oxford-scope.sqlite.gz.part-00-1",
  "lexora-open-oxford-scope.sqlite.gz.part-01",
]);
const offlineObjectPrefix = "lexora-offline/";
const datamuseDailyLimit = 90000;
const datamuseBurstCapacity = 32;
const datamuseRefillPerSecond = 1;
const datamuseLimiterObjectName = "global-v1";

function offlineObjectName(pathname) {
  const prefix = "/v1/offline/download/";
  if (!pathname.startsWith(prefix)) return null;
  const filename = decodeURIComponent(pathname.slice(prefix.length));
  if (!/^[a-z0-9][a-z0-9._-]{0,159}$/i.test(filename)) return null;
  return `${offlineObjectPrefix}${filename}`;
}

function withCors(response, originName, cacheStatus) {
  const headers = new Headers(response.headers);
  headers.set("Access-Control-Allow-Origin", "*");
  headers.set("Access-Control-Allow-Methods", "GET, HEAD, POST, OPTIONS");
  headers.set(
    "Access-Control-Allow-Headers",
    "Content-Type, Range, If-Range, If-None-Match, X-Lexora-Client-Hash",
  );
  headers.set(
    "Access-Control-Expose-Headers",
    "Accept-Ranges, Content-Disposition, Content-Length, Content-Range, ETag, X-Lexora-Daily-Remaining, X-Lexora-Filename, X-Lexora-Skipped",
  );
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

async function offlineObjectResponse(request, env, key, isManifest = false) {
  if (!env.DOWNLOADS) {
    return Response.json(
      { detail: "offline lexicon storage unavailable" },
      { status: 503 },
    );
  }
  const object =
    request.method === "HEAD"
      ? await env.DOWNLOADS.head(key)
      : await env.DOWNLOADS.get(key, {
          onlyIf: request.headers,
          range: request.headers,
        });
  if (!object) {
    return Response.json({ detail: "offline lexicon not found" }, { status: 404 });
  }

  const headers = new Headers();
  object.writeHttpMetadata(headers);
  headers.set("ETag", object.httpEtag);
  headers.set("Accept-Ranges", "bytes");
  headers.set(
    "Cache-Control",
    isManifest ? "public, max-age=300" : "public, max-age=86400, immutable",
  );
  headers.set(
    "Content-Type",
    isManifest ? "application/json; charset=utf-8" : "application/gzip",
  );

  let status = 200;
  if ("range" in object && object.range) {
    const offset = object.range.offset ?? 0;
    const length = object.range.length ?? object.size;
    headers.set(
      "Content-Range",
      `bytes ${offset}-${offset + length - 1}/${object.size}`,
    );
    headers.set("Content-Length", String(length));
    status = 206;
  } else {
    headers.set("Content-Length", String(object.size));
  }
  if (!isManifest) {
    headers.set(
      "Content-Disposition",
      `attachment; filename="${key.slice(offlineObjectPrefix.length)}"`,
    );
  }
  const body =
    request.method === "HEAD" || !("body" in object) ? null : object.body;
  return withCors(
    new Response(body, { status, headers }),
    "r2",
    isManifest ? "MANIFEST" : "DOWNLOAD",
  );
}

async function fetchOrigin(origin, originName, request, env, body) {
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
  const clientHash = request.headers.get("X-Lexora-Client-Hash");
  if (clientHash) headers.set("X-Lexora-Client-Hash", clientHash);
  if (request.method === "POST") {
    headers.set(
      "Content-Type",
      request.headers.get("Content-Type") || "application/json",
    );
  }
  const timeoutSetting =
    originName === "primary"
      ? env.PRIMARY_TIMEOUT_MS
      : env.SECONDARY_TIMEOUT_MS;
  const configuredTimeout = Number.parseInt(timeoutSetting || "8000", 10);
  const normalTimeout =
    Number.isFinite(configuredTimeout) && configuredTimeout >= 500
      ? configuredTimeout
      : 8000;
  const timeoutMs = incoming.pathname === "/v1/web/generate"
    ? 120000
    : incoming.pathname === "/v1/web/import"
      ? 30000
      : normalTimeout;
  const response = await fetch(target, {
    method: request.method,
    headers,
    body,
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
  ].filter(([origin]) => Boolean(origin));
  let lastResponse;
  const body = request.method === "POST" ? await request.arrayBuffer() : undefined;

  // Dictionary lookups are read-only and both Always Free origins contain the
  // same published database. Race them so a stalled instance cannot add its
  // full timeout before the healthy instance is tried. Quota-bearing web and
  // document requests intentionally remain sequential below.
  const pathname = new URL(request.url).pathname;
  if (request.method === "GET" && pathname === "/v1/lookup") {
    try {
      return await Promise.any(
        origins.map(async ([origin, originName]) => {
          const result = await fetchOrigin(
            origin,
            originName,
            request,
            env,
            undefined,
          );
          if (result.response.status >= 500) throw result;
          return result;
        }),
      );
    } catch (error) {
      const failures =
        error instanceof AggregateError ? error.errors : [error];
      lastResponse = failures
        .map((failure) => failure?.response ? failure : null)
        .find(Boolean);
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
  }
  for (const [origin, originName] of origins) {
    if (!origin) continue;
    try {
      const result = await fetchOrigin(origin, originName, request, env, body);
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

async function combinedProgressResponse(request, env) {
  const origins = [
    [env.PRIMARY_ORIGIN, "primary"],
    [env.SECONDARY_ORIGIN, "secondary"],
  ].filter(([origin]) => Boolean(origin));
  const results = await Promise.allSettled(
    origins.map(async ([origin, name]) => {
      const target = new URL("/v1/progress", origin);
      const response = await fetch(target, {
        headers: {
          Accept: "application/json",
          "X-Lexora-Origin-Token": env.ORIGIN_TOKEN,
        },
        signal: AbortSignal.timeout(8000),
        cf: { cacheTtl: 0 },
      });
      if (!response.ok) throw new Error(`${name} progress unavailable`);
      const value = await response.json();
      return {
        shard: Number(value.shard),
        finished: Number(value.finished),
        total: Number(value.total),
        remaining: Number(value.remaining),
        percent: Number(value.percent),
        entryStatus: value.entryStatus || {},
        providerStatus: value.providerStatus || {},
        providerAttempts: Number(value.providerAttempts || 0),
        top20k: value.top20k || null,
        updatedAt: value.updatedAt || null,
      };
    }),
  );
  const shards = results
    .filter((result) => result.status === "fulfilled")
    .map((result) => result.value)
    .filter(
      (value) =>
        Number.isInteger(value.shard) &&
        Number.isFinite(value.finished) &&
        Number.isFinite(value.total) &&
        value.finished >= 0 &&
        value.total > 0 &&
        value.finished <= value.total,
    )
    .sort((left, right) => left.shard - right.shard);
  const shardIds = new Set(shards.map((shard) => shard.shard));
  const expectedShardIds = Array.from(
    { length: origins.length },
    (_, index) => index,
  );
  if (
    shards.length !== origins.length ||
    shardIds.size !== origins.length ||
    !expectedShardIds.every((shard) => shardIds.has(shard))
  ) {
    return withCors(
      Response.json(
        { detail: "collection progress temporarily unavailable" },
        { status: 503, headers: { "Cache-Control": "no-store" } },
      ),
      "progress",
      "MISS",
    );
  }
  const finished = shards.reduce((sum, shard) => sum + shard.finished, 0);
  const total = shards.reduce((sum, shard) => sum + shard.total, 0);
  const sumMaps = (values) => {
    const combined = {};
    for (const value of values) {
      if (!value || typeof value !== "object" || Array.isArray(value)) continue;
      for (const [key, rawCount] of Object.entries(value)) {
        const count = Number(rawCount);
        if (!Number.isFinite(count) || count < 0) continue;
        combined[key] = (combined[key] || 0) + count;
      }
    }
    return combined;
  };
  const latestTimestamp = (values) =>
    values.filter(Boolean).sort().at(-1) || null;
  const qualityShards = shards
    .filter((shard) => shard.top20k && typeof shard.top20k === "object")
    .map((shard) => ({ shard: shard.shard, ...shard.top20k }));
  const qualityTotal = qualityShards.reduce(
    (sum, shard) => sum + Number(shard.total || 0),
    0,
  );
  const qualityComplete = qualityShards.reduce(
    (sum, shard) => sum + Number(shard.complete || 0),
    0,
  );
  const qualityIncomplete = qualityShards.reduce(
    (sum, shard) => sum + Number(shard.incomplete || 0),
    0,
  );
  const qualityGateVersions = new Set(
    qualityShards.map((shard) => Number(shard.qualityGateVersion || 0)),
  );
  const qualityGateVersion =
    qualityGateVersions.size === 1 ? [...qualityGateVersions][0] : null;
  const candidateDigests = new Set(
    qualityShards.map((shard) => String(shard.candidateDigest || "")),
  );
  const candidateDigest =
    candidateDigests.size === 1 ? [...candidateDigests][0] : null;
  const qualityAvailable =
    qualityShards.length === origins.length &&
    Number.isInteger(qualityGateVersion) &&
    qualityGateVersion > 0 &&
    Boolean(candidateDigest);
  const top20k = {
    available: qualityAvailable,
    ready:
      qualityAvailable &&
      qualityTotal === 20000 &&
      qualityIncomplete === 0,
    total: qualityTotal,
    qualityGateVersion,
    candidateDigest,
    complete: qualityComplete,
    incomplete: qualityIncomplete,
    percent:
      qualityTotal > 0
        ? Number(((qualityComplete / qualityTotal) * 100).toFixed(3))
        : 0,
    terms: sumMaps(qualityShards.map((shard) => shard.terms)),
    missing: sumMaps(qualityShards.map((shard) => shard.missing)),
    entryStatus: sumMaps(
      qualityShards.map((shard) => shard.entryStatus),
    ),
    updatedAt: latestTimestamp(
      qualityShards.map((shard) => shard.updatedAt),
    ),
    unresolved: qualityShards
      .flatMap((shard) =>
        Array.isArray(shard.unresolved)
          ? shard.unresolved.map((item) => ({ ...item, shard: shard.shard }))
          : [],
      )
      .slice(0, 12),
    shards: qualityShards.map((shard) => ({
      shard: shard.shard,
      total: Number(shard.total || 0),
      complete: Number(shard.complete || 0),
      incomplete: Number(shard.incomplete || 0),
      percent: Number(shard.percent || 0),
      updatedAt: shard.updatedAt || null,
    })),
  };
  const response = Response.json(
    {
      finished,
      total,
      remaining: Math.max(0, total - finished),
      percent: total > 0 ? Number(((finished / total) * 100).toFixed(3)) : 100,
      entryStatus: sumMaps(shards.map((shard) => shard.entryStatus)),
      providerStatus: sumMaps(shards.map((shard) => shard.providerStatus)),
      providerAttempts: shards.reduce(
        (sum, shard) => sum + Number(shard.providerAttempts || 0),
        0,
      ),
      updatedAt: latestTimestamp(shards.map((shard) => shard.updatedAt)),
      oldestShardUpdatedAt:
        shards.map((shard) => shard.updatedAt).filter(Boolean).sort().at(0) ||
        null,
      shards,
      top20k,
    },
    { headers: { "Cache-Control": "public, max-age=60" } },
  );
  if (request.method === "HEAD") {
    return withCors(
      new Response(null, { status: response.status, headers: response.headers }),
      "progress",
      "MISS",
    );
  }
  return withCors(response, "progress", "MISS");
}

function normalizeTerm(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function isValidTerm(term) {
  // Match the canonical dataset's term envelope.  Dotted abbreviations and
  // entries up to 120 characters are valid rows and must not silently vanish
  // from a successful outer batch response.
  return /^[a-z][a-z' .-]{0,119}$/.test(term);
}

const enrichmentNeedNames = [
  "definition",
  "pos",
  "phonetic",
  "examples",
  "frequency",
  "deep",
  "synonyms",
  "antonyms",
  "phrases",
  "related",
  "usPhonetic",
  "ukPhonetic",
];

function legacyEnrichmentNeeds(profile) {
  const deep = profile === "deep";
  return {
    definition: true,
    pos: true,
    phonetic: true,
    examples: true,
    frequency: true,
    deep,
    synonyms: deep,
    antonyms: deep,
    phrases: deep,
    related: deep,
    usPhonetic: true,
    ukPhonetic: true,
  };
}

function normalizeEnrichmentNeeds(value, profile, legacy = false) {
  const granularDeepNames = [
    "synonyms",
    "antonyms",
    "phrases",
    "related",
  ];
  const source =
    !legacy && value && typeof value === "object" && !Array.isArray(value)
      ? value
      : legacyEnrichmentNeeds(profile);
  const needs = Object.fromEntries(
    enrichmentNeedNames.map((name) => [name, source[name] === true]),
  );
  // Dialect hints refine ``phonetic`` and are never standalone requests.
  if (!needs.phonetic) {
    needs.usPhonetic = false;
    needs.ukPhonetic = false;
  } else if (!needs.usPhonetic && !needs.ukPhonetic) {
    // Clients predating the optional hints still get a useful generic
    // pronunciation lookup.
    needs.usPhonetic = true;
  }
  // A core request cannot accidentally opt into relationship providers.
  if (profile !== "deep") {
    needs.deep = false;
    for (const name of granularDeepNames) needs[name] = false;
    return needs;
  }
  const hasGranularDeepNeeds =
    !legacy &&
    granularDeepNames.some((name) =>
      Object.prototype.hasOwnProperty.call(source, name),
    );
  // ``deep: true`` was the only relationship selector used by v5 clients.
  // Preserve that exact all-relationship meaning unless a newer client sent
  // at least one granular selector.
  if (needs.deep && !hasGranularDeepNeeds) {
    for (const name of granularDeepNames) needs[name] = true;
  }
  needs.deep = granularDeepNames.some((name) => needs[name]);
  return needs;
}

function mergeEnrichmentNeeds(target, incoming) {
  return Object.fromEntries(
    enrichmentNeedNames.map((name) => [
      name,
      Boolean(target?.[name] || incoming?.[name]),
    ]),
  );
}

function enrichmentNeedSignature(needs) {
  return enrichmentNeedNames
    .map((name) => (needs[name] ? "1" : "0"))
    .join("");
}

function enrichmentCacheKey(term, profile, needs) {
  return new Request(
    `https://lexora-enrichment-cache.invalid/v6/${profile}/${enrichmentNeedSignature(needs)}/${encodeURIComponent(term)}`,
  );
}

function normalizedRetryAfter(value, fallback = 60) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) && parsed > 0
    ? Math.min(parsed, 86400)
    : fallback;
}

async function leaseDatamuseQuota(env, cost) {
  if (cost === 0) return { granted: true, retryAfter: 0 };
  if (!env.DATAMUSE_RATE_LIMITER) {
    return { granted: false, retryAfter: 60 };
  }

  try {
    const objectId =
      env.DATAMUSE_RATE_LIMITER.idFromName(datamuseLimiterObjectName);
    const stub = env.DATAMUSE_RATE_LIMITER.get(objectId);
    const response = await stub.fetch(
      "https://datamuse-rate-limiter.invalid/lease",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ cost }),
      },
    );
    const retryAfter = normalizedRetryAfter(
      response.headers.get("Retry-After"),
    );
    if (!response.ok) return { granted: false, retryAfter };

    let result;
    try {
      result = await response.json();
    } catch {
      return { granted: false, retryAfter: 60 };
    }
    return result?.granted === true
      ? { granted: true, retryAfter: 0 }
      : { granted: false, retryAfter };
  } catch {
    // The limiter is the source of truth for the free Datamuse allowance.
    // Failing closed prevents an outage from accidentally becoming unbounded.
    return { granted: false, retryAfter: 60 };
  }
}

async function datamuseQuotaStatus(env) {
  if (!env.DATAMUSE_RATE_LIMITER) {
    return Response.json(
      { error: "Datamuse quota limiter is unavailable" },
      {
        status: 503,
        headers: { "Cache-Control": "no-store" },
      },
    );
  }
  try {
    const objectId =
      env.DATAMUSE_RATE_LIMITER.idFromName(datamuseLimiterObjectName);
    const stub = env.DATAMUSE_RATE_LIMITER.get(objectId);
    const response = await stub.fetch(
      "https://datamuse-rate-limiter.invalid/status",
    );
    const headers = new Headers(response.headers);
    headers.set("Cache-Control", "no-store");
    return new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
  } catch {
    return Response.json(
      { error: "Datamuse quota status is unavailable" },
      {
        status: 503,
        headers: { "Cache-Control": "no-store" },
      },
    );
  }
}

async function enrichmentTerm(
  term,
  profile,
  needs,
  quotaLease,
  ctx,
  {
    cachedResponse = null,
    cacheChecked = false,
  } = {},
) {
  const cache = caches.default;
  const cacheKey = enrichmentCacheKey(term, profile, needs);
  if (cachedResponse) return cachedResponse;
  if (!cacheChecked) {
    const cached = await cache.match(cacheKey);
    if (cached) return cached;
  }

  const availableRequests = {
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

  const jsonProvider = async (input) => {
    try {
      const response = await fetch(input, {
        headers: { Accept: "application/json" },
        signal: AbortSignal.timeout(2200),
      });

      if (response.status === 404 || response.status === 204) {
        // Cloudflare counts unread response bodies as live subrequests. A
        // batch containing several misses could otherwise exhaust the
        // connection pool and have a healthy request canceled as a 504.
        response.body?.cancel();
        return {
          data: null,
          provider: {
            ok: true,
            status: response.status,
            found: false,
          },
        };
      }
      if (!response.ok) {
        response.body?.cancel();
        return {
          data: null,
          provider: {
            ok: false,
            status: response.status,
            found: false,
          },
        };
      }

      let data;
      try {
        data = await response.json();
      } catch {
        return {
          data: null,
          provider: {
            ok: false,
            status: response.status,
            found: false,
          },
        };
      }
      const found = Array.isArray(data)
        ? data.length > 0
        : data !== null &&
          typeof data === "object" &&
          Object.keys(data).length > 0;
      return {
        data,
        provider: {
          ok: true,
          status: response.status,
          found,
        },
      };
    } catch {
      return {
        data: null,
        provider: {
          ok: false,
          status: null,
          found: false,
        },
      };
    }
  };

  const dictionaryCapabilities = (data) => {
    const capabilities = {
      definition: false,
      pos: false,
      examples: false,
      synonyms: false,
      antonyms: false,
      usPhonetic: false,
      ukPhonetic: false,
    };
    if (!Array.isArray(data)) return capabilities;
    const hasValues = (value) =>
      Array.isArray(value) &&
      value.some((item) => String(item ?? "").trim());
    for (const entry of data) {
      if (!entry || typeof entry !== "object") continue;
      for (const phonetic of Array.isArray(entry.phonetics)
        ? entry.phonetics
        : []) {
        const text = String(phonetic?.text ?? "").trim();
        if (!text) continue;
        const audio = String(phonetic?.audio ?? "").toLowerCase();
        if (audio.includes("-us.") || audio.includes("/us/")) {
          capabilities.usPhonetic = true;
        } else if (audio.includes("-uk.") || audio.includes("/uk/")) {
          capabilities.ukPhonetic = true;
        }
      }
      for (const meaning of Array.isArray(entry.meanings)
        ? entry.meanings
        : []) {
        if (String(meaning?.partOfSpeech ?? "").trim()) {
          capabilities.pos = true;
        }
        if (hasValues(meaning?.synonyms)) capabilities.synonyms = true;
        if (hasValues(meaning?.antonyms)) capabilities.antonyms = true;
        for (const definition of Array.isArray(meaning?.definitions)
          ? meaning.definitions
          : []) {
          if (String(definition?.definition ?? "").trim()) {
            capabilities.definition = true;
          }
          if (String(definition?.example ?? "").trim()) {
            capabilities.examples = true;
          }
          if (hasValues(definition?.synonyms)) {
            capabilities.synonyms = true;
          }
          if (hasValues(definition?.antonyms)) {
            capabilities.antonyms = true;
          }
        }
      }
    }
    return capabilities;
  };

  const exactCapabilities = (data) => {
    const capabilities = {
      definition: false,
      pos: false,
      usPhonetic: false,
      frequency: false,
    };
    if (!Array.isArray(data)) return capabilities;
    for (const item of data) {
      if (
        !item ||
        typeof item !== "object" ||
        normalizeTerm(item.word) !== term
      ) {
        continue;
      }
      if (
        Array.isArray(item.defs) &&
        item.defs.some((value) => String(value ?? "").trim())
      ) {
        capabilities.definition = true;
      }
      for (const tag of Array.isArray(item.tags) ? item.tags : []) {
        const value = String(tag);
        if (["n", "v", "adj", "adv"].includes(value)) {
          capabilities.pos = true;
        } else if (
          value.startsWith("ipa_pron:") &&
          value.slice("ipa_pron:".length).trim()
        ) {
          capabilities.usPhonetic = true;
        } else if (value.startsWith("f:")) {
          const frequency = Number(value.slice(2));
          if (Number.isFinite(frequency) && frequency > 0) {
            capabilities.frequency = true;
          }
        }
      }
    }
    return capabilities;
  };

  const dictionaryNeeded =
    needs.definition ||
    needs.pos ||
    needs.phonetic ||
    needs.examples ||
    needs.synonyms ||
    needs.antonyms;
  const providerResults = new Map();
  if (dictionaryNeeded) {
    providerResults.set(
      "dictionary",
      await jsonProvider(availableRequests.dictionary),
    );
  }
  const dictionary = dictionaryCapabilities(
    providerResults.get("dictionary")?.data,
  );

  // Datamuse exact adds definition/POS/US IPA and frequency.  It cannot add
  // examples or a UK transcription, so those gaps alone must not consume the
  // globally limited free allowance.
  const exactFallbackNeeded =
    needs.frequency ||
    (needs.definition && !dictionary.definition) ||
    (needs.pos && !dictionary.pos) ||
    (needs.phonetic &&
      needs.usPhonetic &&
      !dictionary.usPhonetic);
  const datamuseNames = [
    ...(needs.related || needs.phrases ? ["related"] : []),
    ...(exactFallbackNeeded ? ["exact"] : []),
    ...(needs.synonyms && !dictionary.synonyms
      ? ["synonyms"]
      : []),
    ...(needs.antonyms && !dictionary.antonyms
      ? ["antonyms"]
      : []),
  ];
  const lease = await quotaLease(datamuseNames.length);
  if (!lease.granted) {
    return Response.json(
      {
        error: "Datamuse free quota is temporarily unavailable",
        retryAfter: lease.retryAfter,
      },
      {
        status: 429,
        headers: {
          "Cache-Control": "no-store",
          "Retry-After": String(lease.retryAfter),
        },
      },
    );
  }
  const datamuseOutcomes = await Promise.all(
    datamuseNames.map((name) => jsonProvider(availableRequests[name])),
  );
  datamuseNames.forEach((name, index) => {
    providerResults.set(name, datamuseOutcomes[index]);
  });

  const requestNames = Array.from(providerResults.keys());
  const outcomes = requestNames.map((name) => providerResults.get(name));
  if (
    outcomes.length > 0 &&
    outcomes.every((outcome) => !outcome.provider.ok)
  ) {
    return Response.json(
      { error: "Dictionary providers are temporarily unavailable" },
      { status: 504, headers: { "Cache-Control": "no-store" } },
    );
  }

  const result = Object.fromEntries(
    requestNames.map((key, index) => [key, outcomes[index].data]),
  );
  result._providers = Object.fromEntries(
    requestNames.map((key, index) => [key, outcomes[index].provider]),
  );
  result._found = outcomes.some((outcome) => outcome.provider.found);
  result._profile = profile;
  result._needs = needs;

  const exact = exactCapabilities(providerResults.get("exact")?.data);
  const phoneticComplete =
    !needs.phonetic ||
    ((!needs.usPhonetic ||
      dictionary.usPhonetic ||
      exact.usPhonetic) &&
      (!needs.ukPhonetic || dictionary.ukPhonetic));
  const relatedComplete =
    (!needs.related && !needs.phrases) ||
    providerResults.get("related")?.provider?.ok === true;
  const synonymsComplete =
    !needs.synonyms ||
    dictionary.synonyms ||
    providerResults.get("synonyms")?.provider?.ok === true;
  const antonymsComplete =
    !needs.antonyms ||
    dictionary.antonyms ||
    providerResults.get("antonyms")?.provider?.ok === true;
  result._complete =
    (!needs.definition ||
      dictionary.definition ||
      exact.definition) &&
    (!needs.pos || dictionary.pos || exact.pos) &&
    phoneticComplete &&
    (!needs.examples || dictionary.examples) &&
    (!needs.frequency || exact.frequency) &&
    relatedComplete &&
    synonymsComplete &&
    antonymsComplete;

  const allProvidersSucceeded = outcomes.every(
    (outcome) => outcome.provider.ok,
  );
  const cacheTtl =
    allProvidersSucceeded && (result._complete || !result._found)
      ? 604800
      : 3600;
  const response = Response.json(result, {
    headers: { "Cache-Control": `public, max-age=${cacheTtl}` },
  });
  ctx.waitUntil(cache.put(cacheKey, response.clone()));
  return response;
}

async function enrichmentDictionaryBatch(request, env, ctx) {
  let requests;
  // Older collectors did not send a profile.  Default them to the bounded
  // core pass so a rolling deployment can never multiply Datamuse usage.
  let profile = "core";
  try {
    const payload = await request.json();
    profile = payload.profile === "deep" ? "deep" : "core";
    const explicitNeeds =
      payload.needs &&
      typeof payload.needs === "object" &&
      !Array.isArray(payload.needs)
        ? payload.needs
        : null;
    const byTerm = new Map();
    for (const raw of Array.isArray(payload.terms) ? payload.terms : []) {
      const rawTerm =
        raw && typeof raw === "object" && !Array.isArray(raw)
          ? raw.term
          : raw;
      const term = normalizeTerm(rawTerm);
      if (!isValidTerm(term)) continue;
      const inlineNeeds =
        raw && typeof raw === "object" && !Array.isArray(raw)
          ? raw.needs
          : null;
      const mappedNeeds =
        explicitNeeds &&
        Object.prototype.hasOwnProperty.call(explicitNeeds, term)
          ? explicitNeeds[term]
          : null;
      const rawNeeds = inlineNeeds ?? mappedNeeds;
      const needs = normalizeEnrichmentNeeds(
        rawNeeds,
        profile,
        rawNeeds === null,
      );
      if (byTerm.has(term)) {
        byTerm.set(
          term,
          mergeEnrichmentNeeds(byTerm.get(term), needs),
        );
      } else if (byTerm.size < 8) {
        byTerm.set(term, needs);
      }
    }
    requests = Array.from(byTerm, ([term, needs]) => ({ term, needs }));
  } catch {
    return Response.json({ error: "Invalid JSON" }, { status: 400 });
  }
  if (requests.length === 0) return Response.json({ results: {} });

  const cache = caches.default;
  const cacheKeys = requests.map(({ term, needs }) =>
    enrichmentCacheKey(term, profile, needs),
  );
  const cachedResponses = await Promise.all(
    cacheKeys.map((cacheKey) => cache.match(cacheKey)),
  );
  const missingCount = cachedResponses.filter(
    (response) => !response,
  ).length;
  const pendingLeases = [];
  let leaseStarted = false;
  const coordinatedLease = (cost) =>
    new Promise((resolve) => {
      pendingLeases.push({ cost, resolve });
      if (leaseStarted || pendingLeases.length < missingCount) return;
      leaseStarted = true;
      const totalCost = pendingLeases.reduce(
        (sum, item) => sum + item.cost,
        0,
      );
      leaseDatamuseQuota(env, totalCost)
        .then((lease) => {
          pendingLeases.forEach((item) =>
            item.resolve(
              item.cost === 0
                ? { granted: true, retryAfter: 0 }
                : lease,
            ),
          );
        })
        .catch(() => {
          const failure = { granted: false, retryAfter: 60 };
          pendingLeases.forEach((item) =>
            item.resolve(
              item.cost === 0
                ? { granted: true, retryAfter: 0 }
                : failure,
            ),
          );
        });
    });

  const entries = await Promise.all(
    requests.map(async ({ term, needs }, index) => {
      const response = await enrichmentTerm(
        term,
        profile,
        needs,
        coordinatedLease,
        ctx,
        {
          cachedResponse: cachedResponses[index],
          cacheChecked: true,
        },
      );
      let data;
      try {
        data = await response.json();
      } catch {
        data = { error: "Invalid upstream response" };
      }
      return {
        term,
        response,
        item: { status: response.status, data },
      };
    }),
  );
  const quotaFailures = entries.filter(
    ({ response }) => response.status === 429,
  );
  if (quotaFailures.length > 0) {
    const retryAfter = Math.max(
      ...quotaFailures.map(({ response }) =>
        normalizedRetryAfter(response.headers.get("Retry-After")),
      ),
    );
    return Response.json(
      {
        error: "Datamuse free quota is temporarily unavailable",
        retryAfter,
      },
      {
        status: 429,
        headers: {
          "Cache-Control": "no-store",
          "Retry-After": String(retryAfter),
        },
      },
    );
  }
  return Response.json(
    {
      results: Object.fromEntries(
        entries.map(({ term, item }) => [term, item]),
      ),
    },
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

  // Do not concatenate multiple definitions with artificial markers.  Google
  // can translate or alter those markers, which makes otherwise successful
  // translations impossible to split reliably.  Small bounded waves keep the
  // input/output mapping exact and stay well below the Worker subrequest cap.
  for (let start = 0; start < missingIndexes.length; start += 8) {
    const wave = missingIndexes.slice(start, start + 8);
    await Promise.all(
      wave.map(async (index) => {
        try {
          const endpoint = new URL(
            "https://translate.googleapis.com/translate_a/single",
          );
          endpoint.search = new URLSearchParams({
            client: "gtx",
            sl: "en",
            tl: "zh-CN",
            dt: "t",
            q: texts[index],
          }).toString();
          const response = await fetch(endpoint, {
            headers: { Accept: "application/json" },
            signal: AbortSignal.timeout(5000),
          });
          if (!response.ok) return;
          const body = await response.json();
          const chunks = Array.isArray(body?.[0]) ? body[0] : [];
          const translated = chunks
            .map((chunk) => String(chunk?.[0] ?? ""))
            .join("")
            .trim();
          if (translated) translations[index] = translated;
        } catch {
          // The bounded MyMemory fallback below handles transient failures.
        }
      }),
    );
  }

  const stillMissing = translations
    .map((value, index) => (value ? -1 : index))
    .filter((index) => index >= 0);
  // A request may already have used one Google subrequest per missing text.
  // Keep the whole invocation below 50 provider subrequests; anything beyond
  // this bounded fallback budget remains empty and is safely retried later.
  const fallbackBudget = Math.max(0, 48 - missingIndexes.length);
  const fallbackIndexes = stillMissing.slice(0, fallbackBudget);
  for (let start = 0; start < fallbackIndexes.length; start += 4) {
    const wave = fallbackIndexes.slice(start, start + 4);
    await Promise.all(
      wave.map(async (index) => {
        try {
          const endpoint = new URL("https://api.mymemory.translated.net/get");
          endpoint.search = new URLSearchParams({
            q: texts[index],
            langpair: "en|zh-CN",
          }).toString();
          const response = await fetch(endpoint, {
            headers: { Accept: "application/json" },
            signal: AbortSignal.timeout(5000),
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
  }

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

function utcDay(now) {
  return new Date(now).toISOString().slice(0, 10);
}

function secondsUntilNextUtcDay(now) {
  const current = new Date(now);
  const next = Date.UTC(
    current.getUTCFullYear(),
    current.getUTCMonth(),
    current.getUTCDate() + 1,
  );
  return Math.max(1, Math.ceil((next - now) / 1000));
}

export class DatamuseRateLimiter {
  constructor(state) {
    this.state = state;
    state.blockConcurrencyWhile(async () => {
      const now = Date.now();
      state.storage.sql.exec(`
        CREATE TABLE IF NOT EXISTS datamuse_quota (
          singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
          utc_day TEXT NOT NULL,
          used INTEGER NOT NULL,
          tokens REAL NOT NULL,
          last_refill_ms INTEGER NOT NULL
        )
      `);
      state.storage.sql.exec(
        `
          INSERT OR IGNORE INTO datamuse_quota (
            singleton,
            utc_day,
            used,
            tokens,
            last_refill_ms
          ) VALUES (1, ?, 0, ?, ?)
        `,
        utcDay(now),
        datamuseBurstCapacity,
        now,
      );
    });
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/status") {
      const now = Date.now();
      const today = utcDay(now);
      const rows = Array.from(
        this.state.storage.sql.exec(
          `
            SELECT utc_day, used, tokens, last_refill_ms
            FROM datamuse_quota
            WHERE singleton = 1
          `,
        ),
      );
      const row = rows[0];
      if (!row) {
        return Response.json(
          { error: "quota state unavailable" },
          {
            status: 503,
            headers: { "Cache-Control": "no-store" },
          },
        );
      }
      const elapsedSeconds = Math.max(
        0,
        (now - Number(row.last_refill_ms)) / 1000,
      );
      const tokens = Math.min(
        datamuseBurstCapacity,
        Number(row.tokens) + elapsedSeconds * datamuseRefillPerSecond,
      );
      const used = row.utc_day === today ? Number(row.used) : 0;
      this.state.storage.sql.exec(
        `
          UPDATE datamuse_quota
          SET utc_day = ?, used = ?, tokens = ?, last_refill_ms = ?
          WHERE singleton = 1
        `,
        today,
        used,
        tokens,
        now,
      );
      return Response.json(
        {
          utcDay: today,
          dailyLimit: datamuseDailyLimit,
          dailyUsed: used,
          dailyRemaining: Math.max(0, datamuseDailyLimit - used),
          burstCapacity: datamuseBurstCapacity,
          burstRemaining: Math.floor(tokens),
          refillPerSecond: datamuseRefillPerSecond,
          resetsInSeconds: secondsUntilNextUtcDay(now),
        },
        { headers: { "Cache-Control": "no-store" } },
      );
    }
    if (request.method !== "POST" || url.pathname !== "/lease") {
      return Response.json({ error: "not found" }, { status: 404 });
    }

    let cost;
    try {
      const payload = await request.json();
      cost = Number(payload?.cost);
    } catch {
      return Response.json({ error: "Invalid JSON" }, { status: 400 });
    }
    if (
      !Number.isSafeInteger(cost) ||
      cost < 1 ||
      cost > datamuseBurstCapacity
    ) {
      return Response.json(
        { error: `cost must be an integer from 1 to ${datamuseBurstCapacity}` },
        { status: 400 },
      );
    }

    const now = Date.now();
    const today = utcDay(now);
    const rows = Array.from(
      this.state.storage.sql.exec(
        `
          SELECT utc_day, used, tokens, last_refill_ms
          FROM datamuse_quota
          WHERE singleton = 1
        `,
      ),
    );
    const row = rows[0];
    if (!row) {
      return Response.json(
        { error: "quota state unavailable" },
        {
          status: 503,
          headers: { "Cache-Control": "no-store" },
        },
      );
    }

    const elapsedSeconds = Math.max(
      0,
      (now - Number(row.last_refill_ms)) / 1000,
    );
    let tokens = Math.min(
      datamuseBurstCapacity,
      Number(row.tokens) + elapsedSeconds * datamuseRefillPerSecond,
    );
    let used = row.utc_day === today ? Number(row.used) : 0;

    let retryAfter = 0;
    if (used + cost > datamuseDailyLimit) {
      retryAfter = secondsUntilNextUtcDay(now);
    } else if (tokens < cost) {
      retryAfter = Math.max(
        1,
        Math.ceil((cost - tokens) / datamuseRefillPerSecond),
      );
    }

    if (retryAfter > 0) {
      this.state.storage.sql.exec(
        `
          UPDATE datamuse_quota
          SET utc_day = ?, used = ?, tokens = ?, last_refill_ms = ?
          WHERE singleton = 1
        `,
        today,
        used,
        tokens,
        now,
      );
      return Response.json(
        {
          granted: false,
          retryAfter,
          dailyRemaining: Math.max(0, datamuseDailyLimit - used),
        },
        {
          status: 429,
          headers: {
            "Cache-Control": "no-store",
            "Retry-After": String(retryAfter),
          },
        },
      );
    }

    tokens -= cost;
    used += cost;
    this.state.storage.sql.exec(
      `
        UPDATE datamuse_quota
        SET utc_day = ?, used = ?, tokens = ?, last_refill_ms = ?
        WHERE singleton = 1
      `,
      today,
      used,
      tokens,
      now,
    );
    return Response.json(
      {
        granted: true,
        leased: cost,
        dailyRemaining: datamuseDailyLimit - used,
        burstRemaining: Math.floor(tokens),
      },
      { headers: { "Cache-Control": "no-store" } },
    );
  }
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (
      ["GET", "HEAD"].includes(request.method) &&
      url.pathname === "/v1/offline/manifest.json"
    ) {
      return offlineObjectResponse(
        request,
        env,
        `${offlineObjectPrefix}manifest.json`,
        true,
      );
    }
    const offlineKey = offlineObjectName(url.pathname);
    if (["GET", "HEAD"].includes(request.method) && offlineKey) {
      return offlineObjectResponse(request, env, offlineKey);
    }
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
        return enrichmentDictionaryBatch(request, env, ctx);
      }
      if (
        url.pathname === "/internal/api/translate/batch" &&
        request.method === "POST"
      ) {
        return enrichmentTranslationBatch(request, ctx);
      }
      if (
        url.pathname === "/internal/api/datamuse-quota" &&
        request.method === "GET"
      ) {
        return datamuseQuotaStatus(env);
      }
      return Response.json({ detail: "not found" }, { status: 404 });
    }
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Methods": "GET, HEAD, POST, OPTIONS",
          "Access-Control-Allow-Headers":
            "Content-Type, Range, If-Range, If-None-Match, X-Lexora-Client-Hash",
        },
      });
    }
    if (
      ["GET", "HEAD"].includes(request.method) &&
      url.pathname === "/v1/progress"
    ) {
      const progressCache = caches.default;
      const normalizedProgressUrl = new URL(url.origin);
      normalizedProgressUrl.pathname = "/v1/progress";
      const progressKey = new Request(normalizedProgressUrl.toString(), {
        method: "GET",
      });
      const cached = await progressCache.match(progressKey);
      if (cached) {
        const headers = new Headers(cached.headers);
        headers.set("X-Lexora-Cache", "HIT");
        if (request.method === "HEAD") {
          return new Response(null, {
            status: cached.status,
            statusText: cached.statusText,
            headers,
          });
        }
        return new Response(cached.body, {
          status: cached.status,
          statusText: cached.statusText,
          headers,
        });
      }
      const fullRequest = new Request(request.url, {
        method: "GET",
        headers: request.headers,
      });
      const progress = await combinedProgressResponse(fullRequest, env);
      if (progress.ok) {
        ctx.waitUntil(progressCache.put(progressKey, progress.clone()));
      }
      if (request.method === "HEAD") {
        return new Response(null, {
          status: progress.status,
          statusText: progress.statusText,
          headers: progress.headers,
        });
      }
      return progress;
    }
    const allowedMethod =
      ["GET", "HEAD"].includes(request.method) ||
      (request.method === "POST" &&
        ["/v1/web/generate", "/v1/web/import"].includes(url.pathname));
    if (!allowedMethod || !allowedPaths.has(url.pathname)) {
      return Response.json({ detail: "not found" }, { status: 404 });
    }

    const cache = caches.default;
    const cacheKey = new Request(url.toString(), { method: "GET" });
    if (request.method === "GET" && url.pathname !== "/health" && !url.pathname.startsWith("/v1/web/")) {
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
    headers.set("Cache-Control", url.pathname.startsWith("/v1/web/") ? "no-store" : `public, max-age=${ttl}`);
    const relayResponse = new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
    if (request.method === "GET" && response.status < 500 && url.pathname !== "/health" && !url.pathname.startsWith("/v1/web/")) {
      ctx.waitUntil(cache.put(cacheKey, relayResponse.clone()));
    }
    return withCors(relayResponse, originName, "MISS");
  },
};
