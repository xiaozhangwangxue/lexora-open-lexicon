from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT / "worker" / "dictionary-edge.js"
WRANGLER = ROOT / "wrangler.dictionary.jsonc"
RATE_LIMITER_JS = """
          const leaseCosts = [];
          const quota = {
            idFromName: (name) => name,
            get: () => ({
              fetch: async (_url, init) => {
                const payload = JSON.parse(init.body);
                leaseCosts.push(payload.cost);
                return Response.json({ granted: true });
              },
            }),
          };
"""


class DictionaryEdgeWorkerTest(unittest.TestCase):
    def run_node(self, script: str) -> dict:
        completed = subprocess.run(
            ["node", "--no-warnings", "--input-type=module", "-e", script],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(completed.stdout)

    def test_core_profile_uses_one_datamuse_request_per_term(self) -> None:
        script = f"""
          {RATE_LIMITER_JS}
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          const urls = [];
          globalThis.fetch = async (input) => {{
            urls.push(String(input));
            return Response.json([]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const request = new Request(
            "https://dict.example/internal/api/dictionary/batch",
            {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json",
                "X-Lexora-Origin-Token": "test-token",
              }},
              body: JSON.stringify({{
                profile: "core",
                terms: ["word"],
              }}),
            }},
          );
          const response = await worker.fetch(
            request,
            {{
              ORIGIN_TOKEN: "test-token",
              DATAMUSE_RATE_LIMITER: quota,
            }},
            {{ waitUntil: () => undefined }},
          );
          const body = await response.json();
          const item = body.results.word;
          console.log(JSON.stringify({{
            status: response.status,
            profile: item.data._profile,
            providerNames: Object.keys(item.data._providers),
            datamuseRequests: urls.filter(
              (url) => url.includes("api.datamuse.com"),
            ).length,
            totalRequests: urls.length,
            leaseCosts,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertEqual(result["profile"], "core")
        self.assertEqual(result["providerNames"], ["dictionary", "exact"])
        self.assertEqual(result["datamuseRequests"], 1)
        self.assertEqual(result["totalRequests"], 2)
        self.assertEqual(result["leaseCosts"], [1])

    def test_dictionary_satisfied_core_needs_skip_datamuse_and_limiter(
        self,
    ) -> None:
        script = f"""
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          const urls = [];
          globalThis.fetch = async (input) => {{
            const url = String(input);
            urls.push(url);
            if (!url.includes("dictionaryapi.dev")) {{
              throw new Error(`unexpected provider: ${{url}}`);
            }}
            return Response.json([{{
              word: "word",
              phonetics: [{{
                text: "/wɝːd/",
                audio: "https://audio.example/word-us.mp3",
              }}],
              meanings: [{{
                partOfSpeech: "noun",
                definitions: [{{
                  definition: "A unit of language.",
                  example: "This is a word.",
                }}],
              }}],
            }}]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const response = await worker.fetch(
            new Request(
              "https://dict.example/internal/api/dictionary/batch",
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json",
                  "X-Lexora-Origin-Token": "test-token",
                }},
                body: JSON.stringify({{
                  profile: "core",
                  terms: ["word"],
                  needs: {{
                    word: {{
                      definition: true,
                      pos: true,
                      phonetic: true,
                      examples: true,
                      frequency: false,
                      deep: false,
                      usPhonetic: true,
                      ukPhonetic: false,
                    }},
                  }},
                }}),
              }},
            ),
            {{ ORIGIN_TOKEN: "test-token" }},
            {{ waitUntil: () => undefined }},
          );
          const body = await response.json();
          const item = body.results.word;
          console.log(JSON.stringify({{
            outerStatus: response.status,
            itemStatus: item.status,
            providers: Object.keys(item.data._providers),
            complete: item.data._complete,
            urls,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["outerStatus"], 200)
        self.assertEqual(result["itemStatus"], 200)
        self.assertEqual(result["providers"], ["dictionary"])
        self.assertTrue(result["complete"])
        self.assertEqual(len(result["urls"]), 1)
        self.assertIn("dictionaryapi.dev", result["urls"][0])

    def test_dictionary_gap_falls_back_to_one_leased_exact_request(
        self,
    ) -> None:
        script = f"""
          {RATE_LIMITER_JS}
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          const urls = [];
          globalThis.fetch = async (input) => {{
            const url = String(input);
            urls.push(url);
            if (url.includes("dictionaryapi.dev")) {{
              return Response.json([{{
                word: "word",
                phonetic: "/wɜːd/",
                phonetics: [{{ text: "/wɜːd/", audio: "" }}],
                meanings: [{{
                  partOfSpeech: "noun",
                  definitions: [{{
                    definition: "A unit of language.",
                  }}],
                }}],
              }}]);
            }}
            return Response.json([{{
              word: "word",
              defs: ["n\\tA unit of language."],
              tags: ["n", "ipa_pron:wɝːd"],
            }}]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const response = await worker.fetch(
            new Request(
              "https://dict.example/internal/api/dictionary/batch",
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json",
                  "X-Lexora-Origin-Token": "test-token",
                }},
                body: JSON.stringify({{
                  terms: ["word"],
                  needs: {{
                    word: {{
                      definition: true,
                      pos: true,
                      phonetic: true,
                      usPhonetic: true,
                    }},
                  }},
                }}),
              }},
            ),
            {{
              ORIGIN_TOKEN: "test-token",
              DATAMUSE_RATE_LIMITER: quota,
            }},
            {{ waitUntil: () => undefined }},
          );
          const item = (await response.json()).results.word;
          console.log(JSON.stringify({{
            status: response.status,
            providers: Object.keys(item.data._providers),
            complete: item.data._complete,
            dictionaryRequests: urls.filter(
              (url) => url.includes("dictionaryapi.dev"),
            ).length,
            datamuseRequests: urls.filter(
              (url) => url.includes("api.datamuse.com"),
            ).length,
            leaseCosts,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertEqual(result["providers"], ["dictionary", "exact"])
        self.assertTrue(result["complete"])
        self.assertEqual(result["dictionaryRequests"], 1)
        self.assertEqual(result["datamuseRequests"], 1)
        self.assertEqual(result["leaseCosts"], [1])

    def test_example_only_gap_does_not_waste_datamuse_exact_quota(
        self,
    ) -> None:
        script = f"""
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          const urls = [];
          globalThis.fetch = async (input) => {{
            urls.push(String(input));
            return Response.json([{{
              word: "word",
              meanings: [{{
                partOfSpeech: "noun",
                definitions: [{{ definition: "A unit of language." }}],
              }}],
            }}]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const response = await worker.fetch(
            new Request(
              "https://dict.example/internal/api/dictionary/batch",
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json",
                  "X-Lexora-Origin-Token": "test-token",
                }},
                body: JSON.stringify({{
                  terms: ["word"],
                  needs: {{ word: {{ examples: true }} }},
                }}),
              }},
            ),
            {{ ORIGIN_TOKEN: "test-token" }},
            {{ waitUntil: () => undefined }},
          );
          const item = (await response.json()).results.word;
          console.log(JSON.stringify({{
            status: response.status,
            providers: Object.keys(item.data._providers),
            complete: item.data._complete,
            urls,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertEqual(result["providers"], ["dictionary"])
        self.assertFalse(result["complete"])
        self.assertEqual(len(result["urls"]), 1)
        self.assertIn("dictionaryapi.dev", result["urls"][0])

    def test_frequency_only_skips_dictionary_and_uses_exact(self) -> None:
        script = f"""
          {RATE_LIMITER_JS}
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          const urls = [];
          globalThis.fetch = async (input) => {{
            const url = String(input);
            urls.push(url);
            return Response.json([{{
              word: "word",
              tags: ["f:100"],
            }}]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const response = await worker.fetch(
            new Request(
              "https://dict.example/internal/api/dictionary/batch",
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json",
                  "X-Lexora-Origin-Token": "test-token",
                }},
                body: JSON.stringify({{
                  terms: ["word"],
                  needs: {{ word: {{ frequency: true }} }},
                }}),
              }},
            ),
            {{
              ORIGIN_TOKEN: "test-token",
              DATAMUSE_RATE_LIMITER: quota,
            }},
            {{ waitUntil: () => undefined }},
          );
          const item = (await response.json()).results.word;
          console.log(JSON.stringify({{
            status: response.status,
            providers: Object.keys(item.data._providers),
            complete: item.data._complete,
            urls,
            leaseCosts,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertEqual(result["providers"], ["exact"])
        self.assertTrue(result["complete"])
        self.assertEqual(result["leaseCosts"], [1])
        self.assertEqual(len(result["urls"]), 1)
        self.assertIn("api.datamuse.com", result["urls"][0])

    def test_mixed_selective_batch_preserves_mapping_and_leases_only_exact(
        self,
    ) -> None:
        script = f"""
          {RATE_LIMITER_JS}
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          const urls = [];
          globalThis.fetch = async (input) => {{
            const url = new URL(String(input));
            urls.push(url.toString());
            if (url.hostname === "api.dictionaryapi.dev") {{
              return Response.json([{{
                word: "definition",
                meanings: [{{
                  partOfSpeech: "noun",
                  definitions: [{{
                    definition: "An explanation of meaning.",
                  }}],
                }}],
              }}]);
            }}
            return Response.json([{{
              word: "frequency",
              tags: ["f:10"],
            }}]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const response = await worker.fetch(
            new Request(
              "https://dict.example/internal/api/dictionary/batch",
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json",
                  "X-Lexora-Origin-Token": "test-token",
                }},
                body: JSON.stringify({{
                  terms: ["definition", "frequency"],
                  needs: {{
                    definition: {{ definition: true }},
                    frequency: {{ frequency: true }},
                  }},
                }}),
              }},
            ),
            {{
              ORIGIN_TOKEN: "test-token",
              DATAMUSE_RATE_LIMITER: quota,
            }},
            {{ waitUntil: () => undefined }},
          );
          const body = await response.json();
          console.log(JSON.stringify({{
            status: response.status,
            keys: Object.keys(body.results),
            definitionProviders: Object.keys(
              body.results.definition.data._providers,
            ),
            frequencyProviders: Object.keys(
              body.results.frequency.data._providers,
            ),
            definitionComplete:
              body.results.definition.data._complete,
            frequencyComplete:
              body.results.frequency.data._complete,
            dictionaryRequests: urls.filter(
              (url) => url.includes("dictionaryapi.dev"),
            ).length,
            datamuseRequests: urls.filter(
              (url) => url.includes("api.datamuse.com"),
            ).length,
            leaseCosts,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertEqual(result["keys"], ["definition", "frequency"])
        self.assertEqual(result["definitionProviders"], ["dictionary"])
        self.assertEqual(result["frequencyProviders"], ["exact"])
        self.assertTrue(result["definitionComplete"])
        self.assertTrue(result["frequencyComplete"])
        self.assertEqual(result["dictionaryRequests"], 1)
        self.assertEqual(result["datamuseRequests"], 1)
        self.assertEqual(result["leaseCosts"], [1])

    def test_selective_deep_pass_keeps_all_relationship_providers(
        self,
    ) -> None:
        script = f"""
          {RATE_LIMITER_JS}
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          const urls = [];
          globalThis.fetch = async (input) => {{
            urls.push(String(input));
            return Response.json([]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const response = await worker.fetch(
            new Request(
              "https://dict.example/internal/api/dictionary/batch",
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json",
                  "X-Lexora-Origin-Token": "test-token",
                }},
                body: JSON.stringify({{
                  profile: "deep",
                  terms: ["word"],
                  needs: {{ word: {{ deep: true }} }},
                }}),
              }},
            ),
            {{
              ORIGIN_TOKEN: "test-token",
              DATAMUSE_RATE_LIMITER: quota,
            }},
            {{ waitUntil: () => undefined }},
          );
          const item = (await response.json()).results.word;
          console.log(JSON.stringify({{
            status: response.status,
            providers: Object.keys(item.data._providers),
            complete: item.data._complete,
            datamuseRequests: urls.filter(
              (url) => url.includes("api.datamuse.com"),
            ).length,
            leaseCosts,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertEqual(
            result["providers"],
            ["dictionary", "related", "synonyms", "antonyms"],
        )
        self.assertTrue(result["complete"])
        self.assertEqual(result["datamuseRequests"], 3)
        self.assertEqual(result["leaseCosts"], [3])

    def test_granular_deep_needs_call_only_the_required_provider(
        self,
    ) -> None:
        script = f"""
          {RATE_LIMITER_JS}
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          const urls = [];
          globalThis.fetch = async (input) => {{
            urls.push(String(input));
            return Response.json([]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const lookup = async (term, field) => {{
            const start = urls.length;
            const response = await worker.fetch(
              new Request(
                "https://dict.example/internal/api/dictionary/batch",
                {{
                  method: "POST",
                  headers: {{
                    "Content-Type": "application/json",
                    "X-Lexora-Origin-Token": "test-token",
                  }},
                  body: JSON.stringify({{
                    profile: "deep",
                    terms: [term],
                    needs: {{ [term]: {{ [field]: true }} }},
                  }}),
                }},
              ),
              {{
                ORIGIN_TOKEN: "test-token",
                DATAMUSE_RATE_LIMITER: quota,
              }},
              {{ waitUntil: () => undefined }},
            );
            const item = (await response.json()).results[term];
            return {{
              status: item.status,
              providers: Object.keys(item.data._providers),
              complete: item.data._complete,
              urls: urls.slice(start),
            }};
          }};
          const related = await lookup("related-only", "related");
          const synonyms = await lookup("synonyms-only", "synonyms");
          const antonyms = await lookup("antonyms-only", "antonyms");
          console.log(JSON.stringify({{
            related,
            synonyms,
            antonyms,
            leaseCosts,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(
            result["related"]["providers"],
            ["related"],
        )
        self.assertEqual(
            result["synonyms"]["providers"],
            ["dictionary", "synonyms"],
        )
        self.assertEqual(
            result["antonyms"]["providers"],
            ["dictionary", "antonyms"],
        )
        self.assertNotIn(
            "dictionaryapi.dev",
            " ".join(result["related"]["urls"]),
        )
        for name in ("related", "synonyms", "antonyms"):
            self.assertEqual(result[name]["status"], 200)
            self.assertTrue(result[name]["complete"])
        self.assertEqual(result["leaseCosts"], [1, 1, 1])

    def test_dictionary_relationships_skip_matching_datamuse_providers(
        self,
    ) -> None:
        script = f"""
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          const urls = [];
          globalThis.fetch = async (input) => {{
            const url = String(input);
            urls.push(url);
            if (!url.includes("dictionaryapi.dev")) {{
              throw new Error(`unexpected provider: ${{url}}`);
            }}
            return Response.json([{{
              word: "word",
              meanings: [{{
                partOfSpeech: "noun",
                synonyms: ["term"],
                antonyms: ["silence"],
                definitions: [],
              }}],
            }}]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const response = await worker.fetch(
            new Request(
              "https://dict.example/internal/api/dictionary/batch",
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json",
                  "X-Lexora-Origin-Token": "test-token",
                }},
                body: JSON.stringify({{
                  profile: "deep",
                  terms: ["word"],
                  needs: {{
                    word: {{ synonyms: true, antonyms: true }},
                  }},
                }}),
              }},
            ),
            {{ ORIGIN_TOKEN: "test-token" }},
            {{ waitUntil: () => undefined }},
          );
          const item = (await response.json()).results.word;
          console.log(JSON.stringify({{
            status: response.status,
            providers: Object.keys(item.data._providers),
            complete: item.data._complete,
            needs: item.data._needs,
            urls,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertEqual(result["providers"], ["dictionary"])
        self.assertTrue(result["complete"])
        self.assertTrue(result["needs"]["synonyms"])
        self.assertTrue(result["needs"]["antonyms"])
        self.assertEqual(len(result["urls"]), 1)
        self.assertIn("dictionaryapi.dev", result["urls"][0])

    def test_mixed_granular_batch_coordinates_one_quota_lease_without_hanging(
        self,
    ) -> None:
        script = f"""
          {RATE_LIMITER_JS}
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          const urls = [];
          globalThis.fetch = async (input) => {{
            const url = String(input);
            urls.push(url);
            if (url.includes("dictionaryapi.dev") &&
                url.endsWith("/local")) {{
              return Response.json([{{
                word: "local",
                meanings: [{{
                  synonyms: ["nearby"],
                  antonyms: ["remote"],
                  definitions: [],
                }}],
              }}]);
            }}
            return Response.json([]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const work = worker.fetch(
            new Request(
              "https://dict.example/internal/api/dictionary/batch",
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json",
                  "X-Lexora-Origin-Token": "test-token",
                }},
                body: JSON.stringify({{
                  profile: "deep",
                  terms: ["local", "remote", "related"],
                  needs: {{
                    local: {{ synonyms: true, antonyms: true }},
                    remote: {{ synonyms: true }},
                    related: {{ related: true }},
                  }},
                }}),
              }},
            ),
            {{
              ORIGIN_TOKEN: "test-token",
              DATAMUSE_RATE_LIMITER: quota,
            }},
            {{ waitUntil: () => undefined }},
          );
          const response = await Promise.race([
            work,
            new Promise((_, reject) =>
              setTimeout(
                () => reject(new Error("coordinated lease deadlocked")),
                2000,
              ),
            ),
          ]);
          const body = await response.json();
          console.log(JSON.stringify({{
            status: response.status,
            resultKeys: Object.keys(body.results),
            completes: Object.fromEntries(
              Object.entries(body.results).map(
                ([term, item]) => [term, item.data._complete],
              ),
            ),
            dictionaryRequests: urls.filter(
              (url) => url.includes("dictionaryapi.dev"),
            ).length,
            datamuseRequests: urls.filter(
              (url) => url.includes("api.datamuse.com"),
            ).length,
            leaseCosts,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertEqual(
            result["resultKeys"],
            ["local", "remote", "related"],
        )
        self.assertEqual(
            result["completes"],
            {"local": True, "remote": True, "related": True},
        )
        self.assertEqual(result["dictionaryRequests"], 2)
        self.assertEqual(result["datamuseRequests"], 2)
        self.assertEqual(result["leaseCosts"], [2])

    def test_batch_accepts_dataset_dots_and_120_character_terms(self) -> None:
        script = f"""
          {RATE_LIMITER_JS}
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          globalThis.fetch = async () => Response.json([]);
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const longValid = "a".repeat(120);
          const tooLong = "a".repeat(121);
          const request = new Request(
            "https://dict.example/internal/api/dictionary/batch",
            {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json",
                "X-Lexora-Origin-Token": "test-token",
              }},
              body: JSON.stringify({{
                terms: ["e.g.", longValid, tooLong, "bad/term"],
              }}),
            }},
          );
          const response = await worker.fetch(
            request,
            {{
              ORIGIN_TOKEN: "test-token",
              DATAMUSE_RATE_LIMITER: quota,
            }},
            {{ waitUntil: () => undefined }},
          );
          const body = await response.json();
          console.log(JSON.stringify({{
            status: response.status,
            keys: Object.keys(body.results || {{}}),
            leaseCosts,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertEqual(result["keys"], ["e.g.", "a" * 120])
        self.assertEqual(result["leaseCosts"], [2])

    def test_404_and_empty_results_are_successful_provider_responses(self) -> None:
        script = f"""
          {RATE_LIMITER_JS}
          let matchedCacheUrl = "";
          let putCacheUrl = "";
          let putCacheControl = "";
          globalThis.caches = {{
            default: {{
              match: async (request) => {{
                matchedCacheUrl = request.url;
                return null;
              }},
              put: async (request, response) => {{
                putCacheUrl = request.url;
                putCacheControl = response.headers.get("Cache-Control");
              }},
            }},
          }};
          globalThis.fetch = async (input) => {{
            const url = String(input);
            if (url.includes("dictionaryapi.dev")) {{
              return Response.json(
                {{ title: "No Definitions Found" }},
                {{ status: 404 }},
              );
            }}
            return Response.json([]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const waiters = [];
          const request = new Request(
            "https://dict.example/internal/api/dictionary/batch",
            {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json",
                "X-Lexora-Origin-Token": "test-token",
              }},
              body: JSON.stringify({{
                profile: "deep",
                terms: ["unfindable"],
              }}),
            }},
          );
          const response = await worker.fetch(
            request,
            {{
              ORIGIN_TOKEN: "test-token",
              DATAMUSE_RATE_LIMITER: quota,
            }},
            {{ waitUntil: (promise) => waiters.push(promise) }},
          );
          await Promise.all(waiters);
          const body = await response.json();
          const item = body.results.unfindable;
          console.log(JSON.stringify({{
            status: response.status,
            found: item.data._found,
            providers: item.data._providers,
            matchedCacheUrl,
            putCacheUrl,
            putCacheControl,
            leaseCosts,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertFalse(result["found"])
        self.assertEqual(
            result["providers"]["dictionary"],
            {"ok": True, "status": 404, "found": False},
        )
        for provider in ("related", "exact", "synonyms", "antonyms"):
            self.assertEqual(
                result["providers"][provider],
                {"ok": True, "status": 200, "found": False},
            )
        self.assertIn(
            "/v6/deep/111111111111/unfindable",
            result["matchedCacheUrl"],
        )
        self.assertEqual(result["putCacheUrl"], result["matchedCacheUrl"])
        self.assertEqual(result["putCacheControl"], "public, max-age=604800")
        self.assertEqual(result["leaseCosts"], [4])

    def test_partial_provider_failure_is_exposed_and_cached_for_one_hour(self) -> None:
        script = f"""
          {RATE_LIMITER_JS}
          let putCacheControl = "";
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async (_request, response) => {{
                putCacheControl = response.headers.get("Cache-Control");
              }},
            }},
          }};
          globalThis.fetch = async (input) => {{
            const url = new URL(String(input));
            if (url.hostname === "api.dictionaryapi.dev") {{
              return Response.json([{{ word: "word" }}]);
            }}
            if (url.searchParams.has("ml")) {{
              throw new TypeError("network unavailable");
            }}
            return Response.json([]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const waiters = [];
          const request = new Request(
            "https://dict.example/internal/api/dictionary/batch",
            {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json",
                "X-Lexora-Origin-Token": "test-token",
              }},
              body: JSON.stringify({{
                profile: "deep",
                terms: ["word"],
              }}),
            }},
          );
          const response = await worker.fetch(
            request,
            {{
              ORIGIN_TOKEN: "test-token",
              DATAMUSE_RATE_LIMITER: quota,
            }},
            {{ waitUntil: (promise) => waiters.push(promise) }},
          );
          await Promise.all(waiters);
          const body = await response.json();
          const item = body.results.word;
          console.log(JSON.stringify({{
            status: response.status,
            found: item.data._found,
            complete: item.data._complete,
            providers: item.data._providers,
            putCacheControl,
            leaseCosts,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertTrue(result["found"])
        self.assertFalse(result["complete"])
        self.assertEqual(
            result["providers"]["dictionary"],
            {"ok": True, "status": 200, "found": True},
        )
        self.assertEqual(
            result["providers"]["related"],
            {"ok": False, "status": None, "found": False},
        )
        self.assertEqual(result["putCacheControl"], "public, max-age=3600")
        self.assertEqual(result["leaseCosts"], [4])

    def test_all_provider_failures_return_uncached_gateway_timeout(self) -> None:
        script = f"""
          {RATE_LIMITER_JS}
          let cachePutCount = 0;
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => {{
                cachePutCount += 1;
              }},
            }},
          }};
          globalThis.fetch = async () => {{
            throw new TypeError("network unavailable");
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const request = new Request(
            "https://dict.example/internal/api/dictionary/batch",
            {{
              method: "POST",
              headers: {{
                "Content-Type": "application/json",
                "X-Lexora-Origin-Token": "test-token",
              }},
              body: JSON.stringify({{
                profile: "deep",
                terms: ["word"],
              }}),
            }},
          );
          const response = await worker.fetch(
            request,
            {{
              ORIGIN_TOKEN: "test-token",
              DATAMUSE_RATE_LIMITER: quota,
            }},
            {{ waitUntil: () => undefined }},
          );
          const body = await response.json();
          const item = body.results.word;
          console.log(JSON.stringify({{
            outerStatus: response.status,
            itemStatus: item.status,
            cacheControl: item.data && item.data.error
              ? "no-store"
              : response.headers.get("Cache-Control"),
            error: item.data.error,
            cachePutCount,
            leaseCosts,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["outerStatus"], 200)
        self.assertEqual(result["itemStatus"], 504)
        self.assertEqual(
            result["error"],
            "Dictionary providers are temporarily unavailable",
        )
        self.assertEqual(result["cachePutCount"], 0)
        self.assertEqual(result["leaseCosts"], [4])

    def test_cache_hits_do_not_lease_quota_or_fetch_providers(self) -> None:
        script = f"""
          let cacheMatchCount = 0;
          let providerFetchCount = 0;
          globalThis.caches = {{
            default: {{
              match: async () => {{
                cacheMatchCount += 1;
                return Response.json({{
                  exact: [],
                  _providers: {{
                    dictionary: {{ ok: true, status: 200, found: false }},
                    exact: {{ ok: true, status: 200, found: false }},
                  }},
                  _found: false,
                  _profile: "core",
                }});
              }},
              put: async () => undefined,
            }},
          }};
          globalThis.fetch = async () => {{
            providerFetchCount += 1;
            throw new Error("provider fetch must not run");
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const response = await worker.fetch(
            new Request(
              "https://dict.example/internal/api/dictionary/batch",
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json",
                  "X-Lexora-Origin-Token": "test-token",
                }},
                body: JSON.stringify({{
                  profile: "core",
                  terms: ["word"],
                }}),
              }},
            ),
            {{ ORIGIN_TOKEN: "test-token" }},
            {{ waitUntil: () => undefined }},
          );
          const body = await response.json();
          console.log(JSON.stringify({{
            status: response.status,
            itemStatus: body.results.word.status,
            cacheMatchCount,
            providerFetchCount,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertEqual(result["itemStatus"], 200)
        self.assertEqual(result["cacheMatchCount"], 1)
        self.assertEqual(result["providerFetchCount"], 0)

    def test_cache_key_separates_requested_field_capabilities(self) -> None:
        script = f"""
          const matchedUrls = [];
          globalThis.caches = {{
            default: {{
              match: async (request) => {{
                matchedUrls.push(request.url);
                return Response.json({{
                  _providers: {{}},
                  _found: false,
                  _complete: true,
                  _profile: "core",
                }});
              }},
              put: async () => undefined,
            }},
          }};
          globalThis.fetch = async () => {{
            throw new Error("cache hits must not fetch");
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const lookup = async (needs) => {{
            const response = await worker.fetch(
              new Request(
                "https://dict.example/internal/api/dictionary/batch",
                {{
                  method: "POST",
                  headers: {{
                    "Content-Type": "application/json",
                    "X-Lexora-Origin-Token": "test-token",
                  }},
                  body: JSON.stringify({{
                    terms: ["word"],
                    needs: {{ word: needs }},
                  }}),
                }},
              ),
              {{ ORIGIN_TOKEN: "test-token" }},
              {{ waitUntil: () => undefined }},
            );
            return response.status;
          }};
          const statuses = [
            await lookup({{ definition: true }}),
            await lookup({{ frequency: true }}),
          ];
          console.log(JSON.stringify({{ statuses, matchedUrls }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["statuses"], [200, 200])
        self.assertIn(
            "/v6/core/100000000000/word",
            result["matchedUrls"][0],
        )
        self.assertIn(
            "/v6/core/000010000000/word",
            result["matchedUrls"][1],
        )
        self.assertNotEqual(
            result["matchedUrls"][0],
            result["matchedUrls"][1],
        )

    def test_cache_key_isolates_each_granular_deep_capability(self) -> None:
        script = f"""
          const matchedUrls = [];
          globalThis.caches = {{
            default: {{
              match: async (request) => {{
                matchedUrls.push(request.url);
                return Response.json({{
                  _providers: {{}},
                  _found: false,
                  _complete: true,
                  _profile: "deep",
                }});
              }},
              put: async () => undefined,
            }},
          }};
          globalThis.fetch = async () => {{
            throw new Error("cache hits must not fetch");
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const lookup = async (needs) => {{
            const response = await worker.fetch(
              new Request(
                "https://dict.example/internal/api/dictionary/batch",
                {{
                  method: "POST",
                  headers: {{
                    "Content-Type": "application/json",
                    "X-Lexora-Origin-Token": "test-token",
                  }},
                  body: JSON.stringify({{
                    profile: "deep",
                    terms: ["word"],
                    needs: {{ word: needs }},
                  }}),
                }},
              ),
              {{ ORIGIN_TOKEN: "test-token" }},
              {{ waitUntil: () => undefined }},
            );
            return response.status;
          }};
          const statuses = [
            await lookup({{ synonyms: true }}),
            await lookup({{ antonyms: true }}),
            await lookup({{ phrases: true }}),
            await lookup({{ related: true }}),
          ];
          console.log(JSON.stringify({{ statuses, matchedUrls }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["statuses"], [200, 200, 200, 200])
        self.assertEqual(len(set(result["matchedUrls"])), 4)
        for url in result["matchedUrls"]:
            self.assertIn("/v6/deep/", url)
            signature = url.split("/v6/deep/", 1)[1].split("/", 1)[0]
            self.assertEqual(len(signature), 12)

    def test_mixed_batch_leases_only_uncached_datamuse_requests(self) -> None:
        script = f"""
          {RATE_LIMITER_JS}
          globalThis.caches = {{
            default: {{
              match: async (request) =>
                request.url.endsWith("/cached")
                  ? Response.json({{
                      exact: [],
                      _providers: {{}},
                      _found: false,
                      _profile: "deep",
                    }})
                  : null,
              put: async () => undefined,
            }},
          }};
          const urls = [];
          globalThis.fetch = async (input) => {{
            urls.push(String(input));
            return Response.json([]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const response = await worker.fetch(
            new Request(
              "https://dict.example/internal/api/dictionary/batch",
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json",
                  "X-Lexora-Origin-Token": "test-token",
                }},
                body: JSON.stringify({{
                  profile: "deep",
                  terms: ["cached", "missing"],
                }}),
              }},
            ),
            {{
              ORIGIN_TOKEN: "test-token",
              DATAMUSE_RATE_LIMITER: quota,
            }},
            {{ waitUntil: () => undefined }},
          );
          console.log(JSON.stringify({{
            status: response.status,
            leaseCosts,
            datamuseRequests: urls.filter(
              (url) => url.includes("api.datamuse.com"),
            ).length,
            dictionaryRequests: urls.filter(
              (url) => url.includes("dictionaryapi.dev"),
            ).length,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertEqual(result["leaseCosts"], [4])
        self.assertEqual(result["datamuseRequests"], 4)
        self.assertEqual(result["dictionaryRequests"], 1)

    def test_quota_denial_rejects_datamuse_after_dictionary_first(self) -> None:
        script = f"""
          const leaseCosts = [];
          const quota = {{
            idFromName: (name) => name,
            get: () => ({{
              fetch: async (_url, init) => {{
                leaseCosts.push(JSON.parse(init.body).cost);
                return Response.json(
                  {{ granted: false }},
                  {{ status: 429, headers: {{ "Retry-After": "27" }} }},
                );
              }},
            }}),
          }};
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          let providerFetchCount = 0;
          globalThis.fetch = async () => {{
            providerFetchCount += 1;
            return Response.json([]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const response = await worker.fetch(
            new Request(
              "https://dict.example/internal/api/dictionary/batch",
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json",
                  "X-Lexora-Origin-Token": "test-token",
                }},
                body: JSON.stringify({{
                  profile: "deep",
                  terms: ["word", "phrase"],
                }}),
              }},
            ),
            {{
              ORIGIN_TOKEN: "test-token",
              DATAMUSE_RATE_LIMITER: quota,
            }},
            {{ waitUntil: () => undefined }},
          );
          const body = await response.json();
          console.log(JSON.stringify({{
            status: response.status,
            retryAfter: response.headers.get("Retry-After"),
            bodyRetryAfter: body.retryAfter,
            leaseCosts,
            providerFetchCount,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 429)
        self.assertEqual(result["retryAfter"], "27")
        self.assertEqual(result["bodyRetryAfter"], 27)
        self.assertEqual(result["leaseCosts"], [8])
        # DictionaryAPI is intentionally consulted before deciding whether
        # Datamuse exact is needed. The denied lease still prevents all eight
        # planned Datamuse requests.
        self.assertEqual(result["providerFetchCount"], 2)

    def test_limiter_failure_fails_closed_after_dictionary_first(self) -> None:
        script = f"""
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          let providerFetchCount = 0;
          globalThis.fetch = async () => {{
            providerFetchCount += 1;
            return Response.json([]);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const response = await worker.fetch(
            new Request(
              "https://dict.example/internal/api/dictionary/batch",
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json",
                  "X-Lexora-Origin-Token": "test-token",
                }},
                body: JSON.stringify({{
                  profile: "core",
                  terms: ["word"],
                }}),
              }},
            ),
            {{ ORIGIN_TOKEN: "test-token" }},
            {{ waitUntil: () => undefined }},
          );
          console.log(JSON.stringify({{
            status: response.status,
            retryAfter: response.headers.get("Retry-After"),
            providerFetchCount,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 429)
        self.assertEqual(result["retryAfter"], "60")
        self.assertEqual(result["providerFetchCount"], 1)

    def test_durable_object_enforces_burst_refill_and_daily_cap(self) -> None:
        script = f"""
          let now = Date.UTC(2026, 6, 29, 12, 0, 0);
          Date.now = () => now;
          const database = {{ row: null }};
          const sql = {{
            exec: (query, ...bindings) => {{
              const normalized = query.replace(/\\s+/g, " ").trim();
              if (normalized.startsWith("CREATE TABLE")) return [];
              if (normalized.startsWith("INSERT OR IGNORE")) {{
                if (!database.row) {{
                  database.row = {{
                    utc_day: bindings[0],
                    used: 0,
                    tokens: bindings[1],
                    last_refill_ms: bindings[2],
                  }};
                }}
                return [];
              }}
              if (normalized.startsWith("SELECT utc_day")) {{
                return database.row ? [{{ ...database.row }}] : [];
              }}
              if (normalized.startsWith("UPDATE datamuse_quota")) {{
                database.row = {{
                  utc_day: bindings[0],
                  used: bindings[1],
                  tokens: bindings[2],
                  last_refill_ms: bindings[3],
                }};
                return [];
              }}
              throw new Error(`unexpected SQL: ${{normalized}}`);
            }},
          }};
          let ready = Promise.resolve();
          const state = {{
            storage: {{ sql }},
            blockConcurrencyWhile: (callback) => {{
              ready = callback();
              return ready;
            }},
          }};
          const {{ DatamuseRateLimiter }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const limiter = new DatamuseRateLimiter(state);
          await ready;
          const lease = async (cost) => {{
            const response = await limiter.fetch(
              new Request(
                "https://datamuse-rate-limiter.invalid/lease",
                {{
                  method: "POST",
                  headers: {{ "Content-Type": "application/json" }},
                  body: JSON.stringify({{ cost }}),
                }},
              ),
            );
            return {{
              status: response.status,
              retryAfter: response.headers.get("Retry-After"),
              body: await response.json(),
            }};
          }};

          const burst = await lease(32);
          const blocked = await lease(1);
          now += 1000;
          const refilled = await lease(1);
          database.row.used = 89999;
          database.row.tokens = 32;
          database.row.last_refill_ms = now;
          const dailyBlocked = await lease(2);
          console.log(JSON.stringify({{
            burst,
            blocked,
            refilled,
            dailyBlocked,
            row: database.row,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["burst"]["status"], 200)
        self.assertEqual(result["burst"]["body"]["leased"], 32)
        self.assertEqual(result["blocked"]["status"], 429)
        self.assertEqual(result["blocked"]["retryAfter"], "1")
        self.assertEqual(result["refilled"]["status"], 200)
        self.assertEqual(result["dailyBlocked"]["status"], 429)
        self.assertEqual(result["dailyBlocked"]["body"]["dailyRemaining"], 1)
        self.assertGreater(int(result["dailyBlocked"]["retryAfter"]), 0)
        self.assertEqual(result["row"]["used"], 89999)

    def test_translation_batch_keeps_each_google_result_mapped_to_its_text(
        self,
    ) -> None:
        script = f"""
          const requestedTexts = [];
          let fallbackRequests = 0;
          globalThis.caches = {{
            default: {{
              match: async () => null,
              put: async () => undefined,
            }},
          }};
          globalThis.fetch = async (input) => {{
            const url = new URL(String(input));
            if (url.hostname === "translate.googleapis.com") {{
              const text = url.searchParams.get("q");
              requestedTexts.push(text);
              return Response.json([[[`中:${{text}}`]]]);
            }}
            if (url.hostname === "api.mymemory.translated.net") {{
              fallbackRequests += 1;
              throw new Error("fallback must not be needed");
            }}
            throw new Error(`unexpected URL: ${{url}}`);
          }};
          const {{ default: worker }} = await import(
            {json.dumps(WORKER.as_uri())}
          );
          const texts = [
            "The first definition.",
            "A second, different meaning.",
            "An abbreviation such as e.g.",
          ];
          const response = await worker.fetch(
            new Request(
              "https://dict.example/internal/api/translate/batch",
              {{
                method: "POST",
                headers: {{
                  "Content-Type": "application/json",
                  "X-Lexora-Origin-Token": "test-token",
                }},
                body: JSON.stringify({{ texts }}),
              }},
            ),
            {{ ORIGIN_TOKEN: "test-token" }},
            {{ waitUntil: () => undefined }},
          );
          const body = await response.json();
          console.log(JSON.stringify({{
            status: response.status,
            requestedTexts,
            translations: body.translations,
            fallbackRequests,
          }}));
        """
        result = self.run_node(script)
        self.assertEqual(result["status"], 200)
        self.assertCountEqual(
            result["requestedTexts"],
            [
                "The first definition.",
                "A second, different meaning.",
                "An abbreviation such as e.g.",
            ],
        )
        self.assertEqual(
            result["translations"],
            [
                "中:The first definition.",
                "中:A second, different meaning.",
                "中:An abbreviation such as e.g.",
            ],
        )
        self.assertEqual(result["fallbackRequests"], 0)

    def test_wrangler_uses_sqlite_durable_object_migration(self) -> None:
        config = json.loads(WRANGLER.read_text(encoding="utf-8"))
        self.assertIn(
            {
                "name": "DATAMUSE_RATE_LIMITER",
                "class_name": "DatamuseRateLimiter",
            },
            config["durable_objects"]["bindings"],
        )
        self.assertIn(
            "DatamuseRateLimiter",
            config["migrations"][0]["new_sqlite_classes"],
        )


if __name__ == "__main__":
    unittest.main()
