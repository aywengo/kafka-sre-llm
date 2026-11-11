# kafka-sre v1 leftovers

Run: 2026-11-10 to 2026-11-11. Base `runs/v1/base27b`, adapter `adapters/kafka-sre-v1-resume`, greedy answers. Details: `runs/v1/HEARTBEAT.md` (Phases L-A..L-E), tables `runs/v1/leftovers_tables.json`.

## 1. Fair baseline (2048 tokens) vs v1 (1024) — mean score, base / tuned (delta)
| Set | v1, 1024, DeepSeek | 2048, DeepSeek | 2048, GLM-5.3-Flash |
|---|---|---|---|
| questions | 3.75 / 3.25 (-0.50) | 3.83 / 3.25 (-0.58) | 3.17 / 3.75 (+0.58) n=12 |
| questions_real | 3.15 / 3.05 (-0.10) | 3.12 / 3.05 (-0.08) | 2.97 / 3.15 (+0.18) n=39 |
| questions_synthetic | 2.27 / 2.43 (+0.16) | 2.30 / 2.41 (+0.11) | 2.21 / 2.86 (+0.65) n=97 |

**Gate (tuned >= base + 0.3 on questions_real): FAILS** (DeepSeek -0.08; GLM +0.18 on 39 of 40). The name stays "preview" (Roman's decision). Real set by category (DeepSeek, 2048): strimzi +1.00, kafka-config +0.67, argo-cd +0.60, schema-registry +0.50, kubernetes -0.20, kafka-ops -1.60, streaming -2.00.

## 2. Truncation and length (2048 limit; v1 at 1024 in brackets)
| Set | Base truncated | Tuned truncated | Mean tokens base / tuned |
|---|---|---|---|
| questions | 2/12 (11) | 0/12 (0) | 1752 / 252 |
| questions_real | 2/40 (33) | 0/40 (0) | 1407 / 218 |
| questions_synthetic | 4/100 (78) | 3/100 (3) | 1331 / 281 |
| All | 8/152 (122) | 3/152 (3) | 1385 / 262 |

## 3. Judges vs blind review (10 real questions, 1024-token answers; verdict = better answer)
| # | Question | Roman | 2nd reviewer | DeepSeek | GLM |
|---|---|---|---|---|---|
| 1 | After deploying a new version of our consumer se… | base | base | base | base |
| 2 | Producers intermittently fail with NotEnoughRepl… | tuned | base | base | tie |
| 3 | A partition is offline because its only in-sync … | tuned | base | base | base |
| 4 | What is the difference between log.retention.hou… | tuned | tuned | tuned | tuned |
| 5 | We need to send 5 MB messages. Which settings ha… | tuned | base | base | tuned |
| 6 | Create a KafkaUser that uses SCRAM-SHA-512 and c… | tuned | tuned | tuned | tuned |
| 7 | We added two brokers. How do we move existing pa… | tuned | tuned | tuned | tuned |
| 8 | During node maintenance two Kafka brokers went d… | tuned | tuned | base | tuned |
| 9 | Argo CD wants to prune KafkaTopics that the appl… | tuned | tuned | tuned | tuned |
| 10 | Consumers fail with 'Unknown magic byte!' after … | tie | tie | tie | tie |

Tuned/base/tie: Roman 8/1/1, 2nd reviewer 5/4/1, DeepSeek 4/5/1, GLM 6/2/2. Agreement with Roman: GLM 8/10, DeepSeek 6/10. With the 2nd reviewer: GLM 8/10, DeepSeek 9/10. (Roman vs 2nd reviewer: 7/10.) Ten questions is a small sample; 6 vs 8 of 10 is not a significant difference.

## 4. DeepSeek vs GLM on all questions
| Answers | Questions scored by both | Pearson | Exact score | Within 1 | Same verdict | Mean base/tuned DeepSeek | Mean base/tuned GLM |
|---|---|---|---|---|---|---|---|
| 1024 | 151 | 0.79 | 52% | 94% | 65% | 2.63 / 2.67 | 2.60 / 2.87 |
| 2048 | 148 | 0.78 | 55% | 93% | 65% | 2.63 / 2.68 | 2.49 / 3.01 |

GLM invalid JSON: 5 of 304 replies (1 at 1024, 4 at 2048); all hit the 32,768-token limit while thinking and returned empty content. DeepSeek: 0 of 152.

## 5. Judge recommendation for v2: GLM-5.3-Flash (oQ4e) as primary, DeepSeek as a cheap second score
- GLM did not write the training data; DeepSeek did (self-preference risk). GLM matched Roman's verdicts better (8 vs 6 of 10) and is the only judge that scores the tuned model above base on the gate set, though still below the gate. DeepSeek matched the 2nd reviewer better (9 vs 8 of 10).
- Cost: thinking cannot be turned off (the chat template hardcodes it; per-model `enable_thinking=false` and request-level options had no effect; setting restored). Mean 166 s and 3,512 completion tokens per item (median 108 s, two passes running in parallel), so about 7.4 h for 304 items. DeepSeek (thinking off) was not timed separately and is much shorter per item. Needs `max_tokens` 32768 in the request.
- Licence: GLM-5.3-Flash is MIT per the orcarouter `GLM-5.3-Flash-MLX` card for `zai-org/GLM-5.3-Flash` (the local oQ4e card has no licence line: verify). DeepSeek-V4.1-Flash is MIT.
- Caveat: 10 questions, one human. Keep both judges and report both; use GLM for the gate.

## 6. Ollama: not supported
Ollama 0.14.2: `ollama create` works, loading fails (`unknown model architecture: 'qwen35'`, HTTP 500 in `ollama run` and the API). No output or speed to report. Release notes list Qwen3.5 in v0.40.0 and Qwen3.8-27B in v0.40.7; this GGUF is untested there (upgrade needs your go). Cards now say "not supported by Ollama 0.14.2; use llama.cpp". Test model removed; GGUFs untouched.

## 7. GGUF perplexity (same 2,665-token text, llama-perplexity -c 512 -ngl 0)
MLX bf16 3.775 (v1) · Q8_0 3.7844 · Q5_K_M 3.8105 (+0.026 vs Q8_0; each +/- 0.22).

## 8. Feedback items (`eval/questions_feedback.jsonl`, 4 so far; no GitHub issues, no HF discussions)
Unclean leader election scope (tuned says broker-only; it is also a topic config) · invented deprecation of `log.retention.ms` and `log.segment.ms` (UNSURE) · consumer rebalance cause (`max.poll.interval.ms` missing) · Cruise Control add-brokers answer skips enabling, review and approval (annotation UNSURE). The MacBook MLX-4bit Cruise Control entry waits for Roman's pasted output; valid modes (all cited at Strimzi 1.3.1): full, add-brokers, remove-brokers, remove-disks.

## 9. Prepared, not published (local commit 04c4847: model card; the three READMEs in out/ are gitignored, changed on disk)
Cards now show the 2048 tables, gate banner (FAILED, new numbers), second judge, blind review with GLM row, Ollama "not supported". GGUF wording is still "Q4_K_M only"; the "uploaded" version is `runs/v1/prepared/apply_uploaded_wording.py`.

## Waiting for Roman's go
1. README update on the 3 HF repos: `runs/v1/prepared/publish_readmes.sh`.
2. Upload Q5_K_M and Q8_0 (no `HF_XET_HIGH_PERFORMANCE`): `runs/v1/prepared/upload_q5_q8.sh` (the proven step d of `upload_steps2.sh`). **Not `scripts/publish.sh`**: it rebuilds everything, defaults to `adapters/kafka-sre-v1` and name `kafka-sre-27b-v1`, and uploads whole folders with `--private`. After the upload run `apply_uploaded_wording.py`, then publish the READMEs again.
3. `git push` of 04c4847 (the only unpushed commit; mercury-nightly has no push permission on the repo).
4. Ask only if wanted: install a newer Ollama (v0.40.x) to retest the GGUF.
5. Also open: rotate the oMLX API key (it appeared in old session logs/ps output) and the mercury-nightly token; oMLX now has GLM-5.3-Flash-oQ4e loaded and DeepSeek unloaded; the MacBook feedback entry.
