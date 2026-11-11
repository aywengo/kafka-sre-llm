# kafka-sre v1 report (draft; written up at the end of the run)

## Source refs
- apache-kafka: 4.4.0 @ 1156b2752aaa28eafc0b4c4d387e63356793268d
- strimzi: 1.3.1 @ 555c3e0f07c203911bc3cb7d13ac085bfaa81b50
- argo-cd: v3.6.1 @ 94037c4d4e1af4bd0651d37a179fbd9e347e668c
- kubernetes: default branch @ 4f32cd5cc6a8fd69775fb9afad6113bff374b2f8
- apache-kafka-config: default branch @ e6cb1ef29f17ccb21d6d56efc9331d7098ed3719

Kafka, Strimzi and Argo CD are pinned to release tags (`ref:` in sources.yaml). Kubernetes follows main; apache-kafka-config is kafka-site `content/en/44/generated` (matches Kafka 4.4.0).

## Base model
mlx-community/Qwen3.8-27B-bf16, revision 6f265714824f3c38d4452baa1628aef3d9b9aae9, wrapped by scripts/make_nothink_base.py (runs/v1/base27b, non-thinking chat template). mlx 0.32.4, mlx-lm 0.32.2.

## Notes for v2 (from the spot checks)
- v1 data: 13 OK / 8 weak / 9 wrong of 30. Causes: stale or from-memory API versions (v1beta2), unresolved Strimzi attributes, wrong premise accepted, wrong scope.
- v2 data (after the fixes): 17 OK / 8 weak / 5 wrong of 30. The strict verifier rejected only 1-3% of existing items, so it is a weak filter.
- Remaining generator failure modes in v2: caveats and examples added from memory (items 10, 11, 14: deprecated `blacklist`, wrong validate endpoint, wrong POST behaviour, wrong session-timeout cause); a false premise accepted (item 4: null key vs null value in a KTable join); a scope error (item 23).
- Add to GEN_SYSTEM: caveats, examples, defaults and side effects must also be supported by the excerpt; if unsure, leave them out.
- Extend the premise check: the verifier must restate the premise of the question and check it against the excerpt (key vs value, component, scope) before judging the answer; reject answers whose extra claims (caveats, examples, endpoints, deprecations) are not in the excerpt.
- Consider running the verifier with thinking on for items that contain caveats or examples, and a second independent verifier model.
- Generator inserted kafka.strimzi.io/v1beta2 from memory in 121 items despite the rule (53 from appendix_crds schema chunks); the build filter drops them.
