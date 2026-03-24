# Ayder Social Post Bank

Reusable copy for the Jepsen-backed launch. This bank is optimized for trust-first replies and low-noise follow-up.

## Launch Post Variants

### Short Launch Post

Ayder has a strict Jepsen-backed broker-log result: 45/45 pass in the latest full `hn-ready` matrix, verified on 2026-03-13.

Repo: <REPO_LINK>
Artifacts: <ARTIFACT_LINK>

### Long Launch Post

Ayder now has a strict Jepsen-backed broker-log result.

Published evidence:
- verified date: 2026-03-13
- matrix dir: `tests/jepsen/results/gold_20260313T103615Z`
- matrix summary: `tests/jepsen/results/gold_20260313T103615Z/cells.csv`
- proof bundles: `tests/jepsen/artifacts/`

The claim is intentionally narrow. It is tied to the published workload, matrix, and artifacts, not a general statement about every possible failure mode.

Repo: <REPO_LINK>
Release artifacts: <ARTIFACT_LINK>

## Thread Bank

### Thread Opener

Ayder has a strict Jepsen-backed update to share.

The claim is broker-log only, and it is backed by a published matrix and artifact bundle.

### Evidence Post

Verified on 2026-03-13:
- `tests/jepsen/results/gold_20260313T103615Z`
- `tests/jepsen/results/gold_20260313T103615Z/cells.csv`
- `tests/jepsen/artifacts/`

### Scope Post

This is a narrow correctness claim, not a claim that every workload or every bug class is solved.

### Verification Post

If you want to inspect it, start with the repo README and the Jepsen suite README. The command flow and bundle layout are both documented.

### CTA Post

Repo first, artifacts second:
- <REPO_LINK>
- <ARTIFACT_LINK>

## 10 Reply Templates

### 1. "Is this hype?"

No. The wording is deliberately narrow and tied to a published Jepsen matrix and artifact bundle.

### 2. "What did you actually prove?"

We published a broker-log Jepsen result with 45/45 pass in the latest full `hn-ready` matrix, verified on 2026-03-13.

### 3. "What should I look at?"

The best starting points are `README.md`, `tests/jepsen/README.md`, the matrix summary, and the artifact bundle.

### 4. "Where is the exact run?"

`tests/jepsen/results/gold_20260313T103615Z`

### 5. "Can I trust this without rerunning it?"

You can inspect the published evidence, but the point of the package is that rerunning it should also be possible from the repo.

### 6. "Why only broker-log?"

Because that is the scope of the published public claim. We should not expand it beyond what was actually tested.

### 7. "Did you test faults?"

Yes. The published matrix covers the strict campaign shape used for the claim, including mixed, partition-only, and kill-only modes.

### 8. "Do you have a checksum?"

Yes. The release bundle includes a SHA256 file in `tests/jepsen/artifacts/`.

### 9. "Why this format?"

Because trust is easier to earn when the claim, the date, the scope, and the proof are all easy to find.

### 10. "Where can I start if I'm skeptical?"

Start with the repo README, then the Jepsen README, then the matrix summary. If the evidence is thin, it should be obvious from those files.

## 7-Day Posting Schedule

### Day 1

- Publish the short launch post.
- Pin it if the launch should be discoverable from the profile.
- Answer only the first wave of obvious scope questions.

### Day 2

- Post the long version with the evidence bullets.
- Use one reply that points directly to the matrix directory.

### Day 3

- Share a thread opener plus the exact matrix summary path.
- Avoid adding new claims or new metrics.

### Day 4

- Post a verification-focused reply: repo, artifacts, rerun path.
- Stay away from comparative claims about other systems.

### Day 5

- Repost the scope statement if the conversation drifts into generality.
- Use the "why only broker-log" and "did you test faults" replies.

### Day 6

- Share the checksum reminder.
- Keep the post plain: no adjectives that are not backed by evidence.

### Day 7

- Close with a calm summary and the two links again.
- Ask readers to inspect the evidence, not to accept the claim on authority.

## Do / Don't

### Do

- Do lead with scope, date, and result.
- Do link to the repo and the artifact bundle.
- Do use exact directory names and bundle names.
- Do acknowledge limits before someone challenges them.
- Do answer "what exactly was tested?" before answering "why should I care?"

### Don't

- Don't say "solved" or "guaranteed".
- Don't generalize broker-log results to every workload.
- Don't hide the matrix behind marketing language.
- Don't use vague phrases like "battle-tested" without a concrete proof path.
- Don't add extra performance claims unless they are already published in the repo.

## Link and CTA Strategy

Recommended structure:

1. Primary link: repo
2. Secondary link: release artifact bundle
3. In replies, use direct file paths when the question is about evidence

Good CTAs:

- "Read the strict claim in the repo."
- "Inspect the matrix summary."
- "Download the artifact bundle and SHA256."
- "Rerun the published campaign."

Bad CTAs:

- "Try Ayder now"
- "See why this changes everything"
- "The future of durable systems"

## Tone Rules

- Keep claims specific.
- Keep verbs modest.
- Keep the proof path visible.
- Keep the reply short when the question is short.
- Keep the scope narrow when the question tries to widen it.

