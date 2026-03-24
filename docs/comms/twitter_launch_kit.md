# Ayder Twitter/X Launch Kit

Use this package for the Jepsen-backed update. Keep the tone factual, specific, and easy to verify.

## Core Positioning

- Ayder published a strict Jepsen-backed correctness result for the broker-log workload.
- The public claim in the repo is: `45/45 pass` in the latest full `hn-ready` matrix.
- Verified run date: `2026-03-13`.
- Matrix directory: `tests/jepsen/results/gold_20260313T103615Z`
- Matrix summary: `tests/jepsen/results/gold_20260313T103615Z/cells.csv`
- Proof bundles live under `tests/jepsen/artifacts/`

Use this language:

- "validated in Jepsen"
- "strict claim"
- "published artifacts"
- "reproducible command and result bundle"

Avoid this language:

- "guaranteed"
- "unbreakable"
- "industrial-strength" unless you back it with a concrete measurement
- "solved consistency" or any absolute claim broader than the repo evidence

## Launch Tweet Options

### Short Version

Ayder now has a strict Jepsen-backed broker-log claim: 45/45 pass in the latest full `hn-ready` matrix, verified on 2026-03-13.

Repo: <REPO_LINK>
Artifacts: <ARTIFACT_LINK>

### Long Version

Ayder now has a strict Jepsen-backed broker-log result.

What is published:
- Verified matrix date: 2026-03-13
- Result: 45/45 pass in the latest full `hn-ready` matrix
- Matrix directory: `tests/jepsen/results/gold_20260313T103615Z`
- Summary: `tests/jepsen/results/gold_20260313T103615Z/cells.csv`
- Proof bundles and SHA256 files: `tests/jepsen/artifacts/`

What this means:
- The claim is narrow and specific.
- The test setup, command flow, and artifacts are in the repo.
- Anyone can inspect the evidence and rerun the campaign from the published instructions.

Repo: <REPO_LINK>
Release artifacts: <ARTIFACT_LINK>

## 5-Post Thread

### Post 1

Ayder has a strict Jepsen-backed update to share.

The public claim is narrow on purpose: broker-log, latest full `hn-ready` matrix, 45/45 pass.

### Post 2

Verified run date: 2026-03-13.

Published evidence:
- matrix dir: `tests/jepsen/results/gold_20260313T103615Z`
- summary: `tests/jepsen/results/gold_20260313T103615Z/cells.csv`
- proof bundles: `tests/jepsen/artifacts/`

### Post 3

We are not asking you to take the claim on faith.

The repo includes the Jepsen suite, the strict run command, and the artifact layout used to produce the result.

### Post 4

If you want to review the evidence:
- read the repo README for the strict claim and command flow
- inspect the Jepsen suite docs
- download the release artifact bundle and SHA256

### Post 5

Links:
- Repo: <REPO_LINK>
- Release artifacts: <ARTIFACT_LINK>

If you are skeptical, that is the right default. Check the command, the matrix, and the published bundle hash.

## 10 Reply Templates

Use these as direct replies. Keep them short. Do not defend beyond the evidence.

### 1. "What exactly is the claim?"

The claim is narrow: the broker-log workload passed the latest full `hn-ready` Jepsen matrix, 45/45, verified on 2026-03-13. The repo links the matrix directory and artifact bundles.

### 2. "Does this mean Ayder is bug-free?"

No. It means a specific broker-log claim was validated under the published Jepsen setup. It does not prove absence of all bugs or all failure modes.

### 3. "Where is the proof?"

Start here:
- `README.md`
- `tests/jepsen/README.md`
- `tests/jepsen/results/gold_20260313T103615Z`
- `tests/jepsen/artifacts/`

### 4. "Can I rerun it?"

Yes. The repo includes the run command and the Jepsen campaign scripts. Use the exact command flow in the README and compare your results to the published matrix.

### 5. "Is this a marketing claim or a lab result?"

It is a lab result first. The wording is intentionally strict and limited to the published Jepsen evidence.

### 6. "Why only broker-log?"

Because the published public claim is for broker-log. We should not generalize the result beyond the workload that was actually validated.

### 7. "What fault model did you test?"

The published matrix covers the strict Jepsen campaign shape used in the repo, including mixed, partition-only, and kill-only modes. The exact commands and config are in the README and test scripts.

### 8. "Do you have hashes?"

Yes. The release bundles include SHA256 files alongside the tarballs in `tests/jepsen/artifacts/`.

### 9. "Why should I trust this?"

You do not need to trust the wording. Inspect the repo, the matrix summary, and the artifacts. The point of the package is to make verification easier than debate.

### 10. "What should I read first?"

Read `README.md` for the strict claim, then `tests/jepsen/README.md` for the test setup, then open the matrix summary and bundle artifacts.

## 7-Day Posting Schedule

### Day 1

- Post the short launch tweet in the morning.
- Reply manually to the first wave of questions within 1-2 hours.
- Pin the launch tweet if this is the primary announcement.

### Day 2

- Post the long version as a follow-up or quote tweet.
- Answer process questions with links to the repo and artifacts.
- Keep replies focused on what was tested, not broad positioning.

### Day 3

- Share the matrix summary path and a short explanation of why the claim is narrow.
- Use one reply template about scope limits.

### Day 4

- Post a thread excerpt or a single evidence-focused post.
- Emphasize reproducibility: command, matrix, bundle hash.

### Day 5

- Reply to questions about fault model and verification.
- Use the "do not generalize beyond broker-log" template when needed.

### Day 6

- Share one artifact-oriented reminder: repo, results directory, bundle SHA256.
- Avoid adding new claims or performance numbers.

### Day 7

- Close the week with a calm recap:
  - what was proven
  - where the evidence lives
  - how to rerun it
- Do not re-litigate the claim. Point back to the artifacts.

## Link and CTA Strategy

Primary CTA:

- Send people to the repo first. That is where the claim, the command, and the scope live.

Secondary CTA:

- Send people to the release artifact bundle for the exact proof package.

Recommended link order:

1. Repo link
2. Release artifact bundle link
3. Direct path references in follow-up replies if asked

Recommended CTA wording:

- "Read the strict claim and command in the repo."
- "Inspect the matrix summary and artifact bundle."
- "Rerun the campaign using the published scripts."

Do not bury the evidence behind a vague product page. The whole point is verification.

## Trust-First Tone

- Lead with scope, date, and result.
- Separate claim from interpretation.
- Use exact paths where possible.
- Invite inspection, not belief.
- Acknowledge limits before someone else has to point them out.

## Final Posting Rule

If a post cannot be defended with a repo path, artifact path, or an explicit scope statement, do not publish it.
