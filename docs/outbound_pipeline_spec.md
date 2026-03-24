# Founder-Led Outbound Pipeline Spec

This is a spreadsheet-plus-Gmail-only outbound system for founder-led selling. It is designed to be run manually, repeatably, and auditable without a CRM or sales automation platform.

## Operating Model

- Primary seller: founder
- Supporting system: Google Sheets + Gmail
- Unit of work: one lead
- Primary goal: convert targeted outbound into booked first calls
- Secondary goal: create a reliable feedback loop on message-market fit

### Daily Operating Cadence

- 30 minutes: source and qualify new leads
- 30 minutes: enrich and score leads
- 60 minutes: send outbound and follow-ups
- 15 minutes: update spreadsheet and next-action dates
- 15 minutes: review dashboard and correct bottlenecks

## Spreadsheet Structure

Use one Google Sheet with these tabs:

1. `Leads`
2. `Lists`
3. `Templates`
4. `Sequence`
5. `Dashboard`
6. `Source Log`

### `Leads` Tab

One row per lead. Required columns:

- `Lead ID`
- `Company`
- `Contact First Name`
- `Contact Last Name`
- `Title`
- `Email`
- `LinkedIn URL`
- `Company Website`
- `Industry`
- `Segment`
- `Geo`
- `Employee Count`
- `Source`
- `Source Date`
- `ICP Fit` (`A`, `B`, `C`, `D`)
- `Pain Hypothesis`
- `Trigger Event`
- `Personalization Hook`
- `Email Status` (`Not Sent`, `Sent 1`, `Sent 2`, `Sent 3`, `Replied`, `Booked`, `Disqualified`, `Do Not Contact`)
- `Last Sent Date`
- `Next Follow-up Date`
- `Last Touch Type`
- `Reply Type` (`Positive`, `Neutral`, `Negative`, `Auto`, `Unsubscribe`)
- `Meeting Booked Date`
- `Owner`
- `Notes`

### `Lists` Tab

Reference lists for dropdowns:

- `Industry`
- `Segment`
- `Geo`
- `ICP Fit`
- `Source`
- `Reply Type`
- `Email Status`

### `Templates` Tab

Store approved message variants and subject lines:

- `Variant Name`
- `Audience`
- `Opening Line`
- `Value Proposition`
- `CTA`
- `Subject Line A`
- `Subject Line B`

### `Sequence` Tab

Tracks timing and content for the sequence:

- `Step`
- `Delay After Prior Step`
- `Channel`
- `Purpose`
- `Template Name`
- `Stop Condition`

### `Dashboard` Tab

Weekly and monthly summary metrics with thresholds:

- lead volume
- enrichment completion
- outbound send volume
- reply rate
- positive reply rate
- booked meeting rate
- show rate
- conversion by source
- conversion by segment
- conversion by message variant

### `Source Log` Tab

Capture which source produced each lead:

- `Source`
- `Source Type`
- `Notes`
- `Added By`
- `Date Added`
- `Expected Quality`

## Lead Sourcing Process

### Target Lead Definition

Source only leads that match at least 4 of 5:

- clear business pain that can plausibly be solved by the offer
- founder or buyer-level influence
- company is actively growing, hiring, launching, or changing tools/processes
- enough sophistication to pay
- reachable by email

### Source Priorities

Use sources in this order:

1. Recent company changes
2. Relevant job postings
3. Founder posts and podcasts
4. Directory lists and ecosystem lists
5. Competitor/customer lookalike research

### Sourcing Workflow

1. Build a source list for a narrow market slice.
2. Pull 25 to 50 leads per session.
3. Capture only leads with a plausible trigger event.
4. Reject vague fits immediately.
5. Add source date and trigger to the sheet the same day.

### Sourcing Rules

- Do not source without a reason to contact now.
- Do not send generic lists of names from broad databases.
- Do not include leads without an email route unless a direct email can be found in the same session.
- Keep each source batch tied to one segment so the message can be tested cleanly.

## Enrichment Standards

### Minimum Enrichment Before Sending

Every lead must have:

- first and last name
- verified email or high-confidence email
- title
- company
- website
- one trigger event
- one personalization hook
- ICP fit score

### Enrichment Fields

Required:

- `Title`
- `Company Website`
- `Industry`
- `Segment`
- `Geo`
- `Employee Count`
- `Trigger Event`
- `Personalization Hook`
- `Pain Hypothesis`
- `ICP Fit`

Optional if easy to find:

- `LinkedIn URL`
- `Recent post`
- `Hiring signal`
- `Tech stack clue`
- `Funding or expansion note`

### Verification Standard

- Email must be verified by at least one of:
  - direct evidence on company domain
  - a known email pattern plus matching company contact format
  - a confidence check from manual search
- If confidence is low, mark `Email Status = Disqualified`.
- If any lead lacks a trigger event, do not send.

### ICP Fit Scoring

Score A to D:

- `A`: ideal fit, strong pain, strong trigger, reachable buyer
- `B`: good fit, minor uncertainty
- `C`: weak fit, send only if list quality is poor and test volume is needed
- `D`: not a fit, disqualify

Only `A` and `B` leads should be included in the main sequence. `C` leads can be held for later tests. `D` leads are excluded.

## Message Strategy

### Core Messaging Rules

- Lead with the trigger, not the product.
- One message = one problem.
- Keep first email under 120 words.
- One CTA only.
- Use plain language.
- Avoid attachments and long decks.
- If possible, reference the recipient?s current situation in one sentence.

### Message Angles

Use 4 repeatable angles:

1. `Trigger-based`: use a recent event
2. `Pain-based`: describe an operational bottleneck
3. `Outcome-based`: show business result
4. `Peer-based`: mention a similar company pattern

### Subject Line Variants

Use 2 subject lines per angle:

- `Quick question about [trigger]`
- `Idea for [company]`
- `Worth discussing?`
- `Re: [specific initiative]`

### Email Variant 1: Trigger-Based

Use when a lead has a concrete event.

Structure:

1. reference the trigger
2. connect to a likely pain
3. offer a concise hypothesis
4. ask for a short call

Example pattern:

- `Saw you [trigger event].`
- `Teams usually hit [pain] at that stage.`
- `We help with [outcome].`
- `Open to a 15-minute call next week?`

### Email Variant 2: Pain-Based

Use when the trigger is weak but the pain is obvious.

Structure:

1. name the pain
2. state why it matters now
3. explain the result
4. ask for a conversation

### Email Variant 3: Outcome-Based

Use for buyers who respond to business metrics.

Structure:

1. state the metric improved
2. tie it to their likely priority
3. mention proof or mechanism
4. ask if it is relevant

### Email Variant 4: Peer-Based

Use when the recipient is similar to known customers.

Structure:

1. note similarity
2. mention the result a peer got
3. name the category problem
4. ask to compare notes

### Follow-Up Variants

Follow-ups should vary angle, not repeat the same copy.

- Follow-up 1: add a short clarification of the problem
- Follow-up 2: add a relevant proof point
- Follow-up 3: add a break-up style closing line

## Follow-Up Cadence

Use a 6-touch sequence over 18 to 21 days.

### Cadence

1. Day 0: initial email
2. Day 2: short follow-up
3. Day 5: proof or example
4. Day 9: alternate angle
5. Day 14: direct CTA reminder
6. Day 21: close-the-loop note

### Stop Rules

Stop the sequence immediately if:

- they reply positively
- they ask to stop
- they book a meeting
- the email bounces
- the contact is wrong

### Response Handling

- Positive reply: move to `Booked` or `Reply Positive`, send scheduling link manually, and stop sequence.
- Neutral reply: answer the question, keep sequence paused until resolved.
- Negative reply: mark disqualified and stop.
- Auto-reply: do not count as a human reply unless it contains a meaningful response.
- Unsubscribe: mark `Do Not Contact` immediately.

## Gmail Execution Rules

### Sending Method

Use Gmail drafts and manual sends only.

Recommended Gmail labels:

- `Outbound - New`
- `Outbound - Sent`
- `Outbound - Replied`
- `Outbound - Booked`
- `Outbound - Do Not Contact`
- `Outbound - Needs Review`

### Daily Gmail Flow

1. Create drafts from the approved template.
2. Personalize the first line only when possible.
3. Send in small batches.
4. Label sent threads immediately.
5. Move replies into the appropriate label before actioning them.

### Gmail Hygiene

- Use one sender identity per founder.
- Keep domain reputation safe by sending in small batches.
- Avoid large identical sends.
- Do not include multiple recipients in one outbound thread.
- Do not use BCC for cold outbound.

## CRM Fields

This system uses the spreadsheet as the CRM. Minimum fields are below.

### Identity Fields

- `Lead ID`
- `Company`
- `Contact First Name`
- `Contact Last Name`
- `Title`
- `Email`
- `LinkedIn URL`
- `Owner`

### Fit Fields

- `Industry`
- `Segment`
- `Geo`
- `Employee Count`
- `ICP Fit`
- `Pain Hypothesis`
- `Trigger Event`
- `Source`

### Activity Fields

- `Email Status`
- `Last Sent Date`
- `Next Follow-up Date`
- `Last Touch Type`
- `Reply Type`
- `Meeting Booked Date`
- `Notes`

### Outcome Fields

- `Meeting Held`
- `Opportunity Created`
- `Stage`
- `Deal Value`
- `Win/Loss`
- `Loss Reason`

## KPI Dashboard Thresholds

Track weekly and trailing 4-week metrics.

### Input Volume

- New leads added: target 50 to 100 per week
- Leads fully enriched: at least 85 percent
- Leads sent to: at least 70 percent of enriched leads

### Email Performance

- Open rate: not a primary KPI
- Reply rate: 8 to 15 percent
- Positive reply rate: 2 to 5 percent
- Booked meeting rate: 1 to 3 percent
- Bounce rate: under 5 percent
- Unsubscribe rate: under 1 percent

### Pipeline Quality

- A/B lead share: at least 80 percent of sent volume
- Triggered leads share: at least 60 percent of sent volume
- Personalization hook completion: at least 90 percent
- Follow-up completion: at least 95 percent of scheduled touches

### Sales Efficiency

- Time from lead added to first send: under 3 business days
- Time from positive reply to booked call: under 2 business days
- No-response sequence completion rate: at least 90 percent

### Red Flags

Escalate if any of the following happen:

- reply rate below 5 percent for 2 consecutive weeks
- positive reply rate below 1 percent for 2 consecutive weeks
- bounce rate above 8 percent
- unsubscribe rate above 2 percent
- more than 25 percent of leads are `C` or `D`
- follow-up completion below 85 percent

## Weekly Review Process

Every week, review:

1. top-performing source
2. top-performing segment
3. best message variant
4. weakest follow-up step
5. common reply objections
6. leads that should be excluded earlier

### Weekly Actions

- double down on the best source/segment pair
- remove any source with poor reply quality
- rewrite the worst-performing subject line
- tighten the ICP filter if too many poor-fit leads are being sent
- log objections and turn them into future message variants

## Testing Rules

Change only one variable at a time:

- source
- segment
- angle
- subject line
- CTA

Run each test on at least 20 to 30 sends before judging it unless the signal is extreme.

## Minimum Viable Operating Standard

If the team can only do the minimum, do this:

- source 25 qualified leads per day
- enrich only `A` and `B` leads
- send one initial email plus 3 follow-ups
- update the sheet the same day
- review metrics every Friday

## Definition of Done

The outbound pipeline is working when all of the following are true:

- leads are sourced from repeatable channels
- enrichment is standardized
- every send maps to a template variant
- follow-ups are scheduled in advance
- replies are categorized in the sheet
- booked meetings are visible in the dashboard
- weekly metrics drive changes to source and messaging
