---
name: "tailor-resume"
description: "Tailor the candidate's LaTeX resume and Deedy cover letter to a specific job description, and output the LaTeX source for them to compile on Overleaf. Use whenever they paste or link a JD, name a company/role to apply to, or ask to customize, tailor, or rewrite their resume or cover letter."
---

# Tailor a resume and cover letter

Produces the **LaTeX source** for one application: a tailored resume and a tailored cover letter. The candidate compiles them on Overleaf themselves. Never deliver PDFs.

## Setup (private files)

This skill is generic. All personal data lives in three files next to this one, which are **gitignored and must never be committed**:

| File | Contents | Template |
|---|---|---|
| `references/profile.md` | Candidate facts: graduation date, fixed resume structure, project bank, confidentiality rules, spent opening devices | `references/profile.example.md` |
| `references/master-resume.tex` | The master resume, verbatim | `references/master-resume.example.tex` |
| `references/cover-letter.tex` | The Deedy cover letter template with contact header filled in | `references/cover-letter.example.tex` |

Copy each `.example` file to its private name and fill it in. Read all three before starting. If any is missing, stop and tell the candidate which one to create.

## Inputs

Ask for anything missing before starting. Required:

1. **Job description**: pasted text, a file, or a URL (fetch it).
2. **Office address** for the cover letter block (street, city, postal code). If only a city is given, use the company's known office there; if unsure, ask rather than invent an address.

Optional:

3. **Projects document**: a longer doc of project descriptions. Use it for project bullet wording and cover letter material. Draw on personal/motivational passages only where genuinely relevant.

If given a JD and nothing else, ask only for the office address. Do not run a full clarifying round for a task this well specified.

If only the resume is requested (no cover letter), produce and print the resume; skip Step 3, skip the cover-letter half of Steps 4/5, and skip asking for the office address.

## Step 1: Read the JD, pick the angle

Extract, in this order:

- **Role type**: backend / infra-platform / full-stack / ML-AI / embedded-systems / generalist. This decides everything downstream.
- **Named technologies**, verbatim. These become the keyword targets.
- **Company domain** and what breaks if their software is wrong (payments, satellites, GPUs, health, consumer). The cover letter opens on this.
- **Seniority framing**: intern vs new grad, based on the graduation date in `profile.md`. Never write a letter that misstates where the candidate is in their degree.

State the angle in one sentence before editing anything.

## Step 2: Tailor the resume

Start from `references/master-resume.tex`, written verbatim to a working `.tex`, then edit.

**Fixed, never change** (specifics in `profile.md`):

- Section order: Education, Experience, Projects, Technical Skills.
- Experience order: reverse chronological.
- Bullet counts per role and per project, as listed in `profile.md`.
- 10pt, one page.

**Swap, never add.** A one-page resume has roughly one bullet of slack. Anything added must displace something of similar length.

**What to actually tailor:**

1. **Projects (highest leverage).** Pick the projects from the bank in `profile.md` that match the role type, using its role-type mapping. Always include **at least one non-LLM project**; two LLM-wrapper projects in a row reads weak.

2. **Swap-in bullets.** The master may contain commented `% SWAP-IN for ...` bullets. When the employer matches the stated condition, uncomment the swap-in and comment out the bullet it replaces. One in, one out.

3. **Skills rows.** Reorder *within* each row so JD-named technologies appear first. Add a technology only if it appears in the bullets, the project bank, or the projects doc. Never add a skill the candidate has not used. When space is tight, drop a low-relevance item not named in the JD rather than cutting a JD-relevant addition.

4. **Bullet emphasis.** Re-bold to match JD keywords (2-3 bolded terms per bullet, never more). Light rewording to surface a JD term is fine when the underlying fact is unchanged.

Respect any ratio rules in `profile.md` (e.g. a cap on QA/testing-flavored bullets, so the resume doesn't read as a different role than intended).

**Page-fit note:** assume the master compiles to exactly one page with zero slack. After every edit, compile with pdflatex and compare wrapped-line counts against the master. If a replacement bullet wraps to more lines than the one it replaced, shorten it until it doesn't, rather than cutting content elsewhere.

## Step 3: Tailor the cover letter

Four paragraphs, one page, from `references/cover-letter.tex`.

### The opening device is mandatory

**Every letter opens with an anaphora or an antimetabole.** Alternate between the two across applications so consecutive letters do not read identically.

- **Anaphora**: the same opening phrase across three successive sentences. Exactly three beats, never more, and the third beat lands on the company and the role.
- **Antimetabole**: one sentence that reverses its own terms (A then B, then B then A). E.g. *"The hard part was never making the system fast; it was making the fast system correct."*

Rules for the device:

- It must be **true** and it must be **the candidate's**, not a generic statement about ambition or excellence.
- It must point at the company's actual problem or at how the candidate actually works. Never at how impressive the company is.
- Use one device, not both, and never more than one per letter.
- Rotate the anaphora's repeated phrase; never reuse a phrase listed as spent in `profile.md`. Pick a verb that fits the domain ("Reliability is...", "I learned...", "Every X starts as...", "Performance is...").
- If it feels forced after two attempts, the angle is wrong. Rethink paragraph 1 rather than forcing the figure.

### The four paragraphs

1. **Opening device, landing on the company's actual problem.** One concrete, true detail about what they build and why correctness matters there. Search if the JD does not give enough. Never invent a product fact or a metric about the company.
2. **The most relevant experience, with specifics**: real systems, real numbers, drawn only from the resume or projects doc.
3. **A second story showing range or judgment.** When it retells a fact that is on the resume, use the resume's exact current phrasing and figures.
4. **What they want to learn there, then close.** Concrete, not flattering.

### Voice

- Declarative, first person, specific. Plain, no padding.
- Do not restate resume bullets verbatim. The letter explains *why* the work mattered.
- Ban: "I am passionate about", "I am excited to apply", "fast-paced environment", "leverage my skills", any em dash.
- Never invent a comparison or number that isn't on the resume/projects doc. Descriptive elaboration of a real bullet is fine; invented specifics are not.
- Never let the letter be more specific or more jargon-heavy than the resume about the same fact. If a resume bullet was deliberately generalized (e.g. for confidentiality), the letter stays at or below that level of generality.

## Step 4: Verify internally, before printing anything

Compiling is a **check, never a deliverable**. Run `pdflatex` on the tailored resume and confirm:

- [ ] Resume is exactly **1 page**. If it is 2, cut before printing; never hand over source that overflows.
- [ ] The letter opens with an anaphora or antimetabole, and it is not the same device or phrase as the last letter.
- [ ] Any fact the letter shares with the resume uses the resume's current phrasing and figures, and complies with the confidentiality rules in `profile.md`.
- [ ] `grep -- "---"` finds no em dash outside `%` comment banners.
- [ ] No `MONTH YEAR`, `CITY, PROV`, `COMPANY`, `STREET`, `PARAGRAPH` or other placeholder survives.
- [ ] No `\textasciitilde` or `$\sim$` anywhere; write plain numbers or "roughly".
- [ ] `\resumeItem` macro still reads `{#1\vspace{-2pt}}` with **no space** before `\vspace`. The space is a real bug: when a bullet's last line lands near the right margin, the negative spacing is pushed onto a phantom line and the gap under that block visibly widens.
- [ ] Every claim traces to the master resume, the project bank, or the projects doc. Nothing invented.

If `pdflatex` is unavailable, say plainly that page fit is unverified rather than asserting one page.

`cover.cls` is the Deedy class file and lives in the Overleaf project, so `\documentclass{cover}` may not compile locally. If it is unavailable, verify the letter by structure and word count (aim 350-450 words) and say so; do not substitute a different document class.

## Step 5: Output the LaTeX

**Print the source, do not attach files and do not produce PDFs.** The candidate pastes into Overleaf and compiles there.

- Print both documents (or just the resume) as complete fenced ```latex blocks, resume first, cover letter second.
- **Never abbreviate.** No `% ...rest unchanged`, no ellipses, no "only the changed bullet". Print the whole document, preamble included.
- Mention Overleaf dependencies only if they matter: the resume needs `glyphtounicode` (standard in TeX Live); the cover letter needs `cover.cls` in the same project.

Then close with a short changelog: the angle chosen, the opening device used (if a letter was made), which projects were swapped in and why, which skills were reordered, and anything the candidate must confirm.

## Hard rules

- **The resume is the single source of truth.** Where the projects doc, an older draft, or any other source disagrees on a number, date, title, link or metric, the resume wins. Do not surface the discrepancy; just use the resume's value. This applies to the cover letter too.
- **Confidentiality rules in `profile.md` are absolute** and govern the resume and the letter equally. Never reintroduce a figure or phrasing that file forbids.
- **No fabricated metrics, ever.** If a tailored bullet needs a number that hasn't been given, leave it unquantified and flag it.
- Honor any deliberate omissions listed in `profile.md` (e.g. no GPA, no personal website, broken demo links).
- Dates use `Apr. 2025 -- Dec. 2025` style; en dash `--` in ranges only.
