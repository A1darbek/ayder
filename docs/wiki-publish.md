# GitHub Wiki Publish Guide

This repository keeps wiki-ready pages in the `wiki/` directory.

## Wiki Pages

- wiki/Home.md
- wiki/Quickstart.md
- wiki/Architecture.md
- wiki/Operations.md
- wiki/Jepsen-Evidence.md
- wiki/FAQ.md

## How To Publish

GitHub Wiki is a separate Git repository named `<repo>.wiki.git`.

1. Clone the wiki repo:
   `git clone https://github.com/A1darbek/ayder.wiki.git`
2. Copy pages from this repo:
   `cp -f ../ayder/wiki/*.md ayder.wiki/`
3. Commit and push:
   `cd ayder.wiki`
   `git add *.md`
   `git commit -m "Update wiki pages"`
   `git push`

## Publishing Rules

- Keep page names stable so links do not break.
- Keep claim wording in `wiki/Jepsen-Evidence.md` aligned with `README.md`.
- Keep dates, matrix paths, and artifact paths exact.
