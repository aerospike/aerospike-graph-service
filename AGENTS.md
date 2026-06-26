# Agent instructions

Guidance for AI coding agents (Cursor, etc.) working in this repository.

## Commits and DCO sign-off

Every commit must include a [Developer Certificate of Origin](https://developercertificate.org/) (DCO) sign-off. CI enforces this on all pull requests (see `.github/workflows/dco.yml`).

**Always create commits with `-s` / `--signoff`:**

```bash
git commit -s -m "your message"
```

The `Signed-off-by:` trailer must **exactly match** the commit author (`git config user.name` and `user.email`). A mismatch fails the DCO check even if a sign-off line is present.

Example trailer:

```
Signed-off-by: Your Name <you@example.com>
```

When amending or rebasing commits for a PR, re-apply sign-off:

```bash
git rebase --signoff origin/3.x-dev
git push --force-with-lease
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for full contribution requirements.
