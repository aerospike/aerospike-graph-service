<!--
Thanks for opening a PR. A few notes to make review fast:

- Link to the issue you're closing (`Fixes #123`) in the description
  below. If there is no issue, explain why.
- One logical change per PR. Split drive-by cleanups into their own PR.
- Sign off every commit with the DCO (`git commit -s`).
- Read CONTRIBUTING.md if you haven't recently.
-->

## Summary

<!-- What does this PR change and why? -->

## Related issues

Fixes #

## Type of change

- [ ] Bug fix (non-breaking, fixes an issue)
- [ ] New feature (non-breaking, adds functionality)
- [ ] Breaking change (fix or feature that changes existing behavior)
- [ ] Performance / internal refactor (no user-visible change)
- [ ] Docs only
- [ ] CI / build / tooling only

## Checklist

- [ ] I have read and agree to [CONTRIBUTING.md](../CONTRIBUTING.md).
- [ ] Every commit is signed off (`git commit -s`).
- [ ] I added or updated tests for the changed behavior, or explained
      in the description why tests are not needed.
- [ ] `mvn -ntp verify` passes locally.
- [ ] If I touched any `pom.xml`, I ran `mvn dependency:tree` and
      confirmed no GPL / AGPL / SSPL dependency landed in compile or
      runtime scope, and `mvn license:check` is green.
- [ ] I updated the docs under `docs/` for any user-visible change.
- [ ] I did **not** commit any secret, credential, or real TLS key.
      (Test fixtures under the paths allow-listed in `.gitleaks.toml`
      are fine.)

## Test plan

<!--
How did you verify this works? Be concrete: commands run, traversals
exercised, benchmarks compared, etc.
-->

## Notes for reviewers

<!-- Anything non-obvious, trade-offs considered, or areas where you
want extra scrutiny. -->
