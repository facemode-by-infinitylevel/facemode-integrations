# Contributing

Thanks for helping improve the official FaceMode integration plugins.

## Updating a published plugin

Registries are immutable per version. Every change, no matter how small, requires a version bump and a fresh publish.

1. Make your code changes in the package directory.
2. Bump the version:
   - npm: `npm version patch|minor|major`
   - Python: edit `version` in `pyproject.toml`
3. Follow semantic versioning: patch = bugfix, minor = feature, major = breaking change.
4. Update the package's CHANGELOG.md in Keep a Changelog format.
5. Delete the old `dist/` folder before rebuilding.
6. Rebuild:
   - npm: `npm run build`
   - Python: `python -m build`
7. Publish:
   - npm: `npm publish` (the package is also published under the unscoped alias `agents-plugin-facemode` by temporarily changing the `name` field)
   - Python: `twine upload dist/*`
8. Commit, tag `git tag -a v<version>-plugins -m "<description>"`, and push the tag.

## Code style

- Keep PRs small and focused.
- Match the existing code style in each package.
- Use plain hyphens in docs, no em/en dashes.
- Run package tests before publishing: `npm test` in `agents-plugin-facemode`; python tests live under each package's `test/` directory.
