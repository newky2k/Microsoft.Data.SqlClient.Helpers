# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Branching

- `main` is protected and is the release branch. Never commit or push to it directly: every push to `main` publishes the packages to nuget.org.
- `development` is the merge target for features and bug fixes. Branch from `development` and open pull requests back into `development`.
- Releases are made by a pull request from `development` into `main`.
- Pull requests into `development` or `main` must pass the `CI` workflow before merging.

### Protection rules for `main`

Defined in `.github/rulesets/protect-main.json`. To apply them: GitHub > Settings > Rules > Rulesets > New ruleset > Import a ruleset, then pick that file.

- Changes go in through a pull request only (no direct pushes); review threads must be resolved
- The `build` check from the `CI` workflow must pass
- No force pushes and the branch cannot be deleted

## Commands

```bash
# Build the entire solution
dotnet build Microsoft.Data.SqlClient.Helpers.slnx

# Build in Release (also produces the NuGet packages, under */bin/Release)
dotnet build Microsoft.Data.SqlClient.Helpers.slnx -c Release
```

`SqlHelperSample` targets `net10.0-windows`; on Linux or macOS add `-p:EnableWindowsTargeting=true`. There is no test project.

## Architecture

Two NuGet packages and a sample console app:

```
DSoft.System.Data.Common.Extensions        System.Data.Common.Extensions/
    ↑
DSoft.Microsoft.Data.SqlClient.Helpers     Microsoft.Data.SqlClient.Helpers/
```

Both target `netstandard2.1`, `net462`, `net8.0`, `net9.0` and `net10.0`.

**`DSoft.System.Data.Common.Extensions`**: provider-neutral helpers.
- `DbConnectionExtension`: extension methods on any `DbConnection` for running scripts and queries (`Execute`, `ExecuteScalar`, `Query`, plus async versions) and for inserting, updating and checking rows without writing SQL (`InsertOneAsync`, `InsertManyAsync`, `UpdateOneAsync`, `ExistsAsync`, ...)
- Value helpers for parameters: `AsValueOrDbNull` / `AsValueOrDefault` (strings, dates), `DataRow.WhenValid`, `NameValueCollection.ValueAsInt`

**`DSoft.Microsoft.Data.SqlClient.Helpers`**: SQL Server specifics, built on `Microsoft.Data.SqlClient`.
- `DataConnection`: an `IDisposable` wrapper around `SqlConnection` that opens the connection on demand and exposes the extension methods above, transactions, and `CanConnect` / `CanConnectAsync`
- `DataConnectionStringManager`: central connection string store. Resolves a key through `ConnectionStringLoader`, then overrides (`AddOverride`), then values set with `SetConnectionString`

## Key configuration

- **Shared build settings** (`Directory.Build.props`): NuGet metadata, the `DSIcon.png` package icon, the root `README.md` as the package readme, SourceLink in Release, and strong-name signing with each project's `DSoft.snk`. The sample turns signing and packaging off.
- **Strong naming**: each package project has its own `DSoft.snk`. Do not remove these.
- **Package version**: the `<Version>` in each project is only used for local builds. The release workflow sets the version for both packages.

## CI (GitHub Actions, `.github/workflows/`)

- `ci.yml`: runs on pull requests into `development` and `main`. Restores and builds Release. Publishes nothing.
- `release.yml`: runs on every push to `main` (changes only to Markdown or `ci.yml` are skipped; run it by hand with workflow_dispatch to test a workflow change). Builds Release as `6.1.yyMM.dd<daily revision>` plus `RELEASE_SUFFIX` (empty for a stable version, `-prerelease` for a prerelease), uploads the packages as the `drop` artifact, pushes them to nuget.org with Trusted Publishing (OIDC, `NUGET_USER` secret, `nuget` environment), then tags the commit `v<version>` and creates a GitHub release with the packages attached.
