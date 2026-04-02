## Ducklake Plugin Test Notes

- Run this plugin as a module from `plugin/trino-ducklake` (current workflow is not tied into full reactor runs).
- Preferred command form: `cd plugin/trino-ducklake` then run `../../mvnw ...`.
- Use `-Dair.check.skip-all` during iterative test/debug runs to skip expensive validation plugins.
- Fast-loop example: `../../mvnw -Dair.check.skip-all -Dtest=TestDucklakeIntegration test`.
- `ducklake.test.catalog-backend` accepted values: `sqlite` (default), `duckdb`, `postgresql` (or `postgres`).
- Backend example: `../../mvnw -Dair.check.skip-all -Dducklake.test.catalog-backend=duckdb -Dtest=TestDucklakeIntegration test`.
- `ReportLeakedContainers` is disabled by default in this module's test runs to avoid Podman compatibility warning noise.
- Re-enable leaked-container checks with `-DReportLeakedContainers.disabled=false` when needed.
