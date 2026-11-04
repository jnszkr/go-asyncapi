# Agent instructions

## Before pushing

Run these before every push to a branch. They match what CI checks. Commit any changes they make, and fix whatever they report, so that every command exits cleanly before you push.

1. Apply lint autofixes:

   ```sh
   golangci-lint run --fix
   ```

2. If you changed the generator or an example spec, refresh the golden files:

   ```sh
   go test ./internal/codegen -update
   ```

3. Bring generated code and the vendor directory up to date:

   ```sh
   go generate ./...
   go mod tidy
   go mod vendor
   ```

4. Run the checks:

   ```sh
   go vet ./...
   go test -race ./...
   ```

   `go test -race ./...` takes over two minutes, mostly in `internal/codegen`, so run it in the background.

Use a [Conventional Commits](https://www.conventionalcommits.org/) title for the PR, since CI checks it and it decides the next release.
