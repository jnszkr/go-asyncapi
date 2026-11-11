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

5. Run mutation testing (see below) and make sure it passes.

## Mutation testing

We use [gremlins](https://gremlins.dev) to check that the tests actually catch bugs, not just execute code. It flips conditions, operators and increments one at a time and reruns the tests; a mutant that the tests do not notice is reported as `LIVED`. Configuration, including the minimum efficacy that fails the run, lives in `.gremlins.yaml`.

Install it once:

```sh
go install github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0
```

Use it in two places:

- **Whenever you add or change tests**, run it on the package you touched and look at what survived:

  ```sh
  gremlins unleash --timeout-coefficient 50 ./internal/spec
  ```

  The path covers that directory and everything below it. Each mutant's timeout is a multiple of how long the coverage run took, and that run is short for a single small package, so without the larger coefficient most mutants are wrongly reported as `TIMED OUT`.

  Add or tighten tests until every `LIVED` mutant in the code you are working on is `KILLED`. If a mutant cannot be killed because it does not change behaviour (an equivalent mutant), say so in the PR description rather than writing a test that pins an implementation detail.

- **Before every push**, run it on the whole module. It must exit cleanly, which means it met the thresholds in `.gremlins.yaml`:

  ```sh
  gremlins unleash
  ```

  It takes several minutes, mostly in `internal/codegen`, so run it in the background. Do not lower the thresholds to get a push through; add tests instead.

Use a [Conventional Commits](https://www.conventionalcommits.org/) title for the PR, since CI checks it and it decides the next release.
