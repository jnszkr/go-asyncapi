# Agent instructions

## Before pushing

Apply lint autofixes before every push to a branch, and commit any changes they make:

```sh
golangci-lint run --fix
```

Fix whatever it reports but cannot fix on its own, so that the command exits cleanly before you push.
