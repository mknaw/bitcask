Rust bitcask implementation for my educational purposes.

Comes with a simple CLI to interact with running server:
```
> bitcask-cli set foo bar
> bitcask-cli get foo
bar
```

Also allows log compaction via a `merge` command.
