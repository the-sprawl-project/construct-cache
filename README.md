# Construct Cache

A simple key value store that I personally use for learning rust and getting
comfortable with its learning curve, including concurrency support, borrowing
and whatnot. Part of The Sprawl Project.

## Try it out!

1. Clone using SSH

```bash
$ git clone git@github.com:the-sprawl-project/construct-cache.git
```

## Multi-Node Sync Demo (Recommended)

The system now supports synchronous multi-node replication via a central controlplane.

1. **Start the Controlplane**: This launches the orchestrator and 3 cache instances (ports 9001-9003).
   ```bash
   cargo run --bin controlplane
   ```

2. **Launch Clients**: Open new terminal windows and connect to different nodes to see replication in action:
   ```bash
   # Terminal A
   cargo run --bin construct_cache_client 9001
   
   # Terminal B
   cargo run --bin construct_cache_client 9002
   ```

3. **Verify Sync**: Create a key on port 9001 (`c key val`) and then read it from port 9002 (`g key`).

## Manual / Single Node
If you prefer running a single isolated instance:

1. **Run the server**:
   ```bash
   cargo run --bin construct_cache_server
   ```

2. **Run the client**:
   ```bash
   cargo run --bin construct_cache_client
   ```
   (Note: Use `h` for help within the client.)

## Syncing protobufs with `sprawl-protocol`

When developing features, you might want to sync your protobufs to the main
`spawl-protocol` repository as well. This will allow other repositories to use
the same protobuf structures when accessing the Construct Cache.

To sync protobufs, do the following:

1. Setup the `sprawl-protocol` repository through a `git clone`.
2. Set the `SPRAWL_PROTOCOLS_LOCAL_PATH` variable in your `.rc` file (`.zshrc`
for example) to point to the root of the repository
3. Run `make sync-protos-local` to sync the protobufs from the `sprawl-protocol`
repo to this project.
4. If any changes are made in this repo to the protobuf structures and should be
committed to the `sprawl-protocol` repository, run `make push-protos` and commit
the changes in that repository.

New features coming soon! Check out the issues tab.