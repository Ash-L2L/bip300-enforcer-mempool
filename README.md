# BIP300 Enforcer Mempool

## Build

* Install dependencies (rustup)
* Clone this repository
* Build with `cargo build` or `cargo build --release`

## Configure Bitcoin node
* Must use a version of Bitcoin Core more recent than `75118a608fc22a57567743000d636bc1f969f748`.
* RPC server MUST be enabled.
* ZMQ sequence publishing MUST be enabled.
* `txindex` MUST be enabled.