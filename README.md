## KRC-20 Node System For Kaspa Ecosystem

<br>

### Operating Environment

OS - 64-bit Linux (Ubuntu24.04 recommended)

HW - 64-bit little-endian

   - 12 cores, 24GB RAM, 800GB SSD at least

   - 16 cores, 32GB RAM, 1TB NVMe recommended

<br>

### Download the latest version

https://github.com/kasplex/go-krc20d/releases

<br>

### Deploying using binary

A usable kaspa node for gRPC is required.

#### Mainnet:

<pre>./krc20d --kaspad-grpc=127.0.0.1:16110</pre>

#### Testnet-10:

<pre>./krc20d --testnet --kaspad-grpc=127.0.0.1:16210</pre>

#### Show startup parameters:

<pre>
./krc20d --help

KASPlex KRC-20 Node v3.01.260403
Usage:
  krc20d [OPTIONS]

Application Options:
      --configfile=          Use the specified configuration file; command-line arguments will be ignored.
      --showconfig           Show all configuration parameters.
      --sequencer=           Sequencer type; "kaspad" or "archive". (default: kaspad)
      --hysteresis=          Number of DAA Scores hysteresis for data scanning. (default: 3)
      --loopdelay=           Scan loop delay in milliseconds. (default: 550)
      --blockgenesis=        Genesis block hash.
      --daascorerange=       Valid DAA Score range.
      --seedisd=             Seed URL for Initial State Download (ISD). (default: https://seed-krc20.kasplex.org)
      --fullisd              Fully synchronize historical data (if supported by the ISD seed).
      --rollbackoninit=      Number of DAA Scores to rollback on initialization.
      --checkcommitment      Check state commitment on initialization.
      --debug=               Debug information level; [0-3] available. (default: 2)
      --testnet              Apply testnet parameters.
      --kaspad-grpc=         Kaspa node gRPC endpoint (comma-separated for multiple). (default: 127.0.0.1:16110)
      --cassa-host=          Cassandra cluster host (comma-separated for multiple).
      --cassa-port=          Cassandra cluster port.
      --cassa-user=          Cassandra cluster username.
      --cassa-pass=          Cassandra cluster password.
      --cassa-crt=           Cassandra cluster SSL certificate.
      --cassa-space=         Cassandra cluster keyspace name.
      --rocks-path=          RocksDB data path. (default: ./data)
      --rocks-dtl-index=     Maximum DAA Score lifetime for indexed data. (default: 86400000)
      --rocks-dtl-failed=    Maximum DAA Score lifetime for indexed failed transactions. (default: 8640000)
      --rocks-indexdisabled  Disable data indexing.
      --rocks-compactoninit  Perform compaction on RocksDB initialization (may take a long time).
      --lyncs-numslot=       Number of parallel slots for the Lyncs engine. (default: 8)
      --lyncs-maxinslot=     Maximum number of tasks per slot for the Lyncs engine. (default: 128)
      --api-host=            Listen host for the API server. (default: 0.0.0.0)
      --api-port=            Listen port for the API server. (default: 8005)
      --api-timeout=         Processing timeout for the API server in seconds. (default: 5)
      --api-connmax=         Maximum number of concurrent connections for the API server. (default: 500)
      --api-port-isd=        Listen port for the ISD server. (default: 8006)
      --api-connmax-isd=     Maximum number of concurrent connections for the ISD server. (default: 4)
      --api-fullisd          Enable ISD server for full historical data.
      --api-allow-unsync     Enable API server when not synchronized.
      --api-allow-debug      Enable debug API.

Help Options:
  -h, --help                 Show this help message
</pre>

<br>

### Deploying using docker-compose

After startup, a separate kaspa node will be deployed simultaneously, and no additional node needs to be provided; the startup parameters of kaspad and krc20d can be changed in the yml file.

#### Mainnet:

<pre>docker compose -f ./compose.mainnet.yml up -d</pre>

#### Testnet-10:

<pre>docker compose -f ./compose.testnet.yml up -d</pre>

<br>

### API Reference

https://docs-kasplex.gitbook.io/krc20/public-node-api
