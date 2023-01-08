# DIPAAL ETL

## Running DIPAAL ETL
### Running with Docker
```docker build . -t dipaal-etl```

To init the DWL:

```docker run dipaal-etl python3 main.py --init```

To load 2022 data:

```docker run -v data:/data dipaal-etl python3 main.py --clean --from_date 2022-01-01 --to_date 2022-12-31```

### Running locally
Please copy ```config-local-template.properties``` to ```config-local.properties``` and change the desired values.

### Configuring the ETL
Various parameters can be configured in the `config.properties` file.

The database master connection must be specified in the host/database/user/password properties.

For DW initialization, how to connect to the workers of the citus cluster must be specified as `host:port` entries in the `worker_connection_hosts` and `worker_connection_internal_hosts` properties. The first being the hosts ETL can reach the containers on, and the second being the host the Citus master can reach the containers on.

The `ais_path` property specified where to store downloaded ais files, and the `ais_url` property specifies where to download the files from.