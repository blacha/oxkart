# oxkart

CLI to convert kart datasets into parquet




## List datasets
```bash
$ oxkart list ~/kart-test

nz_vineyard_polygons_topo_150k
```

List with dataset schemas and output into json
```bash
$ oxkart list ~/kart-test --schema --json

```

List with dataset stats
```bash
$ oxkart list ~/kart-test --stats
{"name":"nz_imagery_surveys","count":518,"size":9685898}
```

## Export to parquet

```bash

$ oxkart export ~/kart-test/ nz_vineyard_polygons_topo_150k.parquet

Finished processing 2,362 features in 11.85ms (199,383 features/sec)

```

Can be run on both packed `.git` or `.kart` indexes and on full clones

```bash
$ gh repo clone blacha/kart-test
$ ls -a1 kart-test/nz_vineyard_polygons_topo_150k

metadata.xml
.table-dataset/

# Read from the file system
$ oxkart export ~/kart-test/nz_vineyard_polygons_topo_150k nz_vineyard_polygons_topo_150k.parquet

# Force read from git
$ oxkart export ~/kart-test/.git nz_vineyard_polygons_topo_150k.parquet

```