# oxkart

CLI to convert kart datasets into parquet

## List datasets

```bash
$ oxkart list ~/kart-test

nz_vineyard_polygons_topo_150k
```

## Export to parquet

```bash

$ oxkart to-parquet ~/kart-test/ nz_vineyard_polygons_topo_150k.parquet

Finished processing 2,362 features in 11.85ms (199,383 features/sec)

```

Can be run on both packed `.git` or `.kart` indexes and on full clones

```bash
$ gh repo clone blacha/kart-test
$ ls kart-test/nz_vineyard_polygons_topo_150k

# Read from the file system
$ oxkart to-parquet ~/kart-test/nz_vineyard_polygons_topo_150k nz_vineyard_polygons_topo_150k.parquet

# Force read from git
$ oxkart to-parquet ~/kart-test/.git nz_vineyard_polygons_topo_150k.parquet

```
