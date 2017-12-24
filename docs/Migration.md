# From Cayley 0.6.1 to 0.7.x

First you need to dump all the data from the database via v0.6.1:

```bash
./cayley-0.6 dump --db <backend> --dbpath <address> --dump_type pquads --dump ./data.pq.gz
```

or using config file:

```bash
./cayley-0.6 dump --config <config> --dump_type pquads --dump ./data.pq.gz
```

And load the data into new database via v0.7.x (`pq` file extension is important):

```bash
./cayley load --init -d <new-backend> -a <new-address> -i ./data.pq.gz
```

or using config file:

```bash
./cayley load --init -c <new-config> -i ./data.pq.gz
```

## Dump via text format

An above guide uses Cayley-specific binary format to avoid encoding and parsing overhead and to compress output file better.

As an alternative, a standard nquads file format can be used to dump and load data (note `nq` extension):

```bash
./cayley-0.6 dump --config <config> --dump ./data.nq.gz
./cayley load --init -c <new-config> -i ./data.nq.gz
```

## Bolt, LevelDB, MongoDB

Cayley v0.7.0 is still able to read and write databases of these types in old format. It can be accessed by changing
backend name from `bolt`/`leveldb`/`mongo` to `bolt1`/`leveldb1`/`mongo1`. Thus, you can use guide for moving data between
different backends for v0.7.x (see below).

**Note:** support for old versions will be dropped starting from v0.7.1.

## SQL

Data format for SQL between v0.6.1 and v0.7.x has not changed significantly. Database can be upgraded
by executing the following SQL statements:

```sql
ALTER TABLE quads DROP id;
ALTER TABLE nodes ADD refs INT DEFAULT 0 NOT NULL;
ALTER TABLE nodes ALTER COLUMN refs DROP DEFAULT;
UPDATE nodes SET refs = 
  (SELECT count(1) FROM quads WHERE subject_hash = nodes.hash) +
  (SELECT count(1) FROM quads WHERE predicate_hash = nodes.hash) +
  (SELECT count(1) FROM quads WHERE object_hash = nodes.hash) +
  (SELECT count(1) FROM quads WHERE label_hash = nodes.hash);
```

# From different backend (Cayley 0.7+)

First you need to dump all the data from old backend (`pq` extension is important):

```bash
./cayley dump -d <backend> -a <address> -o ./data.pq.gz
```

or using config file:

```bash
./cayley dump -c <config> -o ./data.pq.gz
```

And load the data into a new backend and/or database:

```bash
./cayley load --init -d <new-backend> -a <new-address> -i ./data.pq.gz
```

or using config file:

```bash
./cayley load --init -c <new-config> -i ./data.pq.gz
```

## Dump via text format

An above guide uses Cayley-specific binary format to avoid encoding and parsing overhead and to compress output file better.

As an alternative, a standard nquads file format can be used to dump and load data (note `nq` extension):

```bash
./cayley dump -c <config> -o ./data.nq.gz
./cayley load --init -c <new-config> -i ./data.nq.gz
```