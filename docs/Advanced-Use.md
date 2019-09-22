# Advanced Use

### Initialize A Graph

Now that Cayley is downloaded (or built), let's create our database. `init` is the subcommand to set up a database and the right indices.

You can set up a full [configuration file](Configuration.md) if you'd prefer, but it will also work from the command line.

Examples for each backend can be found in `store.address` format from [config file](Configuration.md).

Those two options (db and dbpath) are always going to be present. If you feel like not repeating yourself, setting up a configuration file for your backend might be something to do now. There's an example file, `cayley_example.yml` in the root directory.

You can repeat the `--db (-i)` and `--dbpath (-a)` flags from here forward instead of the config flag, but let's assume you created `cayley_overview.yml`

Note: when you specify parameters in the config file the config flags (command line arguments) are ignored.

### Load Data Into A Graph

After the database is initialized we load the data.

```bash
./cayley load -c cayley_overview.yml -i data/testdata.nq
```

And wait. It will load. If you'd like to watch it load, you can run

```bash
./cayley load -c cayley_overview.yml -i data/testdata.nq --alsologtostderr=true
```

And watch the log output go by.

If you plan to import a large dataset into Cayley and try multiple backends, it makes sense to first convert the dataset
to Cayley-specific binary format by running:

```bash
./cayley conv -i dataset.nq.gz -o dataset.pq.gz
```

This will minimize parsing overhead on future imports and will compress dataset a bit better.

### Connect a REPL To Your Graph

Now it's loaded. We can use Cayley now to connect to the graph. As you might have guessed, that command is:

```bash
./cayley repl -c cayley_overview.yml
```

Where you'll be given a `cayley>` prompt. It's expecting Gizmo/JS, but that can also be configured with a flag.

New nodes and links can be added with the following command:

```bash
cayley> :a subject predicate object label .
```

Removing links works similarly:

```bash
cayley> :d subject predicate object .
```

This is great for testing, and ultimately also for scripting, but the real workhorse is the next step.

Go ahead and give it a try:

```
// Simple math
cayley> 2 + 2

// JavaScript syntax
cayley> x = 2 * 8
cayley> x

// See all the entities in this small follow graph.
cayley> graph.Vertex().All()

// See only dani.
cayley> graph.Vertex("<dani>").All()

// See who dani follows.
cayley> graph.Vertex("<dani>").Out("<follows>").All()
```

### Serve Your Graph

Just as before:

```bash
./cayley http -c cayley_overview.yml
```

And you'll see a message not unlike

```bash
listening on :64210, web interface at http://localhost:64210
```

If you visit that address (often, [http://localhost:64210](http://localhost:64210)) you'll see the full web interface and also have a graph ready to serve queries via the [HTTP API](HTTP.md)

#### Access from other machines

When you want to reach the API or UI from another machine in the network you need to specify the host argument:

```bash
./cayley http --config=cayley.cfg.overview --host=0.0.0.0:64210
```

This makes it listen on all interfaces. You can also give it the specific the IP address you want Cayley to bind to.

**Warning**: for security reasons you might not want to do this on a public accessible machine.
