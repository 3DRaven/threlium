# Loading and saving RDF

> **Threlium:** Use `memory_query` to retrieve this doc from the knowledge graph. Use `formal_reason` with `facts_ttl`/`shapes_ttl` to validate or run SPARQL on *your* graph (`query` is not LightRAG). See also `turtle_syntax.md`, `sparql_functions.md`, `shacl_sparql.md`.
>
> **Source:** RDFLib @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6 — https://github.com/RDFLib/rdflib/tree/0fdac395078b81313f30eeab74b1a18eb7bb4db6/docs
> **Verified stack:** rdflib 7.6.0, pyshacl 0.31.0


## Reading RDF files

RDF data can be represented using various syntaxes (`turtle`, `rdf/xml`, `n3`, `n-triples`, `trix`, `JSON-LD`, etc.). The simplest format is `ntriples`, which is a triple-per-line format.

Create the file `demo.nt` in the current directory with these two lines in it:

```turtle
<http://example.com/drewp> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
<http://example.com/drewp> <http://example.com/says> "Hello World" .
```

On line 1 this file says "drewp is a FOAF Person:. On line 2 it says "drep says "Hello World"".

The engine detects the format of your TTL and parses it for you. The two
n-triples lines above are read as two triples; subjects, predicates and objects
are URIs or literals. You provide the text via `facts_ttl` (Turtle is the most
convenient syntax) — there is no file path or `parse()` call on the agent path.

## No remote RDF

RDFLib can read data from a URL, but Threlium **does not** fetch remote graphs.
Always inline the data in `facts_ttl` / `ontology_ttl`; a `http://`/`file:`
source is never dereferenced (see the security note below).

## Output / serialisation

You do not serialise to a file. The observation returns Turtle-formatted sections
(`report_text`, `derived_triples`, `query_result`). Turtle is the default RDF
serialisation in rdflib (since 6.0.0). For reference, rdflib supports these
formats out of the box:

| RDF Format | Keyword | Notes |
|------------|---------|-------|
| Turtle | turtle, ttl or turtle2 | turtle2 is just turtle with more spacing & linebreaks |
| RDF/XML | xml or pretty-xml | Was the default format, rdflib < 6.0.0 |
| JSON-LD | json-ld | There are further options for compact syntax and other JSON-LD variants |
| N-Triples | ntriples, nt or nt11 | nt11 is exactly like nt, only utf8 encoded |
| Notation-3 | n3 | N3 is a superset of Turtle that also caters for rules and a few other things |
| Trig | trig | Turtle-like format for RDF triples + context (RDF quads) and thus multiple graphs |
| Trix | trix | RDF/XML-like format for RDF quads |
| N-Quads | nquads | N-Triples-like format for RDF quads |

## Working with multi-graphs

To read and query multi-graphs, that is RDF data that is context-aware, you need to use rdflib's [`Dataset`][rdflib.Dataset] class. This an extension to [`Graph`][rdflib.Graph] that know all about quads (triples + graph IDs).

If you had this multi-graph data file (in the `trig` format, using new-style `PREFIX` statement (not the older `@prefix`):

```turtle
PREFIX eg: <http://example.com/person/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

eg:graph-1 {
    eg:drewp a foaf:Person .
    eg:drewp eg:says "Hello World" .
}

eg:graph-2 {
    eg:nick a foaf:Person .
    eg:nick eg:says "Hi World" .
}
```

Named graphs are queried with SPARQL's `GRAPH` keyword. To list each typed
subject together with the graph it lives in:

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?s ?g WHERE { GRAPH ?g { ?s rdf:type ?o } }
```

Note: `formal_reason` validates a single default graph — author your premises in
`facts_ttl`. Multi-graph TriG/quad datasets are an rdflib feature, not a tool
field.


## Security note (Threlium)

Avoid `Graph.parse()` on untrusted `http://` or `file:` URIs in agent code. In `formal_reason`, pass Turtle strings via `facts_ttl` / `shapes_ttl` only.

## In Threlium

The model does **not** call `rdflib.Graph()` directly in production. Author Turtle in the `formal_reason` tool fields (`facts_ttl`, `shapes_ttl`, optional `ontology_ttl`) and read the observation (`conforms`, `violations`, `query_result`, `derived_triples`). End-to-end tool-call patterns: `formal_reason_workflows.md`. SPARQL language reference for the `query` field: `sparql_functions.md`.
