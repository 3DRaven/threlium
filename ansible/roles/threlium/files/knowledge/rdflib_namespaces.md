# Namespaces and Bindings

> **Threlium:** Use `memory_query` to retrieve this doc from the knowledge graph. Use `formal_reason` with `facts_ttl`/`shapes_ttl` to validate or run SPARQL on *your* graph (`query` is not LightRAG). See also `turtle_syntax.md`, `sparql_functions.md`, `shacl_sparql.md`.
>
> **Source:** RDFLib @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6 — https://github.com/RDFLib/rdflib/tree/0fdac395078b81313f30eeab74b1a18eb7bb4db6/docs
> **Verified stack:** rdflib 7.6.0, pyshacl 0.31.0


RDFLib provides several short-cuts to working with many URIs in the same namespace.

In Turtle you declare a namespace once with `@prefix` and then write compact
names against it. `ex:Person` expands to `http://example.org/Person`:

```turtle
@prefix ex: <http://example.org/> .

ex:Person a ex:Class .
```

For local names that are not valid prefixed-names (spaces, reserved characters),
use the full IRI in angle brackets instead, e.g.
`<http://example.org/first%20name>`.

## Common Namespaces

The `namespace` module defines many common namespaces such as RDF, RDFS, OWL, FOAF, SKOS, PROF, etc. The list of the namespaces provided grows with user contributions to RDFLib.

These Namespaces, and any others that users define, can also be associated with prefixes using the [`NamespaceManager`][rdflib.namespace.NamespaceManager], e.g. using `foaf` for `http://xmlns.com/foaf/0.1/`.

When the engine parses your `facts_ttl` / `shapes_ttl`, it records the namespace
to prefix mappings from your `@prefix` directives. Those prefixes are reused when
serialising the report and when parsing SPARQL. You bind a prefix simply by
declaring it at the top of a TTL field — e.g. `@prefix foaf:` for the standard
FOAF namespace and `@prefix ex:` for your own.

## Namespace binding (engine internals)

Internally RDFLib's `NamespaceManager` auto-binds some well-known prefixes
according to a strategy. This is handled by the engine, not something you
configure from the tool. The strategies are listed below for reference.

Valid strategies are:

- core:
  - binds several core RDF prefixes only
  - owl, rdf, rdfs, xsd, xml from the NAMESPACE_PREFIXES_CORE object
  - this is default
- rdflib:
  - binds all the namespaces shipped with RDFLib as DefinedNamespace instances
  - all the core namespaces and all the following: brick, csvw, dc, dcat
  - dcmitype, dcterms, dcam, doap, foaf, geo, odrl, org, prof, prov, qb, sdo
  - sh, skos, sosa, ssn, time, vann, void
  - see the NAMESPACE_PREFIXES_RDFLIB object in [`rdflib.namespace`][rdflib.namespace] for up-to-date list
- none:
  - binds no namespaces to prefixes
  - note this is NOT default behaviour
- cc:
  - using prefix bindings from prefix.cc which is a online prefixes database
  - not implemented yet - this is aspirational

### Re-binding

Regardless of strategy, you control prefixes in your own TTL: whichever prefix you
write in `@prefix` is the one used. To prefer `geosp:` over the default `geo:` for
GeoSPARQL, just declare `@prefix geosp: <http://www.opengis.net/ont/geosparql#> .`

### Compact term forms

The "readable" representation of a term is exactly its Turtle/N3 form: a URI is
`foaf:Person` (or `<http://xmlns.com/foaf/0.1/Person>` without a prefix), and a
typed literal is `"2"^^xsd:integer`. Equivalences:

| Full IRI / value | Compact (with prefixes) |
|------------------|-------------------------|
| `<http://xmlns.com/foaf/0.1/Person>` | `foaf:Person` |
| `"2"^^<http://www.w3.org/2001/XMLSchema#integer>` | `"2"^^xsd:integer` |
| `<http://foo/bar#baz>` | `ns:baz` (with `@prefix ns: <http://foo/bar#>`) |

## Namespaces in SPARQL Queries

In the `query` field, declare each prefix you use with a `PREFIX` directive at the
top of the query — there is no implicit binding from a graph manager:

```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT * WHERE { ?p a foaf:Person }
```

To use an empty prefix (e.g. `?a :knows ?b`), set a default namespace with a
`PREFIX` directive that has no prefix:

```sparql
PREFIX : <http://xmlns.com/foaf/0.1/>
```

## In Threlium

The model does **not** call `rdflib.Graph()` directly in production. Author Turtle in the `formal_reason` tool fields (`facts_ttl`, `shapes_ttl`, optional `ontology_ttl`) and read the observation (`conforms`, `violations`, `query_result`, `derived_triples`). End-to-end tool-call patterns: `formal_reason_workflows.md`. SPARQL language reference for the `query` field: `sparql_functions.md`.
