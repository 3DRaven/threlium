# Creating RDF triples

> **Threlium:** Use `memory_query` to retrieve this doc from the knowledge graph. Use `formal_reason` with `facts_ttl`/`shapes_ttl` to validate or run SPARQL on *your* graph (`query` is not LightRAG). See also `turtle_syntax.md`, `sparql_functions.md`, `shacl_sparql.md`.
>
> **Source:** RDFLib @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6 — https://github.com/RDFLib/rdflib/tree/0fdac395078b81313f30eeab74b1a18eb7bb4db6/docs
> **Verified stack:** rdflib 7.6.0, pyshacl 0.31.0


## Creating Nodes

RDF data is a graph where the nodes are URI references, Blank Nodes or Literals. In RDFLib, these node types are represented by the classes [`URIRef`][rdflib.term.URIRef], [`BNode`][rdflib.term.BNode], and [`Literal`][rdflib.term.Literal]. `URIRefs` and `BNodes` can both be thought of as resources, such a person, a company, a website, etc.

* A `BNode` is a node where the exact URI is not known - usually a node with identity only in relation to other nodes.
* A `URIRef` is a node where the exact URI is known. In addition to representing some subjects and predicates in RDF graphs, `URIRef`s are always used to represent properties/predicates
* `Literals` represent object values, such as a name, a date, a number, etc. The most common literal values are XML data types, e.g. string, int... but custom types can be declared too

In Turtle you write these node types directly: a `URIRef` is an IRI (`ex:Bob` or
`<http://...>`), a blank node is `[]` or `_:linda`, and a `Literal` is a quoted
value, optionally datatyped (`"24"^^xsd:integer`):

```turtle
@prefix ex:  <http://example.org/people/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:Bob ex:name "Bob" ;            # plain string literal
       ex:age 24 ;                # integer literal (xsd:integer)
       ex:height 76.5 ;           # decimal literal
       ex:knows [ ex:name "Linda" ] .  # blank node
```

Datatyped literals and their XSD mapping are detailed in
[rdflib_rdf_terms.md](rdflib_rdf_terms.md).

Many URIs in the same *namespace* share a prefix; you declare it once with
`@prefix` and reuse it. `ex:bob` then expands to
`http://example.org/people/bob`. Common RDF/OWL namespaces have well-known
prefixes you simply declare at the top of each TTL field:

| Prefix | Namespace IRI | Example term |
|--------|---------------|--------------|
| `rdf:` | `http://www.w3.org/1999/02/22-rdf-syntax-ns#` | `rdf:type` |
| `foaf:` | `http://xmlns.com/foaf/0.1/` | `foaf:knows` |
| `sh:` | `http://www.w3.org/ns/shacl#` | `sh:NodeShape` |
| `sosa:` | `http://www.w3.org/ns/sosa/` | `sosa:Sensor` |
| `xsd:` | `http://www.w3.org/2001/XMLSchema#` | `xsd:integer` |


## Adding Triples to a graph

The engine parses `facts_ttl` for you (see [rdflib_parsing.md](rdflib_parsing.md)).
On the agent path you do not add triples one-by-one in Python — you write the
complete intended graph as Turtle. The "Bob knows Linda" graph is simply:

```turtle
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://example.org/people/Bob> a foaf:Person ;
    foaf:age 24 ;
    foaf:knows [ a foaf:Person ;
            foaf:name "Linda" ] ;
    foaf:name "Bob" .
```

For some properties only one value per resource makes sense — *functional
properties* with max-cardinality 1. In Turtle you write the single value; to
**enforce** uniqueness, validate with a `sh:maxCount 1` shape via `formal_reason`:

```turtle
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

[] a sh:NodeShape ;
  sh:targetClass foaf:Person ;
  sh:property [ sh:path foaf:age ; sh:maxCount 1 ] .
```

You can also combine entire graphs — see [graph-setops](rdflib_graphs.md).

## Removing / rewriting triples

There is no in-place mutation on the agent path: you author the final graph you
want validated, rather than removing triples from a live store. To *derive* a
corrected view (e.g. align a non-standard `foaf:member_name` onto `foaf:name`),
express the mapping as a SPARQL `CONSTRUCT` in the `query` field and read the
result from the observation:

```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
CONSTRUCT { ?s foaf:name ?n }
WHERE     { ?s foaf:member_name ?n }
```

!!! info "Foaf member name"
    Since rdflib 5.0.0, using `foaf:member_name` is somewhat prevented in RDFlib since FOAF is declared as a [`ClosedNamespace`][rdflib.namespace.ClosedNamespace] class instance that has a closed set of members and `foaf:member_name` isn't one of them! If LiveJournal had used RDFlib 5.0.0, an error would have been raised for `foaf:member_name` when the triple was created.


## Creating Containers & Collections

There are two convenience classes for RDF Containers & Collections which you can use instead of declaring each triple of a Containers or a Collections individually:

* [`Container`][rdflib.container.Container] (also `Bag`, `Seq` & `Alt`) and
* [`Collection`][rdflib.collection.Collection]

See their documentation for how.

## In Threlium

The model does **not** call `rdflib.Graph()` directly in production. Author Turtle in the `formal_reason` tool fields (`facts_ttl`, `shapes_ttl`, optional `ontology_ttl`) and read the observation (`conforms`, `violations`, `query_result`, `derived_triples`). End-to-end tool-call patterns: `formal_reason_workflows.md`. SPARQL language reference for the `query` field: `sparql_functions.md`.
