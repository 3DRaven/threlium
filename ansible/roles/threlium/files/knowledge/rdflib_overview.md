# RDFLib overview

> **Threlium:** Indexed reference; use `memory_query` before authoring Turtle/SPARQL. Runtime validation: `formal_reason` + pySHACL.
>
> **Source:** RDFLib README @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6
> **Docs:** https://rdflib.readthedocs.io

## Getting Started
RDFLib aims to be a pythonic RDF API. RDFLib's main data object is a `Graph` which is a Python collection
of RDF *Subject, Predicate, Object* Triples:

A graph is just a set of triples. In Threlium you author them as Turtle in the
`formal_reason` field `facts_ttl`; the engine loads them into a graph and you read
results back from the observation. One triple per *Subject Predicate Object*:

```turtle
@prefix ex: <http://example.org/> .

ex:subject ex:predicate ex:object .
```
The components of the triples are URIs (resources) or Literals (values).

URIs are grouped together by *namespace*. Common namespaces (declared with
`@prefix` in your Turtle) include `dc:`, `dcterms:`, `foaf:`, `skos:`, `owl:`,
`rdf:`, `rdfs:`, `xsd:`. Declare the ones you use at the top of each TTL field:

```turtle
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
```

To add a typed-literal triple, write it directly in Turtle. For example the
n-triples statement
`<http://example.com/person/nick> <http://xmlns.com/foaf/0.1/givenName> "Nick"^^<http://www.w3.org/2001/XMLSchema#string> .`
is expressed with prefixes as:

```turtle
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

<http://example.com/person/nick> foaf:givenName "Nick"^^xsd:string .
```

Binding `foaf:` and `xsd:` to prefixes (via `@prefix`) is what lets the URIs be
written in the short `foaf:givenName` / `^^xsd:string` form for Turtle, N3, TriG,
TriX and JSON-LD. To look up values you write a SPARQL `query` against the graph
rather than calling a Python accessor:

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?label WHERE { <http://dbpedia.org/resource/Semantic_Web> rdfs:label ?label }
```

See `formal_reason_workflows.md` for the end-to-end tool calls.

## Features
The library contains parsers and serializers for RDF/XML, N3,
NTriples, N-Quads, Turtle, TriX, JSON-LD, RDFa and Microdata.

The library presents a Graph interface which can be backed by
any one of a number of Store implementations.

This core RDFLib package includes store implementations for
in-memory storage and persistent storage on top of the Berkeley DB.

A SPARQL 1.1 implementation is included - supporting SPARQL 1.1 Queries and Update statements.

RDFLib is open source and is maintained on [GitHub](https://github.com/RDFLib/rdflib/). RDFLib releases, current and previous
are listed on [PyPI](https://pypi.python.org/pypi/rdflib/)

Multiple other projects are contained within the RDFlib "family", see <https://github.com/RDFLib/>.

## In Threlium

The model does **not** call `rdflib.Graph()` directly in production. Author Turtle in the `formal_reason` tool fields (`facts_ttl`, `shapes_ttl`, optional `ontology_ttl`) and read the observation (`conforms`, `violations`, `query_result`, `derived_triples`). End-to-end tool-call patterns: `formal_reason_workflows.md`. SPARQL language reference for the `query` field: `sparql_functions.md`.
