# Getting started with RDFLib

> **Threlium:** Use `memory_query` to retrieve this doc from the knowledge graph. Use `formal_reason` with `facts_ttl`/`shapes_ttl` to validate or run SPARQL on *your* graph (`query` is not LightRAG). See also `turtle_syntax.md`, `sparql_functions.md`, `shacl_sparql.md`.
>
> **Source:** RDFLib @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6 — https://github.com/RDFLib/rdflib/tree/0fdac395078b81313f30eeab74b1a18eb7bb4db6/docs
> **Verified stack:** rdflib 7.6.0, pyshacl 0.31.0


## Installation

RDFLib is open source and is maintained in a [GitHub](https://github.com/RDFLib/rdflib/) repository. RDFLib releases, current and previous, are listed on [PyPI](https://pypi.python.org/pypi/rdflib/)

The best way to install RDFLib is to use `pip` (sudo as required):

```bash
pip install rdflib
```

If you want the latest code to run, clone the `main` branch of the GitHub repo and use that or you can `pip install` directly from GitHub:

```bash
pip install git+https://github.com/RDFLib/rdflib.git@main#egg=rdflib
```

## Support

Usage support is available via questions tagged with `[rdflib]` on [StackOverflow](https://stackoverflow.com/questions/tagged/rdflib) and development support, notifications and detailed discussion through the rdflib-dev group (mailing list): [http://groups.google.com/group/rdflib-dev](http://groups.google.com/group/rdflib-dev)

If you notice a bug or want to request an enhancement, please do so via our Issue Tracker in Github: [http://github.com/RDFLib/rdflib/issues](http://github.com/RDFLib/rdflib/issues)

## How it all works

*The package uses various Python idioms that offer an appropriate way to introduce RDF to a Python programmer who hasn't worked with RDF before.*

The primary interface that RDFLib exposes for working with RDF is a [`Graph`][rdflib.graph.Graph].

RDFLib graphs are un-sorted containers; they have ordinary Python `set` operations (e.g. [`add()`][rdflib.graph.Graph.add] to add a triple) plus methods that search triples and return them in arbitrary order.

RDFLib graphs are best thought of as a set of 3-item tuples ("triples", in
RDF-speak) — each row is one *Subject Predicate Object* statement. You never build
this set in Python on the agent path; you describe it as Turtle in `facts_ttl`.

## A tiny example

A graph is the parsed form of your Turtle. The data below is two statements; the
engine loads them when you pass it as `facts_ttl`:

```turtle
@prefix ex: <http://example.org/> .

ex:donna a ex:Person .
ex:donna ex:name "Donna Fales" .
```

## A more extensive example

The "build a FOAF graph" example becomes plain Turtle in `facts_ttl`. Two people
with nicks, names and mailboxes:

```turtle
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

<http://example.org/donna> a foaf:Person ;
  foaf:nick "donna"@en ;
  foaf:name "Donna Fales" ;
  foaf:mbox <mailto:donna@example.org> .

<http://example.org/edward> a foaf:Person ;
  foaf:nick "ed"^^xsd:string ;
  foaf:name "Edward Scissorhands" ;
  foaf:mbox "e.scissorhands@example.org"^^xsd:anyURI .
```

To find each `foaf:Person`'s mailbox you ask SPARQL instead of iterating in
Python — see the query example below.

## A SPARQL query example

To query *your* graph, put the data in `facts_ttl` and the SPARQL in the
`formal_reason` `query` field (it runs on the in-memory graph — no remote fetch).
This query returns the `name` of every `foaf:Person`:

```sparql
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?name
WHERE {
  ?p rdf:type foaf:Person .
  ?p foaf:name ?name .
}
```

The bindings appear in the observation under `query_result`. A full runnable tool
call for this is in `formal_reason_workflows.md` (SPARQL query scenario).

## In Threlium

The model does **not** call `rdflib.Graph()` directly in production. Author Turtle in the `formal_reason` tool fields (`facts_ttl`, `shapes_ttl`, optional `ontology_ttl`) and read the observation (`conforms`, `violations`, `query_result`, `derived_triples`). End-to-end tool-call patterns: `formal_reason_workflows.md`. SPARQL language reference for the `query` field: `sparql_functions.md`.
