# RDF terms in rdflib

> **Threlium:** Use `memory_query` to retrieve this doc from the knowledge graph. Use `formal_reason` with `facts_ttl`/`shapes_ttl` to validate or run SPARQL on *your* graph (`query` is not LightRAG). See also `turtle_syntax.md`, `sparql_functions.md`, `shacl_sparql.md`.
>
> **Source:** RDFLib @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6 — https://github.com/RDFLib/rdflib/tree/0fdac395078b81313f30eeab74b1a18eb7bb4db6/docs
> **Verified stack:** rdflib 7.6.0, pyshacl 0.31.0


Terms are the kinds of objects that can appear in a RDFLib's graph's triples. Those that are part of core RDF concepts are: `IRIs`, `Blank Node` and `Literal`, the latter consisting of a literal value and either a [datatype](https://www.w3.org/TR/xmlschema-2/#built-in-datatypes) or an [RFC 3066](https://tools.ietf.org/html/rfc3066) language tag.

!!! info "Origins"
    RDFLib's class for representing IRIs/URIs is called "URIRef" because, at the time it was implemented, that was what the then current RDF specification called URIs/IRIs. We preserve that class name but refer to the RDF object as "IRI".

## Class hierarchy

All terms in RDFLib are sub-classes of the [`Identifier`][rdflib.term.Identifier] class. A class diagram of the various terms is:

![Term Class Hierarchy](_static/term_class_hierarchy.svg)

Nodes are a subset of the Terms that underlying stores actually persist.

The set of such Terms depends on whether or not the store is formula-aware. Stores that aren't formula-aware only persist those terms core to the RDF Model but those that are formula-aware also persist the N3 extensions. However, utility terms that only serve the purpose of matching nodes by term-patterns will probably only be terms and not nodes.

## Python Classes

The three main RDF objects - *IRI*, *Blank Node* and *Literal* are represented in RDFLib by these three main Python classes:

### URIRef

An IRI (Internationalized Resource Identifier) is a resource identifier. In
Turtle you write it in angle brackets or as a prefixed name; both denote the same
node:

```turtle
<http://example.com>          # full IRI
ex:Thing                      # prefixed name (with @prefix ex: <http://example.com#>)
```

### BNodes

In RDF, a blank node (BNode) represents a resource for which no IRI or literal is
given — an anonymous resource. A blank node can only be used as subject or object.
Its label is scoped to a single serialization: `_:p1` in one document is not the
same node as `_:p1` in another. In Turtle write it with a label or the inline
`[ ... ]` form (always a fresh node):

```turtle
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

_:linda foaf:name "Linda" .
ex:bob foaf:knows [ foaf:name "Anon" ] .
```

### Literals

Literals are attribute values — a name, a date, a height — carrying a lexical form
and optionally a datatype or a language tag. In Turtle:

```turtle
"Nicholas"               # plain string literal
"39"^^xsd:integer        # the integer 39 (with @prefix xsd:)
```

A *langString* is a string with a language tag:

```turtle
"Nicholas"@en            # English
"Mikołaj"@pl             # Polish
```

A custom datatype is just a custom IRI after `^^`. For example GeoSPARQL's
`geoJSONLiteral`:

```turtle
@prefix geo: <http://www.opengis.net/ont/geosparql#> .

ex:point geo:asGeoJSON
  "{\"type\": \"Point\", \"coordinates\": [-83.38,33.95]}"^^geo:geoJSONLiteral .
```

See the [`Literal`][rdflib.term.Literal] class' documentation, followed by notes on Literal from the [RDF 1.1 specification 'Literals' section](https://www.w3.org/TR/rdf11-concepts/#section-Graph-Literal).

A literal in an RDF graph contains one or two named components.

All literals have a lexical form being a Unicode string, which SHOULD be in Normal Form C.

Plain literals have a lexical form and optionally a language tag as defined by [RFC 3066](https://tools.ietf.org/html/rfc3066), normalized to lowercase. An exception will be raised if illegal language-tags are passed to [\_\_new\_\_()][rdflib.term.Literal.__new__].

Typed literals have a lexical form and a datatype URI being an RDF URI reference.

!!! abstract "Language vs. locale"
    When using the language tag, care must be taken not to confuse language with locale. The language tag relates only to human language text. Presentational issues should be addressed in end-user applications.

!!! quote "Case sensitive"
    The case normalization of language tags is part of the description of the abstract syntax, and consequently the abstract behaviour of RDF applications. It does not constrain an RDF implementation to actually normalize the case. Crucially, the result of comparing two language tags should not be sensitive to the case of the original input. -- [RDF Concepts and Abstract Syntax](http://www.w3.org/TR/rdf-concepts/#section-Graph-URIref)

#### Common XSD datatypes

Most simple literals such as *string* or *integer* have XML Schema (XSD) datatypes defined for them, see the figure below. Additionally, these XSD datatypes are listed in the [XSD Namespace class][rdflib.namespace.XSD] that ships with RDFLib, so many Python code editors will prompt you with autocomplete for them when using it.

Remember, you don't *have* to use XSD datatypes and can always make up your own, as GeoSPARQL does, as described above.

![datatype hierarchy](_static/datatype_hierarchy.png)

#### Python conversions

RDFLib Literals essentially behave like unicode characters with an XML Schema datatype or language attribute.

The class provides a mechanism to both convert Python literals (and their built-ins such as time/date/datetime) into equivalent RDF Literals and (conversely) convert Literals to their Python equivalent. This mapping to and from Python literals is done as follows:

| XML Datatype | Python type |
|--------------|-------------|
| None | None [^1] |
| xsd:time | time [^2] |
| xsd:date | date |
| xsd:dateTime | datetime |
| xsd:string | None |
| xsd:normalizedString | None |
| xsd:token | None |
| xsd:language | None |
| xsd:boolean | boolean |
| xsd:decimal | Decimal |
| xsd:integer | long |
| xsd:nonPositiveInteger | int |
| xsd:long | long |
| xsd:nonNegativeInteger | int |
| xsd:negativeInteger | int |
| xsd:int | long |
| xsd:unsignedLong | long |
| xsd:positiveInteger | int |
| xsd:short | int |
| xsd:unsignedInt | long |
| xsd:byte | int |
| xsd:unsignedShort | int |
| xsd:unsignedByte | int |
| xsd:float | float |
| xsd:double | float |
| xsd:base64Binary | base64 |
| xsd:anyURI | None |
| rdf:XMLLiteral | Document (xml.dom.minidom.Document [^3] |
| rdf:HTML | DocumentFragment (xml.dom.minidom.DocumentFragment) |

[^1]: plain literals map directly to value space
[^2]: Date, time and datetime literals are mapped to Python instances using the RDFlib xsd_datetime module, that is based on the [isodate](http://pypi.python.org/pypi/isodate/) package).
[^3]: this is a bit dirty - by accident the `html5lib` parser produces `DocumentFragments`, and the xml parser `Documents`, letting us use this to decide what datatype when round-tripping.

An appropriate data-type and lexical representation can be found using `_castPythonToLiteral`, and the other direction with `_castLexicalToPython`.

All this happens automatically when creating `Literal` objects by passing Python objects to the constructor, and you never have to do this manually.

You can add custom data-types with [`bind()`][rdflib.term.bind], see also [`custom_datatype example`][examples.custom_datatype]

## In Threlium

The model does **not** call `rdflib.Graph()` directly in production. Author Turtle in the `formal_reason` tool fields (`facts_ttl`, `shapes_ttl`, optional `ontology_ttl`) and read the observation (`conforms`, `violations`, `query_result`, `derived_triples`). End-to-end tool-call patterns: `formal_reason_workflows.md`. SPARQL language reference for the `query` field: `sparql_functions.md`.
