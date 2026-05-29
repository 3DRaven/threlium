# SPARQL Functions and Patterns Reference

## Common Prefixes

All query examples below assume this SPARQL prolog (so each snippet is self-contained
and parseable on its own). Prepend it to any standalone query:

```sparql
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX ex:   <http://example.org/>
```

## Query Forms

### SELECT
```sparql
SELECT ?person ?name WHERE {
  ?person a ex:Person ;
          ex:name ?name .
}
```

### CONSTRUCT
```sparql
CONSTRUCT { ?person ex:fullName ?name }
WHERE { ?person ex:firstName ?fn ; ex:lastName ?ln .
        BIND(CONCAT(?fn, " ", ?ln) AS ?name) }
```

In `formal_reason`, pass CONSTRUCT (or SELECT) in the `query` field to derive or inspect triples on the graph you built in `facts_ttl` — the observation returns the constructed graph or bindings.

### ASK
```sparql
ASK { ex:Alice ex:knows ex:Bob }
```

## FILTER

```sparql
FILTER (?age > 18)
FILTER (?age >= 18 && ?age <= 65)
FILTER (CONTAINS(?name, "Alice"))
FILTER (STRSTARTS(STR(?uri), "http://example.org/"))  # wrap URIs in STR() first
FILTER (LANG(?label) = "en")
FILTER (DATATYPE(?val) = xsd:integer)
FILTER (!BOUND(?optional))
FILTER EXISTS { ?person ex:email ?email }
FILTER NOT EXISTS { ?person ex:deleted true }
FILTER (REGEX(?name, "^A.*", "i"))
```

## BIND

```sparql
BIND(CONCAT(?first, " ", ?last) AS ?fullName)
BIND(YEAR(NOW()) - YEAR(?birthDate) AS ?age)
BIND(IF(?score > 50, "pass", "fail") AS ?result)
BIND(COALESCE(?preferred, ?default, "unknown") AS ?value)
BIND(URI(CONCAT("http://example.org/", ?id)) AS ?uri)
```

## OPTIONAL

```sparql
SELECT ?person ?name ?email WHERE {
  ?person a ex:Person ; ex:name ?name .
  OPTIONAL { ?person ex:email ?email }
}
```

## UNION

```sparql
SELECT ?contact WHERE {
  { ?person ex:email ?contact }
  UNION
  { ?person ex:phone ?contact }
}
```

## Aggregates

```sparql
SELECT ?dept (COUNT(?emp) AS ?count) (AVG(?salary) AS ?avgSalary)
WHERE {
  ?emp ex:department ?dept ;
       ex:salary ?salary .
}
GROUP BY ?dept
HAVING (COUNT(?emp) > 5)
ORDER BY DESC(?count)
LIMIT 10
```

Available: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `GROUP_CONCAT`, `SAMPLE`

## Property Paths

```sparql
# Sequence (A then B)
?x ex:knows/ex:name ?friendName .

# Alternative (A or B)
?x ex:email|ex:phone ?contact .

# Inverse
?x ^ex:knows ?knownBy .

# Zero or more
?x ex:subClassOf* ?ancestor .

# One or more
?x ex:parent+ ?ancestor .

# Negated property set
?x !(rdf:type|rdfs:label) ?other .
```

## Subqueries

```sparql
SELECT ?person ?name WHERE {
  ?person ex:name ?name .
  {
    SELECT ?person WHERE {
      ?person ex:score ?s .
    }
    ORDER BY DESC(?s)
    LIMIT 10
  }
}
```

## String Functions

| Function | Description |
|----------|-------------|
| `STRLEN(?s)` | String length |
| `SUBSTR(?s, 1, 3)` | Substring (1-indexed) |
| `UCASE(?s)` | Uppercase |
| `LCASE(?s)` | Lowercase |
| `CONTAINS(?s, "x")` | Contains substring |
| `STRSTARTS(?s, "x")` | Starts with |
| `STRENDS(?s, "x")` | Ends with |
| `REPLACE(?s, "old", "new")` | Replace (regex) |
| `CONCAT(?a, ?b)` | Concatenate |
| `STR(?x)` | Convert to string |
| `ENCODE_FOR_URI(?s)` | URL-encode |

## Date/Time Functions

| Function | Description |
|----------|-------------|
| `NOW()` | Current dateTime |
| `YEAR(?d)` | Extract year |
| `MONTH(?d)` | Extract month |
| `DAY(?d)` | Extract day |
| `HOURS(?t)` | Extract hours |
| `MINUTES(?t)` | Extract minutes |

## Type Functions

| Function | Description |
|----------|-------------|
| `BOUND(?x)` | Is variable bound? |
| `ISURI(?x)` | Is a URI? |
| `ISBLANK(?x)` | Is blank node? |
| `ISLITERAL(?x)` | Is a literal? |
| `DATATYPE(?x)` | Get datatype URI |
| `LANG(?x)` | Get language tag |

## VALUES (inline data)

```sparql
SELECT ?person ?name WHERE {
  VALUES ?person { ex:Alice ex:Bob ex:Charlie }
  ?person ex:name ?name .
}
```

## SERVICE (federated query)

```sparql
SELECT ?label WHERE {
  SERVICE <http://dbpedia.org/sparql> {
    <http://dbpedia.org/resource/Berlin> rdfs:label ?label .
    FILTER (LANG(?label) = "en")
  }
}
```
