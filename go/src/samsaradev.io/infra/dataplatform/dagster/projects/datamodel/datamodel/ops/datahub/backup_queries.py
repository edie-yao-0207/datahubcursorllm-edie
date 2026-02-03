from typing import Dict, TypedDict


class Query(TypedDict):
    query: str
    variables: dict


backup_glossary_terms_query = """
query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    total
    searchResults {
      entity {
        urn
        type
        ... on GlossaryTerm {
          ownership {
            ...ownershipFields
          }
          properties {
            name
            description
          }
          institutionalMemory {
            elements {
              label
              url
            }
          }
          parentNodes {
            nodes {
              urn
            }
          }
        }
        ... on GlossaryNode {
          urn
          type
          properties {
            name
          }
          parentNodes {
            nodes {
              urn
            }
          }
        }
      }
    }
  }
}

fragment ownershipFields on Ownership {
  owners {
    owner {
      ... on CorpUser {
        urn
        type
        username
      }
      ... on CorpGroup {
        urn
        type
        name
        properties {
          displayName
          email
        }
      }
    }
    ownershipType {
      urn
      type
    }
  }
  lastModified {
    time
  }
}
"""

backup_glossary_terms_variables = {
    "input": {
        "query": "*",
        "start": 0,
        "count": 1000,
        "orFilters": [
            {
                "and": [
                    {
                        "field": "_entityType",
                        "condition": "EQUAL",
                        "values": ["GLOSSARY_TERM"],
                    }
                ]
            }
        ],
    }
}

backup_queries: Dict[str, Query] = {
    "glossary_terms": {
        "query": backup_glossary_terms_query,
        "variables": backup_glossary_terms_variables,
    }
}
