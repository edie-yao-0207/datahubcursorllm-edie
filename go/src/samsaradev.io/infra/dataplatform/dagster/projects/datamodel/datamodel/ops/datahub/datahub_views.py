datahub_views = {
    "VDP Kinesis Stats": {
        "description": "Kinesis Stats owned by the VDP (Vehicle Data Platform) team",
        "definition": {
            "entityTypes": ["DATASET"],
            "filter": {
                "operator": "AND",
                "filters": [
                    {
                        "field": "owners",
                        "condition": "EQUAL",
                        "values": ["urn:li:corpGroup:firmwarevdp"],
                    },
                    {
                        "field": "domains",
                        "condition": "EQUAL",
                        "values": ["urn:li:domain:kinesis_stats"],
                    },
                ],
            },
        },
    },
    "Safety MPR metrics": {
        "description": "Metrics used in Monthly Product Review within the Safety pillar",
        "definition": {
            "entityTypes": ["DATASET"],
            "filter": {
                "operator": "AND",
                "filters": [
                    {
                        "field": "glossaryTerms",
                        "condition": "EQUAL",
                        "values": ["urn:li:glossaryTerm:glossary_term_mpr"],
                    },
                    {
                        "field": "glossaryTerms",
                        "condition": "EQUAL",
                        "values": ["urn:li:glossaryTerm:glossary_term_safety"],
                    },
                    {
                        "field": "domains",
                        "condition": "EQUAL",
                        "values": ["urn:li:domain:metrics_repo"],
                    },
                ],
            },
        },
    },
    "Platform MPR metrics": {
        "description": "Metrics used in Monthly Product Review within the Platform pillar",
        "definition": {
            "entityTypes": ["DATASET"],
            "filter": {
                "operator": "AND",
                "filters": [
                    {
                        "field": "glossaryTerms",
                        "condition": "EQUAL",
                        "values": ["urn:li:glossaryTerm:glossary_term_mpr"],
                    },
                    {
                        "field": "glossaryTerms",
                        "condition": "EQUAL",
                        "values": ["urn:li:glossaryTerm:glossary_term_platform"],
                    },
                    {
                        "field": "domains",
                        "condition": "EQUAL",
                        "values": ["urn:li:domain:metrics_repo"],
                    },
                ],
            },
        },
    },
    "Telematics MPR metrics": {
        "description": "Metrics used in Monthly Product Review within the Telematics pillar",
        "definition": {
            "entityTypes": ["DATASET"],
            "filter": {
                "operator": "AND",
                "filters": [
                    {
                        "field": "glossaryTerms",
                        "condition": "EQUAL",
                        "values": ["urn:li:glossaryTerm:glossary_term_mpr"],
                    },
                    {
                        "field": "glossaryTerms",
                        "condition": "EQUAL",
                        "values": ["urn:li:glossaryTerm:glossary_term_telematics"],
                    },
                    {
                        "field": "domains",
                        "condition": "EQUAL",
                        "values": ["urn:li:domain:metrics_repo"],
                    },
                ],
            },
        },
    },
    "Telematics Business Value Metrics": {
        "description": "Metrics within the Telematics pillar that are part of the Business Value metrics initiative",
        "definition": {
            "entityTypes": ["DATASET"],
            "filter": {
                "operator": "AND",
                "filters": [
                    {
                        "field": "glossaryTerms",
                        "condition": "EQUAL",
                        "values": ["urn:li:glossaryTerm:glossary_term_bvs"],
                    },
                    {
                        "field": "glossaryTerms",
                        "condition": "EQUAL",
                        "values": ["urn:li:glossaryTerm:glossary_term_telematics"],
                    },
                    {
                        "field": "domains",
                        "condition": "EQUAL",
                        "values": ["urn:li:domain:metrics_repo"],
                    },
                ],
            },
        },
    },
    "Safety & AI Metrics": {
        "description": "Metrics that are part of both the Safety and AI pillars",
        "definition": {
            "entityTypes": ["DATASET"],
            "filter": {
                "operator": "AND",
                "filters": [
                    {
                        "field": "glossaryTerms",
                        "condition": "EQUAL",
                        "values": ["urn:li:glossaryTerm:glossary_term_safety"],
                    },
                    {
                        "field": "glossaryTerms",
                        "condition": "EQUAL",
                        "values": ["urn:li:glossaryTerm:glossary_term_ai"],
                    },
                    {
                        "field": "domains",
                        "condition": "EQUAL",
                        "values": ["urn:li:domain:metrics_repo"],
                    },
                ],
            },
        },
    },
}
