datahub_search_query = """

query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
  searchAcrossEntities(input: $input) {
    ...searchResults
  }
}

fragment searchResults on SearchResults {
  start
  count
  total
  searchResults {
    entity {
      ...searchResultFields
    }
    matchedFields {
      name
      value
      entity {
        urn
        type
        ...entityDisplayNameFields
      }
    }
    insights {
      text
      icon
    }
  }
  facets {
    ...facetFields
  }
  suggestions {
    text
    frequency
    score
  }
}

fragment searchResultFields on Entity {
  urn
  type
  ... on Dataset {
    ...nonSiblingsDatasetSearchFields
    siblings {
      isPrimary
      siblings {
        urn
        type
        ... on Dataset {
          ...nonSiblingsDatasetSearchFields
        }
      }
    }
  }
  ... on Role {
    id
    properties {
      name
      description
    }
  }
  ... on CorpUser {
    username
    properties {
      active
      displayName
      title
      firstName
      lastName
      fullName
      email
    }
    info {
      active
      displayName
      title
      firstName
      lastName
      fullName
      email
    }
    editableProperties {
      displayName
      title
      pictureLink
    }
  }
  ... on CorpGroup {
    name
    info {
      displayName
      description
    }
    memberCount: relationships(
      input: {types: ["IsMemberOfGroup", "IsMemberOfNativeGroup"], direction: INCOMING, start: 0, count: 1}
    ) {
      total
    }
  }
  ... on Dashboard {
    dashboardId
    properties {
      name
      description
      externalUrl
      access
      lastModified {
        time
      }
    }
    ownership {
      ...ownershipFields
    }
    globalTags {
      ...globalTagsFields
    }
    glossaryTerms {
      ...glossaryTerms
    }
    editableProperties {
      description
    }
    platform {
      ...platformFields
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
    }
    domain {
      ...entityDomain
    }
    ...entityDataProduct
    deprecation {
      ...deprecationFields
    }
    parentContainers {
      ...parentContainersFields
    }
    statsSummary {
      viewCount
      uniqueUserCountLast30Days
      topUsersLast30Days {
        urn
        type
        username
        properties {
          displayName
          firstName
          lastName
          fullName
        }
        editableProperties {
          displayName
          pictureLink
        }
      }
    }
    subTypes {
      typeNames
    }
    health {
      ...entityHealth
    }
  }
  ... on Chart {
    chartId
    properties {
      name
      description
      externalUrl
      type
      access
      lastModified {
        time
      }
      created {
        time
      }
    }
    ownership {
      ...ownershipFields
    }
    globalTags {
      ...globalTagsFields
    }
    glossaryTerms {
      ...glossaryTerms
    }
    editableProperties {
      description
    }
    platform {
      ...platformFields
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
    }
    domain {
      ...entityDomain
    }
    ...entityDataProduct
    deprecation {
      ...deprecationFields
    }
    parentContainers {
      ...parentContainersFields
    }
    statsSummary {
      viewCount
      uniqueUserCountLast30Days
      topUsersLast30Days {
        urn
        type
        username
        properties {
          displayName
          firstName
          lastName
          fullName
        }
        editableProperties {
          displayName
          pictureLink
        }
      }
    }
    subTypes {
      typeNames
    }
    health {
      ...entityHealth
    }
  }
  ... on DataFlow {
    flowId
    cluster
    properties {
      name
      description
      project
      externalUrl
    }
    ownership {
      ...ownershipFields
    }
    globalTags {
      ...globalTagsFields
    }
    glossaryTerms {
      ...glossaryTerms
    }
    editableProperties {
      description
    }
    platform {
      ...platformFields
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
    }
    domain {
      ...entityDomain
    }
    ...entityDataProduct
    deprecation {
      ...deprecationFields
    }
    childJobs: relationships(
      input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 100}
    ) {
      total
    }
    health {
      ...entityHealth
    }
  }
  ... on DataJob {
    dataFlow {
      ...nonRecursiveDataFlowFields
    }
    jobId
    ownership {
      ...ownershipFields
    }
    properties {
      name
      description
      externalUrl
    }
    globalTags {
      ...globalTagsFields
    }
    glossaryTerms {
      ...glossaryTerms
    }
    editableProperties {
      description
    }
    domain {
      ...entityDomain
    }
    ...entityDataProduct
    deprecation {
      ...deprecationFields
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
    }
    subTypes {
      typeNames
    }
    lastRun: runs(start: 0, count: 1) {
      count
      start
      total
      runs {
        urn
        type
        created {
          time
          actor
        }
      }
    }
    health {
      ...entityHealth
    }
  }
  ... on GlossaryTerm {
    name
    hierarchicalName
    properties {
      name
      description
      termSource
      sourceRef
      sourceUrl
      rawSchema
      customProperties {
        key
        value
      }
    }
    deprecation {
      ...deprecationFields
    }
    parentNodes {
      ...parentNodesFields
    }
    domain {
      ...entityDomain
    }
  }
  ... on GlossaryNode {
    ...glossaryNode
    parentNodes {
      ...parentNodesFields
    }
  }
  ... on Domain {
    properties {
      name
      description
    }
    ownership {
      ...ownershipFields
    }
    parentDomains {
      ...parentDomainsFields
    }
    ...domainEntitiesFields
  }
  ... on Container {
    properties {
      name
      description
      externalUrl
    }
    platform {
      ...platformFields
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
    }
    editableProperties {
      description
    }
    ownership {
      ...ownershipFields
    }
    tags {
      ...globalTagsFields
    }
    glossaryTerms {
      ...glossaryTerms
    }
    subTypes {
      typeNames
    }
    entities(input: {}) {
      total
    }
    deprecation {
      ...deprecationFields
    }
    parentContainers {
      ...parentContainersFields
    }
  }
  ... on MLFeatureTable {
    name
    description
    featureTableProperties {
      description
      mlFeatures {
        urn
      }
      mlPrimaryKeys {
        urn
      }
    }
    ownership {
      ...ownershipFields
    }
    platform {
      ...platformFields
    }
    deprecation {
      ...deprecationFields
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
    }
  }
  ... on MLFeature {
    ...nonRecursiveMLFeature
  }
  ... on MLPrimaryKey {
    ...nonRecursiveMLPrimaryKey
  }
  ... on MLModel {
    name
    description
    origin
    ownership {
      ...ownershipFields
    }
    platform {
      ...platformFields
    }
    deprecation {
      ...deprecationFields
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
    }
  }
  ... on MLModelGroup {
    name
    origin
    description
    ownership {
      ...ownershipFields
    }
    platform {
      ...platformFields
    }
    deprecation {
      ...deprecationFields
    }
    dataPlatformInstance {
      ...dataPlatformInstanceFields
    }
  }
  ... on Tag {
    name
    properties {
      name
      colorHex
    }
    description
  }
  ... on DataPlatform {
    ...nonConflictingPlatformFields
  }
  ... on DataProduct {
    ...dataProductSearchFields
  }
}

fragment nonSiblingsDatasetSearchFields on Dataset {
  exists
  name
  origin
  uri
  platform {
    ...platformFields
  }
  dataPlatformInstance {
    ...dataPlatformInstanceFields
  }
  editableProperties {
    description
  }
  access {
    ...getAccess
  }
  platformNativeType
  properties {
    name
    description
    qualifiedName
    customProperties {
      key
      value
    }
    externalUrl
    lastModified {
      time
      actor
    }
  }
  ownership {
    ...ownershipFields
  }
  globalTags {
    ...globalTagsFields
  }
  glossaryTerms {
    ...glossaryTerms
  }
  subTypes {
    typeNames
  }
  domain {
    ...entityDomain
  }
  ...entityDataProduct
  parentContainers {
    ...parentContainersFields
  }
  deprecation {
    ...deprecationFields
  }
  health {
    ...entityHealth
  }
  ...datasetStatsFields
}

fragment platformFields on DataPlatform {
  urn
  type
  lastIngested
  name
  properties {
    type
    displayName
    datasetNameDelimiter
    logoUrl
  }
  displayName
  info {
    type
    displayName
    datasetNameDelimiter
    logoUrl
  }
}

fragment dataPlatformInstanceFields on DataPlatformInstance {
  urn
  type
  platform {
    ...platformFields
  }
  instanceId
}

fragment getAccess on Access {
  roles {
    role {
      ...getRolesName
    }
  }
}

fragment getRolesName on Role {
  urn
  type
  id
  properties {
    name
    description
    type
  }
}

fragment ownershipFields on Ownership {
  owners {
    owner {
      ... on CorpUser {
        urn
        type
        username
        info {
          active
          displayName
          title
          email
          firstName
          lastName
          fullName
        }
        properties {
          active
          displayName
          title
          email
          firstName
          lastName
          fullName
        }
        editableProperties {
          displayName
          title
          pictureLink
          email
        }
      }
      ... on CorpGroup {
        urn
        type
        name
        properties {
          displayName
          email
        }
        info {
          displayName
          email
          admins {
            urn
            username
            info {
              active
              displayName
              title
              email
              firstName
              lastName
              fullName
            }
            editableInfo {
              pictureLink
              teams
              skills
            }
          }
          members {
            urn
            username
            info {
              active
              displayName
              title
              email
              firstName
              lastName
              fullName
            }
            editableInfo {
              pictureLink
              teams
              skills
            }
          }
          groups
        }
      }
    }
    type
    ownershipType {
      urn
      type
      info {
        name
        description
      }
      status {
        removed
      }
    }
    associatedUrn
  }
  lastModified {
    time
  }
}

fragment globalTagsFields on GlobalTags {
  tags {
    tag {
      urn
      type
      name
      description
      properties {
        name
        colorHex
      }
    }
    associatedUrn
  }
}

fragment glossaryTerms on GlossaryTerms {
  terms {
    term {
      ...glossaryTerm
    }
    associatedUrn
  }
}

fragment glossaryTerm on GlossaryTerm {
  urn
  name
  type
  hierarchicalName
  properties {
    name
    description
    definition
    termSource
    customProperties {
      key
      value
    }
  }
  ownership {
    ...ownershipFields
  }
  parentNodes {
    ...parentNodesFields
  }
}

fragment parentNodesFields on ParentNodesResult {
  count
  nodes {
    urn
    type
    properties {
      name
    }
  }
}

fragment entityDomain on DomainAssociation {
  domain {
    urn
    type
    properties {
      name
      description
    }
    parentDomains {
      ...parentDomainsFields
    }
    ...domainEntitiesFields
  }
  associatedUrn
}

fragment parentDomainsFields on ParentDomainsResult {
  count
  domains {
    urn
    type
    ... on Domain {
      properties {
        name
        description
      }
    }
  }
}

fragment domainEntitiesFields on Domain {
  entities(input: {start: 0, count: 0}) {
    total
  }
  dataProducts: entities(
    input: {start: 0, count: 0, filters: [{field: "_entityType", value: "DATA_PRODUCT"}]}
  ) {
    total
  }
  children: relationships(
    input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 0}
  ) {
    total
  }
}

fragment entityDataProduct on Entity {
  dataProduct: relationships(
    input: {types: ["DataProductContains"], direction: INCOMING, start: 0, count: 1}
  ) {
    relationships {
      type
      entity {
        urn
        type
        ... on DataProduct {
          properties {
            name
            description
          }
          domain {
            ...entityDomain
          }
        }
      }
    }
  }
}

fragment parentContainersFields on ParentContainersResult {
  count
  containers {
    ...parentContainerFields
  }
}

fragment parentContainerFields on Container {
  urn
  properties {
    name
  }
  subTypes {
    typeNames
  }
}

fragment deprecationFields on Deprecation {
  actor
  deprecated
  note
  decommissionTime
}

fragment entityHealth on Health {
  type
  status
  message
  causes
}

fragment datasetStatsFields on Dataset {
  lastProfile: datasetProfiles(limit: 1) {
    rowCount
    columnCount
    sizeInBytes
    timestampMillis
  }
  lastOperation: operations(limit: 1) {
    lastUpdatedTimestamp
    timestampMillis
  }
  statsSummary {
    queryCountLast30Days
    uniqueUserCountLast30Days
    topUsersLast30Days {
      urn
      type
      username
      properties {
        displayName
        firstName
        lastName
        fullName
      }
      editableProperties {
        displayName
        pictureLink
      }
    }
  }
}

fragment nonRecursiveDataFlowFields on DataFlow {
  urn
  type
  orchestrator
  flowId
  cluster
  properties {
    name
    description
    project
    externalUrl
    customProperties {
      key
      value
    }
  }
  editableProperties {
    description
  }
  ownership {
    ...ownershipFields
  }
  platform {
    ...platformFields
  }
  domain {
    ...entityDomain
  }
  ...entityDataProduct
  deprecation {
    ...deprecationFields
  }
}

fragment glossaryNode on GlossaryNode {
  urn
  type
  properties {
    name
  }
  children: relationships(
    input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 10000}
  ) {
    total
  }
}

fragment nonRecursiveMLFeature on MLFeature {
  urn
  type
  exists
  lastIngested
  name
  featureNamespace
  description
  dataType
  properties {
    description
    dataType
    version {
      versionTag
    }
    sources {
      urn
      name
      type
      origin
      description
      uri
      platform {
        ...platformFields
      }
      platformNativeType
    }
  }
  ownership {
    ...ownershipFields
  }
  institutionalMemory {
    ...institutionalMemoryFields
  }
  status {
    removed
  }
  glossaryTerms {
    ...glossaryTerms
  }
  domain {
    ...entityDomain
  }
  ...entityDataProduct
  tags {
    ...globalTagsFields
  }
  editableProperties {
    description
  }
  deprecation {
    ...deprecationFields
  }
  dataPlatformInstance {
    ...dataPlatformInstanceFields
  }
  browsePathV2 {
    ...browsePathV2Fields
  }
  featureTables: relationships(
    input: {types: ["Contains"], direction: INCOMING, start: 0, count: 100}
  ) {
    relationships {
      type
      entity {
        ... on MLFeatureTable {
          platform {
            ...platformFields
          }
        }
      }
    }
  }
}

fragment institutionalMemoryFields on InstitutionalMemory {
  elements {
    url
    author {
      urn
      username
    }
    description
    created {
      actor
      time
    }
    associatedUrn
  }
}

fragment browsePathV2Fields on BrowsePathV2 {
  path {
    name
    entity {
      urn
      type
      ... on Container {
        properties {
          name
        }
      }
      ... on DataFlow {
        properties {
          name
        }
      }
      ... on DataPlatformInstance {
        platform {
          name
          properties {
            displayName
          }
        }
        instanceId
      }
    }
  }
}

fragment nonRecursiveMLPrimaryKey on MLPrimaryKey {
  urn
  type
  exists
  lastIngested
  name
  featureNamespace
  description
  dataType
  properties {
    description
    dataType
    version {
      versionTag
    }
    sources {
      urn
      name
      type
      origin
      description
      uri
      platform {
        ...platformFields
      }
      platformNativeType
    }
  }
  ownership {
    ...ownershipFields
  }
  institutionalMemory {
    ...institutionalMemoryFields
  }
  status {
    removed
  }
  glossaryTerms {
    ...glossaryTerms
  }
  domain {
    ...entityDomain
  }
  ...entityDataProduct
  tags {
    ...globalTagsFields
  }
  editableProperties {
    description
  }
  deprecation {
    ...deprecationFields
  }
  dataPlatformInstance {
    ...dataPlatformInstanceFields
  }
  featureTables: relationships(
    input: {types: ["KeyedBy"], direction: INCOMING, start: 0, count: 100}
  ) {
    relationships {
      type
      entity {
        ... on MLFeatureTable {
          platform {
            ...platformFields
          }
        }
      }
    }
  }
}

fragment nonConflictingPlatformFields on DataPlatform {
  urn
  type
  name
  properties {
    displayName
    datasetNameDelimiter
    logoUrl
  }
  displayName
  info {
    type
    displayName
    datasetNameDelimiter
    logoUrl
  }
}

fragment dataProductSearchFields on DataProduct {
  urn
  type
  properties {
    name
    description
    externalUrl
  }
  ownership {
    ...ownershipFields
  }
  tags {
    ...globalTagsFields
  }
  glossaryTerms {
    ...glossaryTerms
  }
  domain {
    ...entityDomain
  }
  entities(input: {start: 0, count: 0, query: "*"}) {
    total
  }
}

fragment entityDisplayNameFields on Entity {
  urn
  type
  ... on Dataset {
    name
    properties {
      name
      qualifiedName
    }
  }
  ... on CorpUser {
    username
    properties {
      displayName
      title
      firstName
      lastName
      fullName
      email
    }
    info {
      active
      displayName
      title
      firstName
      lastName
      fullName
      email
    }
  }
  ... on CorpGroup {
    name
    info {
      displayName
    }
  }
  ... on Dashboard {
    dashboardId
    properties {
      name
    }
  }
  ... on Chart {
    chartId
    properties {
      name
    }
  }
  ... on DataFlow {
    properties {
      name
    }
  }
  ... on DataJob {
    jobId
    properties {
      name
    }
  }
  ... on GlossaryTerm {
    name
    hierarchicalName
    properties {
      name
    }
  }
  ... on GlossaryNode {
    properties {
      name
    }
  }
  ... on Domain {
    properties {
      name
    }
  }
  ... on Container {
    properties {
      name
    }
  }
  ... on MLFeatureTable {
    name
  }
  ... on MLFeature {
    name
  }
  ... on MLPrimaryKey {
    name
  }
  ... on MLModel {
    name
  }
  ... on MLModelGroup {
    name
  }
  ... on Tag {
    name
    properties {
      name
      colorHex
    }
  }
  ... on DataPlatform {
    ...nonConflictingPlatformFields
  }
  ... on DataProduct {
    properties {
      name
    }
  }
  ... on DataPlatformInstance {
    instanceId
  }
}

fragment facetFields on FacetMetadata {
  field
  displayName
  aggregations {
    value
    count
    entity {
      urn
      type
      ... on Tag {
        name
        properties {
          name
          colorHex
        }
      }
      ... on GlossaryTerm {
        name
        properties {
          name
        }
        parentNodes {
          ...parentNodesFields
        }
      }
      ... on DataPlatform {
        ...platformFields
      }
      ... on DataPlatformInstance {
        ...dataPlatformInstanceFields
      }
      ... on Domain {
        properties {
          name
        }
        parentDomains {
          ...parentDomainsFields
        }
      }
      ... on Container {
        platform {
          ...platformFields
        }
        properties {
          name
        }
      }
      ... on CorpUser {
        username
        properties {
          displayName
          fullName
        }
        editableProperties {
          displayName
          pictureLink
        }
      }
      ... on CorpGroup {
        name
        properties {
          displayName
        }
      }
      ... on DataProduct {
        properties {
          name
        }
      }
    }
  }
}
"""
