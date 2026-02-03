CREATE OR REPLACE VIEW mdm.amapi_pubsub_device_messages AS (
  SELECT
       *,
       -- This schema was generated using sparktypes package (infra/dataplatform/sparktypes) JsonTypeToSparkType using an androidmanagement.Device object
       -- The JSON API structure can be found in the docs --> https://developers.google.com/android/management/reference/rest/v1/enterprises.devices
       from_json(STRING(unbase64(message)),
'STRUCT<
  `apiLevel`: BIGINT,
  `applicationReports`: ARRAY<
    STRUCT<
      `applicationSource`: STRING,
      `displayName`: STRING,
      `events`: ARRAY<
        STRUCT<
          `createTime`: STRING,
          `eventType`: STRING
        >
      >,
      `installerPackageName`: STRING,
      `keyedAppStates`: ARRAY<
        STRUCT<
          `createTime`: STRING,
          `data`: STRING,
          `key`: STRING,
          `lastUpdateTime`: STRING,
          `message`: STRING,
          `severity`: STRING
        >
      >,
      `packageName`: STRING,
      `packageSha256Hash`: STRING,
      `signingKeyCertFingerprints`: ARRAY<STRING>,
      `state`: STRING,
      `userFacingType`: STRING,
      `versionCode`: BIGINT,
      `versionName`: STRING
    >
  >,
  `appliedPasswordPolicies`: ARRAY<
    STRUCT<
      `maximumFailedPasswordsForWipe`: BIGINT,
      `passwordExpirationTimeout`: STRING,
      `passwordHistoryLength`: BIGINT,
      `passwordMinimumLength`: BIGINT,
      `passwordMinimumLetters`: BIGINT,
      `passwordMinimumLowerCase`: BIGINT,
      `passwordMinimumNonLetter`: BIGINT,
      `passwordMinimumNumeric`: BIGINT,
      `passwordMinimumSymbols`: BIGINT,
      `passwordMinimumUpperCase`: BIGINT,
      `passwordQuality`: STRING,
      `passwordScope`: STRING,
      `requirePasswordUnlock`: STRING,
      `unifiedLockSettings`: STRING
    >
  >,
  `appliedPolicyName`: STRING,
  `appliedPolicyVersion`: BIGINT,
  `appliedState`: STRING,
  `commonCriteriaModeInfo`: STRUCT<`commonCriteriaModeStatus`: STRING>,
  `deviceSettings`: STRUCT<
    `adbEnabled`: BOOLEAN,
    `developmentSettingsEnabled`: BOOLEAN,
    `encryptionStatus`: STRING,
    `isDeviceSecure`: BOOLEAN,
    `isEncrypted`: BOOLEAN,
    `unknownSourcesEnabled`: BOOLEAN,
    `verifyAppsEnabled`: BOOLEAN
  >,
  `disabledReason`: STRUCT<
    `defaultMessage`: STRING,
    `localizedMessages`: MAP<STRING, STRING>
  >,
  `displays`: ARRAY<
    STRUCT<
      `density`: BIGINT,
      `displayId`: BIGINT,
      `height`: BIGINT,
      `name`: STRING,
      `refreshRate`: BIGINT,
      `state`: STRING,
      `width`: BIGINT
    >
  >,
  `enrollmentTime`: STRING,
  `enrollmentTokenData`: STRING,
  `enrollmentTokenName`: STRING,
  `hardwareInfo`: STRUCT<
    `batteryShutdownTemperatures`: ARRAY<DOUBLE>,
    `batteryThrottlingTemperatures`: ARRAY<DOUBLE>,
    `brand`: STRING,
    `cpuShutdownTemperatures`: ARRAY<DOUBLE>,
    `cpuThrottlingTemperatures`: ARRAY<DOUBLE>,
    `deviceBasebandVersion`: STRING,
    `enterpriseSpecificId`: STRING,
    `gpuShutdownTemperatures`: ARRAY<DOUBLE>,
    `gpuThrottlingTemperatures`: ARRAY<DOUBLE>,
    `hardware`: STRING,
    `manufacturer`: STRING,
    `model`: STRING,
    `serialNumber`: STRING,
    `skinShutdownTemperatures`: ARRAY<DOUBLE>,
    `skinThrottlingTemperatures`: ARRAY<DOUBLE>
  >,
  `hardwareStatusSamples`: ARRAY<
    STRUCT<
      `batteryTemperatures`: ARRAY<DOUBLE>,
      `cpuTemperatures`: ARRAY<DOUBLE>,
      `cpuUsages`: ARRAY<DOUBLE>,
      `createTime`: STRING,
      `fanSpeeds`: ARRAY<DOUBLE>,
      `gpuTemperatures`: ARRAY<DOUBLE>,
      `skinTemperatures`: ARRAY<DOUBLE>
    >
  >,
  `lastPolicyComplianceReportTime`: STRING,
  `lastPolicySyncTime`: STRING,
  `lastStatusReportTime`: STRING,
  `managementMode`: STRING,
  `memoryEvents`: ARRAY<
    STRUCT<
      `byteCount`: BIGINT,
      `createTime`: STRING,
      `eventType`: STRING
    >
  >,
  `memoryInfo`: STRUCT<
    `totalInternalStorage`: BIGINT,
    `totalRam`: BIGINT
  >,
  `name`: STRING,
  `networkInfo`: STRUCT<
    `imei`: STRING,
    `meid`: STRING,
    `networkOperatorName`: STRING,
    `telephonyInfos`: ARRAY<
      STRUCT<
        `carrierName`: STRING,
        `phoneNumber`: STRING
      >
    >,
    `wifiMacAddress`: STRING
  >,
  `nonComplianceDetails`: ARRAY<
    STRUCT<
      `fieldPath`: STRING,
      `installationFailureReason`: STRING,
      `nonComplianceReason`: STRING,
      `packageName`: STRING,
      `settingName`: STRING,
      `specificNonComplianceContext`: STRUCT<
        `oncWifiContext`: STRUCT<`wifiGuid`: STRING>,
        `passwordPoliciesContext`: STRUCT<`passwordPolicyScope`: STRING>
      >,
      `specificNonComplianceReason`: STRING
    >
  >,
  `ownership`: STRING,
  `policyCompliant`: BOOLEAN,
  `policyName`: STRING,
  `powerManagementEvents`: ARRAY<
    STRUCT<
      `batteryLevel`: DOUBLE,
      `createTime`: STRING,
      `eventType`: STRING
    >
  >,
  `previousDeviceNames`: ARRAY<STRING>,
  `securityPosture`: STRUCT<
    `devicePosture`: STRING,
    `postureDetails`: ARRAY<
      STRUCT<
        `advice`: ARRAY<
          STRUCT<
            `defaultMessage`: STRING,
            `localizedMessages`: MAP<STRING, STRING>
          >
        >,
        `securityRisk`: STRING
      >
    >
  >,
  `softwareInfo`: STRUCT<
    `androidBuildNumber`: STRING,
    `androidBuildTime`: STRING,
    `androidDevicePolicyVersionCode`: BIGINT,
    `androidDevicePolicyVersionName`: STRING,
    `androidVersion`: STRING,
    `bootloaderVersion`: STRING,
    `deviceBuildSignature`: STRING,
    `deviceKernelVersion`: STRING,
    `primaryLanguageCode`: STRING,
    `securityPatchLevel`: STRING,
    `systemUpdateInfo`: STRUCT<
      `updateReceivedTime`: STRING,
      `updateStatus`: STRING
    >
  >,
  `state`: STRING,
  `systemProperties`: MAP<STRING, STRING>,
  `user`: STRUCT<`accountIdentifier`: STRING>,
  `userName`: STRING
>'
       ) as device
  FROM datastreams.amapi_pubsub_messages
  WHERE notification_type == "STATUS_REPORT"
  OR notification_type == "ENROLLMENT"
)
