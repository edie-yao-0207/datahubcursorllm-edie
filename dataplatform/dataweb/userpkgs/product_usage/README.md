# Product Usage feature configuration

Hello! If you made it to this README, then chances are that you`re going to be either adding a new feature or editing the details on an existing feature for our Product Usage dashboard. For more details on what every single field represents, check out [this page](https://samsara.atlassian-us-gov-mod.net/wiki/spaces/DATA/pages/17850416/How+To+Adding+New+Features+To+Product+Usage). Check out the templates folder for some common patterns around enablement/usage (while the JSON will have everything, the files are appropriately named in their subfolders based on the functionality their enablement/usage provides). In case of questions, reach out to #ask-product-usage. #ask-data-engineering can help with code reviews when ready.

## Enablement

The enablement array is where you provide all the details that determine which customers have access to view a particular feature. Licenses, LaunchDarkly release flags, etc. are all generally relevant depending on how your feature is gated.

When making changes to an existing feature (as it progresses through different release phases or more licenses become available), please add a new entry to the array instead of editing the existing one. Our code will automatically take the latest date based on the date being run (for example, take the latest for anything up until 2025-09-21 when running for said day). This helps ensure everything is properly date-effective.

## Usage

The usage array is where you provide the relevant details (Cloud routes, Mixpanel events, etc.) for how we measure usage for a particular feature.

If providing a custom query, your query should return org and date at a minimum (if user ID is tracked, please add that to the query as well, as we count active users for stickiness). If a particular field should be counted as usage, please specify that in the usage_field. has_user_id_field is used to represent if the query has a user ID field, while filter_internal_usage is an additional option where we will filter out any actions connected to internal users.

If making changes to the usage definition for a feature (whether adding more Mixpanel events/Cloud routes or changing the definition entirely), add a new entry to the usage array. We look at usage over a 56 day span, so we will grab all relevant usage entries that correspond to the date range in question (if needed).
