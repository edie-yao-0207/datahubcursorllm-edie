# Databricks notebook source
# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Load Tap Code Mappings Provided By AT&T

# COMMAND ----------

from pyspark.sql.functions import explode, split, col, trim

# Get TADIG Code (name for tap_code in CSV) and map to Country in Dataframe. Remove whitespace by calling trim
df = (
    spark.sql("select * from definitions.att_tap_codes")
    .select(
        explode(split("TADIG Code", ",")).alias("tap_code"),
        col("Country").alias("country"),
    )
    .withColumn("tap_code", trim(col("tap_code")))
)
# Convert dataframe to dictionary: Tap Code -> Country
tap_code_to_country = {row["tap_code"]: row["country"] for row in df.collect()}

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Constants

# COMMAND ----------

NORAM = "NorAm"
GROUP_A = "GroupA"
EUROPE = "Europe"
OUTSIDE_GROUP_A = "OutsideGroupA"

# As per Jasper: 1 MB = 1,048,576 bytes
SIZEOFMB = 1048576
NUMBYTESINGIGABYTE = 1074000000

# Roaming Cutoff (40 MB)
# Increase size of international pool for every device added (100 MB)
# Defined in PRD: https://paper.dropbox.com/doc/PRD-ATT-Backend-Cellular-Optimization-System--A1hUqI3rHjs6B6AAYUylezXFAg-2vFYQhcD5iKTbHYr5ZJ2T#:uid=872768827528865718030009&h2=Logic-Flow
ROAMING_CUTOFF = 40 * SIZEOFMB
POOL_INCREASE_PER_DEVICE = 100 * SIZEOFMB

# Country constants
# These are extracted from the tap_code reference given to us by AT&T:
# https://docs.google.com/spreadsheets/d/1-GHTggr5nqDl7DqOH2ANIzZp9WXs2dXMUAy8y_HKkQA/edit?usp=sharing
AFGHANISTAN = "Afghanistan"
ALBANIA = "Albania"
ALGERIA = "Algeria"
AMERICAN_SAMOA = "American Samoa"
ANDORRA = "Andorra"
ANGOLA = "Angola"
ANGUILLA = "Anguilla"
ANTARCTICA = "Antarctica"
ANTIGUA_AND_BARBUDA = "Antigua and Barbuda"
ARGENTINA = "Argentina"
ARMENIA = "Armenia"
ARUBA = "Aruba"
ASCENSION_ISLAND = "Ascension Island"
AUSTRALIA = "Australia"
AUSTRIA = "Austria"
AZERBAIDJAN = "Azerbaidjan"
BAHAMAS = "Bahamas"
BAHRAIN = "Bahrain"
BANGLADESH = "Bangladesh"
BARBADOS = "Barbados"
BELARUS = "Belarus"
BELGIUM = "Belgium"
BELIZE = "Belize"
BENIN = "Benin"
BERMUDA = "Bermuda"
BHUTAN = "Bhutan"
BOLIVIA = "Bolivia"
BOSNIA_AND_HERZEGOVINA = "Bosnia and Herzegovina"
BOTSWANA = "Botswana"
BRAZIL = "Brazil"
BRUNEI_DARUSSALAM = "Brunei Darussalam"
BULGARIA = "Bulgaria"
BURKINA_FASO = "Burkina Faso"
BURUNDI = "Burundi"
CAMBODIA = "Cambodia"
CAMEROON = "Cameroon"
CANADA = "Canada"
CAPE_VERDE = "Cape Verde"
CAYMAN_ISLANDS = "Cayman Islands"
CENTRAL_AFRICAN_REPUBLIC = "Central African Republic"
CHAD = "Chad"
CHILE = "Chile"
CHINA = "China"
COLOMBIA = "Colombia"
COMORO = "Comoro"
CONGO = "Congo"
CONGO_THE_DEMOCRATIC_REPUBLIC_OF_THE = "Congo, The Democratic Republic of the"
COOK_ISLANDS = "Cook Islands"
COSTA_RICA = "Costa Rica"
CROATIA = "Croatia"
CUBA = "Cuba"
CYPRUS = "Cyprus"
CZECH_REPUBLIC = "Czech Republic"
DENMARK = "Denmark"
DJIBOUTI = "Djibouti"
DOMINICA = "Dominica"
DOMINICAN_REPUBLIC = "Dominican Republic"
EAST_TIMOR = "East Timor"
ECUADOR = "Ecuador"
EGYPT = "Egypt"
EL_SALVADOR = "El Salvador"
EQUATORIAL_GUINEA = "Equatorial Guinea"
ERITREA = "Eritrea"
ESTONIA = "Estonia"
ETHIOPIA = "Ethiopia"
FALKLAND_ISLANDS = "Falkland Islands"
FAROE_ISLANDS = "Faroe Islands"
FIJI = "Fiji"
FINLAND = "Finland"
FRANCE = "France"
FRENCH_GUIANA = "French Guiana"
FRENCH_SOUTHERN_TERRITORIES = "French Southern Territories"
GABON = "Gabon"
GAMBIA = "Gambia"
GEORGIA = "Georgia"
GERMANY = "Germany"
GHANA = "Ghana"
GIBRALTAR = "Gibraltar"
GREECE = "Greece"
GREENLAND = "Greenland"
GRENADA = "Grenada"
GUADELOUPE = "Guadeloupe"
GUAM = "Guam"
GUATEMALA = "Guatemala"
GUERNSEY = "Guernsey"
GUINEA = "Guinea"
GUINEA_BISSAU = "Guinea Bissau"
GUYANA = "Guyana"
HAITI = "Haiti"
HONDURAS = "Honduras"
HONG_KONG = "Hong Kong"
HUNGARY = "Hungary"
ICELAND = "Iceland"
INDIA = "India"
INDONESIA = "Indonesia"
IRAN = "Iran"
IRAQ = "Iraq"
IRELAND = "Ireland"
ISLE_OF_MAN = "Isle of Man"
ISRAEL = "Israel"
ITALY = "Italy"
IVORY_COAST = "Ivory Coast"
JAMAICA = "Jamaica"
JAPAN = "Japan"
JERSEY = "Jersey"
JORDAN = "Jordan"
KAZAKHSTAN = "Kazakhstan"
KENYA = "Kenya"
KIRIBATI = "Kiribati"
KOREA_SOUTH = "Korea (South)"
KOSOVO = "Kosovo"
KUWAIT = "Kuwait"
KYRGYZSTAN = "Kyrgyzstan"
LAOS = "Laos"
LATVIA = "Latvia"
LEBANON = "Lebanon"
LESOTHO = "Lesotho"
LIBERIA = "Liberia"
LIBYA = "Libya"
LIECHTENSTEIN = "Liechtenstein"
LITHUANIA = "Lithuania"
LUXEMBOURG = "Luxembourg"
MACAU = "Macau"
MACEDONIA = "Macedonia"
MADAGASCAR = "Madagascar"
MALAWI = "Malawi"
MALAYSIA = "Malaysia"
MALDIVES = "Maldives"
MALI = "Mali"
MALTA = "Malta"
MARSHALL_ISLANDS = "Marshall Islands"
MAURITANIA = "Mauritania"
MAURITIUS = "Mauritius"
MEXICO = "Mexico"
MICRONESIA = "Micronesia"
MOLDAVIA = "Moldavia"
MONACO = "Monaco"
MONGOLIA = "Mongolia"
MONTENEGRO = "Montenegro"
MONTSERRAT = "Montserrat"
MOROCCO = "Morocco"
MOZAMBIQUE = "Mozambique"
MYANMAR = "Myanmar"
NAMIBIA = "Namibia"
NEPAL = "Nepal"
NETHERLANDS = "Netherlands"
NETHERLANDS_ANTILLES = "Netherlands Antilles"
NEW_CALEDONIA = "New Caledonia"
NEW_ZEALAND = "New Zealand"
NICARAGUA = "Nicaragua"
NIGER = "Niger"
NIGERIA = "Nigeria"
NORFOLK_ISLAND = "Norfolk Island"
NORTHERN_MARIANA_ISLANDS = "Northern Mariana Islands"
NORWAY = "Norway"
OMAN = "Oman"
PAKISTAN = "Pakistan"
PALAU = "Palau"
PALESTINIAN_TERRITORY = "Palestinian Territory"
PANAMA = "Panama"
PAPUA_NEW_GUINEA = "Papua New Guinea"
PARAGUAY = "Paraguay"
PERU = "Peru"
PHILIPPINES = "Philippines"
POLAND = "Poland"
POLYNESIA_FRENCH = "Polynesia (French)"
PORTUGAL = "Portugal"
PUERTO_RICO = "Puerto Rico"
QATAR = "Qatar"
REUNION = "Reunion"
ROMANIA = "Romania"
RUSSIAN_FEDERATION = "Russian Federation"
RWANDA = "Rwanda"
SAINT_KITTS_AND_NEVIS = "Saint Kitts and Nevis"
SAINT_LUCIA = "Saint Lucia"
SAINT_PIERRE_AND_MIQUELON = "Saint Pierre and Miquelon"
SAINT_TOME_AND_PRINCIPE = "Saint Tome and Principe"
SAINT_VINCENT_AND_THE_GRENADINES = "Saint Vincent and the Grenadines"
SAMOA = "Samoa"
SAN_MARINO = "San Marino"
SAUDI_ARABIA = "Saudi Arabia"
SENEGAL = "Senegal"
SERBIA_AND_MONTENEGRO = "Serbia and Montenegro"
SEYCHELLES = "Seychelles"
SIERRA_LEONE = "Sierra Leone"
SINGAPORE = "Singapore"
SLOVAK_REPUBLIC = "Slovak Republic"
SLOVENIA = "Slovenia"
SOLOMON_ISLANDS = "Solomon Islands"
SOMALIA = "Somalia"
SOUTH_AFRICA = "South Africa"
SOUTH_SUDAN = "South Sudan"
SPAIN = "Spain"
SRI_LANKA = "Sri Lanka"
SUDAN = "Sudan"
SURINAME = "Suriname"
SWAZILAND = "Swaziland"
SWEDEN = "Sweden"
SWITZERLAND = "Switzerland"
SYRIA = "Syria"
TADJIKISTAN = "Tadjikistan"
TAIWAN = "Taiwan"
TANZANIA = "Tanzania"
THAILAND = "Thailand"
TOGO = "Togo"
TONGA = "Tonga"
TRINIDAD_AND_TOBAGO = "Trinidad and Tobago"
TUNISIA = "Tunisia"
TURKEY = "Turkey"
TURKMENISTAN = "Turkmenistan"
TURKS_AND_CAICOS_ISLANDS = "Turks and Caicos Islands"
UGANDA = "Uganda"
UKRAINE = "Ukraine"
UNITED_ARAB_EMIRATES = "United Arab Emirates"
UNITED_KINGDOM = "United Kingdom"
UNITED_STATES = "United States"
URUGUAY = "Uruguay"
UZBEKISTAN = "Uzbekistan"
VANUATU = "Vanuatu"
VENEZUELA = "Venezuela"
VIETNAM = "Vietnam"
VIRGIN_ISLANDS_BRITISH = "Virgin Islands (British)"
YEMEN = "Yemen"
ZAMBIA = "Zambia"
ZIMBABWE = "Zimbabwe"

north_america_locales = {UNITED_STATES, MEXICO, CANADA}

schedule_b5_locales = {
    ALBANIA,
    AUSTRIA,
    BELGIUM,
    BULGARIA,
    CROATIA,
    CYPRUS,
    CZECH_REPUBLIC,
    DENMARK,
    ESTONIA,
    FINLAND,
    FRANCE,
    GERMANY,
    GREECE,
    HUNGARY,
    ICELAND,
    IRELAND,
    ITALY,
    LATVIA,
    LIECHTENSTEIN,
    LITHUANIA,
    LUXEMBOURG,
    MACEDONIA,
    MALTA,
    MONTENEGRO,
    NETHERLANDS,
    NORWAY,
    POLAND,
    PORTUGAL,
    ROMANIA,
    SERBIA_AND_MONTENEGRO,
    SLOVAK_REPUBLIC,
    SPAIN,
    SWEDEN,
    SWITZERLAND,
    UNITED_KINGDOM,
    UNITED_STATES,
}

group_a_locales = {
    AFGHANISTAN,
    ALBANIA,
    ANGUILLA,
    ANTIGUA_AND_BARBUDA,
    ARGENTINA,
    ARMENIA,
    ARUBA,
    AUSTRALIA,
    AUSTRIA,
    AZERBAIDJAN,
    BAHAMAS,
    BAHRAIN,
    BANGLADESH,
    BARBADOS,
    BELARUS,
    BELGIUM,
    BELIZE,
    BERMUDA,
    BOLIVIA,
    BOSNIA_AND_HERZEGOVINA,
    BRAZIL,
    VIRGIN_ISLANDS_BRITISH,
    BULGARIA,
    BURKINA_FASO,
    CAMBODIA,
    CAMEROON,
    CANADA,
    CAYMAN_ISLANDS,
    CHAD,
    CHILE,
    CHINA,
    COLOMBIA,
    CONGO_THE_DEMOCRATIC_REPUBLIC_OF_THE,
    COSTA_RICA,
    IVORY_COAST,
    CROATIA,
    CYPRUS,
    CZECH_REPUBLIC,
    DENMARK,
    DOMINICA,
    DOMINICAN_REPUBLIC,
    NETHERLANDS_ANTILLES,
    ECUADOR,
    EGYPT,
    EL_SALVADOR,
    EQUATORIAL_GUINEA,
    ESTONIA,
    FAROE_ISLANDS,
    FIJI,
    FINLAND,
    FRANCE,
    GABON,
    GEORGIA,
    GERMANY,
    GHANA,
    UNITED_KINGDOM,
    GREECE,
    GREENLAND,
    GRENADA,
    GUINEA,
    GUYANA,
    HAITI,
    HONDURAS,
    HONG_KONG,
    HUNGARY,
    ICELAND,
    INDIA,
    INDONESIA,
    IRAQ,
    IRELAND,
    ISRAEL,
    ITALY,
    JAMAICA,
    JAPAN,
    JORDAN,
    KAZAKHSTAN,
    KENYA,
    KUWAIT,
    KYRGYZSTAN,
    LAOS,
    LATVIA,
    LESOTHO,
    LIECHTENSTEIN,
    LITHUANIA,
    LUXEMBOURG,
    MACAU,
    MACEDONIA,
    MALAWI,
    MALAYSIA,
    MALI,
    MALTA,
    MEXICO,
    MONGOLIA,
    MONTENEGRO,
    MONTSERRAT,
    MOROCCO,
    MOZAMBIQUE,
    NEPAL,
    NETHERLANDS,
    NEW_ZEALAND,
    NICARAGUA,
    NIGER,
    NIGERIA,
    NORWAY,
    OMAN,
    PAKISTAN,
    PALESTINIAN_TERRITORY,
    PANAMA,
    PARAGUAY,
    PERU,
    PHILIPPINES,
    POLAND,
    PORTUGAL,
    QATAR,
    REUNION,
    ROMANIA,
    RUSSIAN_FEDERATION,
    RWANDA,
    SAUDI_ARABIA,
    SERBIA_AND_MONTENEGRO,
    SEYCHELLES,
    SIERRA_LEONE,
    SINGAPORE,
    SLOVAK_REPUBLIC,
    SLOVENIA,
    SOUTH_AFRICA,
    KOREA_SOUTH,
    SPAIN,
    SRI_LANKA,
    SAINT_KITTS_AND_NEVIS,
    SAINT_LUCIA,
    SAINT_VINCENT_AND_THE_GRENADINES,
    SURINAME,
    SWEDEN,
    SWITZERLAND,
    TAIWAN,
    TADJIKISTAN,
    TANZANIA,
    THAILAND,
    TOGO,
    TRINIDAD_AND_TOBAGO,
    TURKEY,
    TURKS_AND_CAICOS_ISLANDS,
    UNITED_ARAB_EMIRATES,
    UGANDA,
    UKRAINE,
    UNITED_STATES,
    UZBEKISTAN,
    VENEZUELA,
    ZAMBIA,
    ZIMBABWE,
}

# Country constant --> country string mapping

countryEnumToString = {
    AFGHANISTAN: "Afghanistan",
    ALBANIA: "Albania",
    ALGERIA: "Algeria",
    AMERICAN_SAMOA: "American Samoa",
    ANDORRA: "Andorra",
    ANGOLA: "Angola",
    ANGUILLA: "Anguilla",
    ANTARCTICA: "Antarctica",
    ANTIGUA_AND_BARBUDA: "Antigua and Barbuda",
    ARGENTINA: "Argentina",
    ARMENIA: "Armenia",
    ARUBA: "Aruba",
    ASCENSION_ISLAND: "Ascension Island",
    AUSTRALIA: "Australia",
    AUSTRIA: "Austria",
    AZERBAIDJAN: "Azerbaidjan",
    BAHAMAS: "Bahamas",
    BAHRAIN: "Bahrain",
    BANGLADESH: "Bangladesh",
    BARBADOS: "Barbados",
    BELARUS: "Belarus",
    BELGIUM: "Belgium",
    BELIZE: "Belize",
    BENIN: "Benin",
    BERMUDA: "Bermuda",
    BHUTAN: "Bhutan",
    BOLIVIA: "Bolivia",
    BOSNIA_AND_HERZEGOVINA: "Bosnia and Herzegovina",
    BOTSWANA: "Botswana",
    BRAZIL: "Brazil",
    BRUNEI_DARUSSALAM: "Brunei Darussalam",
    BULGARIA: "Bulgaria",
    BURKINA_FASO: "Burkina Faso",
    BURUNDI: "Burundi",
    CAMBODIA: "Cambodia",
    CAMEROON: "Cameroon",
    CANADA: "Canada",
    CAPE_VERDE: "Cape Verde",
    CAYMAN_ISLANDS: "Cayman Islands",
    CENTRAL_AFRICAN_REPUBLIC: "Central African Republic",
    CHAD: "Chad",
    CHILE: "Chile",
    CHINA: "China",
    COLOMBIA: "Colombia",
    COMORO: "Comoro",
    CONGO: "Congo",
    CONGO_THE_DEMOCRATIC_REPUBLIC_OF_THE: "Congo, The Democratic Republic of the",
    COOK_ISLANDS: "Cook Islands",
    COSTA_RICA: "Costa Rica",
    CROATIA: "Croatia",
    CUBA: "Cuba",
    CYPRUS: "Cyprus",
    CZECH_REPUBLIC: "Czech Republic",
    DENMARK: "Denmark",
    DJIBOUTI: "Djibouti",
    DOMINICA: "Dominica",
    DOMINICAN_REPUBLIC: "Dominican Republic",
    EAST_TIMOR: "East Timor",
    ECUADOR: "Ecuador",
    EGYPT: "Egypt",
    EL_SALVADOR: "El Salvador",
    EQUATORIAL_GUINEA: "Equatorial Guinea",
    ERITREA: "Eritrea",
    ESTONIA: "Estonia",
    ETHIOPIA: "Ethiopia",
    FALKLAND_ISLANDS: "Falkland Islands",
    FAROE_ISLANDS: "Faroe Islands",
    FIJI: "Fiji",
    FINLAND: "Finland",
    FRANCE: "France",
    FRENCH_GUIANA: "French Guiana",
    FRENCH_SOUTHERN_TERRITORIES: "French Southern Territories",
    GABON: "Gabon",
    GAMBIA: "Gambia",
    GEORGIA: "Georgia",
    GERMANY: "Germany",
    GHANA: "Ghana",
    GIBRALTAR: "Gibraltar",
    GREECE: "Greece",
    GREENLAND: "Greenland",
    GRENADA: "Grenada",
    GUADELOUPE: "Guadeloupe",
    GUAM: "Guam",
    GUATEMALA: "Guatemala",
    GUERNSEY: "Guernsey",
    GUINEA: "Guinea",
    GUINEA_BISSAU: "Guinea Bissau",
    GUYANA: "Guyana",
    HAITI: "Haiti",
    HONDURAS: "Honduras",
    HONG_KONG: "Hong Kong",
    HUNGARY: "Hungary",
    ICELAND: "Iceland",
    INDIA: "India",
    INDONESIA: "Indonesia",
    IRAN: "Iran",
    IRAQ: "Iraq",
    IRELAND: "Ireland",
    ISLE_OF_MAN: "Isle of Man",
    ISRAEL: "Israel",
    ITALY: "Italy",
    IVORY_COAST: "Ivory Coast",
    JAMAICA: "Jamaica",
    JAPAN: "Japan",
    JERSEY: "Jersey",
    JORDAN: "Jordan",
    KAZAKHSTAN: "Kazakhstan",
    KENYA: "Kenya",
    KIRIBATI: "Kiribati",
    KOREA_SOUTH: "Korea (South)",
    KOSOVO: "Kosovo",
    KUWAIT: "Kuwait",
    KYRGYZSTAN: "Kyrgyzstan",
    LAOS: "Laos",
    LATVIA: "Latvia",
    LEBANON: "Lebanon",
    LESOTHO: "Lesotho",
    LIBERIA: "Liberia",
    LIBYA: "Libya",
    LIECHTENSTEIN: "Liechtenstein",
    LITHUANIA: "Lithuania",
    LUXEMBOURG: "Luxembourg",
    MACAU: "Macau",
    MACEDONIA: "Macedonia",
    MADAGASCAR: "Madagascar",
    MALAWI: "Malawi",
    MALAYSIA: "Malaysia",
    MALDIVES: "Maldives",
    MALI: "Mali",
    MALTA: "Malta",
    MARSHALL_ISLANDS: "Marshall Islands",
    MAURITANIA: "Mauritania",
    MAURITIUS: "Mauritius",
    MEXICO: "Mexico",
    MICRONESIA: "Micronesia",
    MOLDAVIA: "Moldavia",
    MONACO: "Monaco",
    MONGOLIA: "Mongolia",
    MONTENEGRO: "Montenegro",
    MONTSERRAT: "Montserrat",
    MOROCCO: "Morocco",
    MOZAMBIQUE: "Mozambique",
    MYANMAR: "Myanmar",
    NAMIBIA: "Namibia",
    NEPAL: "Nepal",
    NETHERLANDS: "Netherlands",
    NETHERLANDS_ANTILLES: "Netherlands Antilles",
    NEW_CALEDONIA: "New Caledonia",
    NEW_ZEALAND: "New Zealand",
    NICARAGUA: "Nicaragua",
    NIGER: "Niger",
    NIGERIA: "Nigeria",
    NORFOLK_ISLAND: "Norfolk Island",
    NORTHERN_MARIANA_ISLANDS: "Northern Mariana Islands",
    NORWAY: "Norway",
    OMAN: "Oman",
    PAKISTAN: "Pakistan",
    PALAU: "Palau",
    PALESTINIAN_TERRITORY: "Palestinian Territory",
    PANAMA: "Panama",
    PAPUA_NEW_GUINEA: "Papua New Guinea",
    PARAGUAY: "Paraguay",
    PERU: "Peru",
    PHILIPPINES: "Philippines",
    POLAND: "Poland",
    POLYNESIA_FRENCH: "Polynesia (French)",
    PORTUGAL: "Portugal",
    PUERTO_RICO: "Puerto Rico",
    QATAR: "Qatar",
    REUNION: "Reunion",
    ROMANIA: "Romania",
    RUSSIAN_FEDERATION: "Russian Federation",
    RWANDA: "Rwanda",
    SAINT_KITTS_AND_NEVIS: "Saint Kitts and Nevis",
    SAINT_LUCIA: "Saint Lucia",
    SAINT_PIERRE_AND_MIQUELON: "Saint Pierre and Miquelon",
    SAINT_TOME_AND_PRINCIPE: "Saint Tome and Principe",
    SAINT_VINCENT_AND_THE_GRENADINES: "Saint Vincent and the Grenadines",
    SAMOA: "Samoa",
    SAN_MARINO: "San Marino",
    SAUDI_ARABIA: "Saudi Arabia",
    SENEGAL: "Senegal",
    SERBIA_AND_MONTENEGRO: "Serbia and Montenegro",
    SEYCHELLES: "Seychelles",
    SIERRA_LEONE: "Sierra Leone",
    SINGAPORE: "Singapore",
    SLOVAK_REPUBLIC: "Slovak Republic",
    SLOVENIA: "Slovenia",
    SOLOMON_ISLANDS: "Solomon Islands",
    SOMALIA: "Somalia",
    SOUTH_AFRICA: "South Africa",
    SOUTH_SUDAN: "South Sudan",
    SPAIN: "Spain",
    SRI_LANKA: "Sri Lanka",
    SUDAN: "Sudan",
    SURINAME: "Suriname",
    SWAZILAND: "Swaziland",
    SWEDEN: "Sweden",
    SWITZERLAND: "Switzerland",
    SYRIA: "Syria",
    TADJIKISTAN: "Tadjikistan",
    TAIWAN: "Taiwan",
    TANZANIA: "Tanzania",
    THAILAND: "Thailand",
    TOGO: "Togo",
    TONGA: "Tonga",
    TRINIDAD_AND_TOBAGO: "Trinidad and Tobago",
    TUNISIA: "Tunisia",
    TURKEY: "Turkey",
    TURKMENISTAN: "Turkmenistan",
    TURKS_AND_CAICOS_ISLANDS: "Turks and Caicos Islands",
    UGANDA: "Uganda",
    UKRAINE: "Ukraine",
    UNITED_ARAB_EMIRATES: "United Arab Emirates",
    UNITED_KINGDOM: "United Kingdom",
    UNITED_STATES: "United States",
    URUGUAY: "Uruguay",
    UZBEKISTAN: "Uzbekistan",
    VANUATU: "Vanuatu",
    VENEZUELA: "Venezuela",
    VIETNAM: "Vietnam",
    VIRGIN_ISLANDS_BRITISH: "Virgin Islands (British)",
    YEMEN: "Yemen",
    ZAMBIA: "Zambia",
    ZIMBABWE: "Zimbabwe",
}

country_string_to_enum = {
    "Afghanistan": AFGHANISTAN,
    "Albania": ALBANIA,
    "Algeria": ALGERIA,
    "American Samoa": AMERICAN_SAMOA,
    "Andorra": ANDORRA,
    "Angola": ANGOLA,
    "Anguilla": ANGUILLA,
    "Antarctica": ANTARCTICA,
    "Antigua and Barbuda": ANTIGUA_AND_BARBUDA,
    "Argentina": ARGENTINA,
    "Armenia": ARMENIA,
    "Aruba": ARUBA,
    "Ascension Island": ASCENSION_ISLAND,
    "Australia": AUSTRALIA,
    "Austria": AUSTRIA,
    "Azerbaidjan": AZERBAIDJAN,
    "Bahamas": BAHAMAS,
    "Bahrain": BAHRAIN,
    "Bangladesh": BANGLADESH,
    "Barbados": BARBADOS,
    "Belarus": BELARUS,
    "Belgium": BELGIUM,
    "Belize": BELIZE,
    "Benin": BENIN,
    "Bermuda": BERMUDA,
    "Bhutan": BHUTAN,
    "Bolivia": BOLIVIA,
    "Bosnia and Herzegovina": BOSNIA_AND_HERZEGOVINA,
    "Botswana": BOTSWANA,
    "Brazil": BRAZIL,
    "Brunei Darussalam": BRUNEI_DARUSSALAM,
    "Bulgaria": BULGARIA,
    "Burkina Faso": BURKINA_FASO,
    "Burundi": BURUNDI,
    "Cambodia": CAMBODIA,
    "Cameroon": CAMEROON,
    "Canada": CANADA,
    "Cape Verde": CAPE_VERDE,
    "Cayman Islands": CAYMAN_ISLANDS,
    "Central African Republic": CENTRAL_AFRICAN_REPUBLIC,
    "Chad": CHAD,
    "Chile": CHILE,
    "China": CHINA,
    "Colombia": COLOMBIA,
    "Comoro": COMORO,
    "Congo": CONGO,
    "Congo, The Democratic Republic of the": CONGO_THE_DEMOCRATIC_REPUBLIC_OF_THE,
    "Cook Islands": COOK_ISLANDS,
    "Costa Rica": COSTA_RICA,
    "Croatia": CROATIA,
    "Cuba": CUBA,
    "Cyprus": CYPRUS,
    "Czech Republic": CZECH_REPUBLIC,
    "Denmark": DENMARK,
    "Djibouti": DJIBOUTI,
    "Dominica": DOMINICA,
    "Dominican Republic": DOMINICAN_REPUBLIC,
    "East Timor": EAST_TIMOR,
    "Ecuador": ECUADOR,
    "Egypt": EGYPT,
    "El Salvador": EL_SALVADOR,
    "Equatorial Guinea": EQUATORIAL_GUINEA,
    "Eritrea": ERITREA,
    "Estonia": ESTONIA,
    "Ethiopia": ETHIOPIA,
    "Falkland Islands": FALKLAND_ISLANDS,
    "Faroe Islands": FAROE_ISLANDS,
    "Fiji": FIJI,
    "Finland": FINLAND,
    "France": FRANCE,
    "French Guiana": FRENCH_GUIANA,
    "French Southern Territories": FRENCH_SOUTHERN_TERRITORIES,
    "Gabon": GABON,
    "Gambia": GAMBIA,
    "Georgia": GEORGIA,
    "Germany": GERMANY,
    "Ghana": GHANA,
    "Gibraltar": GIBRALTAR,
    "Greece": GREECE,
    "Greenland": GREENLAND,
    "Grenada": GRENADA,
    "Guadeloupe": GUADELOUPE,
    "Guam": GUAM,
    "Guatemala": GUATEMALA,
    "Guernsey": GUERNSEY,
    "Guinea": GUINEA,
    "Guinea Bissau": GUINEA_BISSAU,
    "Guyana": GUYANA,
    "Haiti": HAITI,
    "Honduras": HONDURAS,
    "Hong Kong": HONG_KONG,
    "Hungary": HUNGARY,
    "Iceland": ICELAND,
    "India": INDIA,
    "Indonesia": INDONESIA,
    "Iran": IRAN,
    "Iraq": IRAQ,
    "Ireland": IRELAND,
    "Isle of Man": ISLE_OF_MAN,
    "Israel": ISRAEL,
    "Italy": ITALY,
    "Ivory Coast": IVORY_COAST,
    "Jamaica": JAMAICA,
    "Japan": JAPAN,
    "Jersey": JERSEY,
    "Jordan": JORDAN,
    "Kazakhstan": KAZAKHSTAN,
    "Kenya": KENYA,
    "Kiribati": KIRIBATI,
    "Korea (South)": KOREA_SOUTH,
    "Kosovo": KOSOVO,
    "Kuwait": KUWAIT,
    "Kyrgyzstan": KYRGYZSTAN,
    "Laos": LAOS,
    "Latvia": LATVIA,
    "Lebanon": LEBANON,
    "Lesotho": LESOTHO,
    "Liberia": LIBERIA,
    "Libya": LIBYA,
    "Liechtenstein": LIECHTENSTEIN,
    "Lithuania": LITHUANIA,
    "Luxembourg": LUXEMBOURG,
    "Macau": MACAU,
    "Macedonia": MACEDONIA,
    "Madagascar": MADAGASCAR,
    "Malawi": MALAWI,
    "Malaysia": MALAYSIA,
    "Maldives": MALDIVES,
    "Mali": MALI,
    "Malta": MALTA,
    "Marshall Islands": MARSHALL_ISLANDS,
    "Mauritania": MAURITANIA,
    "Mauritius": MAURITIUS,
    "Mexico": MEXICO,
    "Micronesia": MICRONESIA,
    "Moldavia": MOLDAVIA,
    "Monaco": MONACO,
    "Mongolia": MONGOLIA,
    "Montenegro": MONTENEGRO,
    "Montserrat": MONTSERRAT,
    "Morocco": MOROCCO,
    "Mozambique": MOZAMBIQUE,
    "Myanmar": MYANMAR,
    "Namibia": NAMIBIA,
    "Nepal": NEPAL,
    "Netherlands": NETHERLANDS,
    "Netherlands Antilles": NETHERLANDS_ANTILLES,
    "New Caledonia": NEW_CALEDONIA,
    "New Zealand": NEW_ZEALAND,
    "Nicaragua": NICARAGUA,
    "Niger": NIGER,
    "Nigeria": NIGERIA,
    "Norfolk Island": NORFOLK_ISLAND,
    "Northern Mariana Islands": NORTHERN_MARIANA_ISLANDS,
    "Norway": NORWAY,
    "Oman": OMAN,
    "Pakistan": PAKISTAN,
    "Palau": PALAU,
    "Palestinian Territory": PALESTINIAN_TERRITORY,
    "Panama": PANAMA,
    "Papua New Guinea": PAPUA_NEW_GUINEA,
    "Paraguay": PARAGUAY,
    "Peru": PERU,
    "Philippines": PHILIPPINES,
    "Poland": POLAND,
    "Polynesia (French)": POLYNESIA_FRENCH,
    "Portugal": PORTUGAL,
    "Puerto Rico": PUERTO_RICO,
    "Qatar": QATAR,
    "Reunion": REUNION,
    "Romania": ROMANIA,
    "Russian Federation": RUSSIAN_FEDERATION,
    "Rwanda": RWANDA,
    "Saint Kitts and Nevis": SAINT_KITTS_AND_NEVIS,
    "Saint Lucia": SAINT_LUCIA,
    "Saint Pierre and Miquelon": SAINT_PIERRE_AND_MIQUELON,
    "Saint Tome and Principe": SAINT_TOME_AND_PRINCIPE,
    "Saint Vincent and the Grenadines": SAINT_VINCENT_AND_THE_GRENADINES,
    "Samoa": SAMOA,
    "San Marino": SAN_MARINO,
    "Saudi Arabia": SAUDI_ARABIA,
    "Senegal": SENEGAL,
    "Serbia and Montenegro": SERBIA_AND_MONTENEGRO,
    "Seychelles": SEYCHELLES,
    "Sierra Leone": SIERRA_LEONE,
    "Singapore": SINGAPORE,
    "Slovak Republic": SLOVAK_REPUBLIC,
    "Slovenia": SLOVENIA,
    "Solomon Islands": SOLOMON_ISLANDS,
    "Somalia": SOMALIA,
    "South Africa": SOUTH_AFRICA,
    "South Sudan": SOUTH_SUDAN,
    "Spain": SPAIN,
    "Sri Lanka": SRI_LANKA,
    "Sudan": SUDAN,
    "Suriname": SURINAME,
    "Swaziland": SWAZILAND,
    "Sweden": SWEDEN,
    "Switzerland": SWITZERLAND,
    "Syria": SYRIA,
    "Tadjikistan": TADJIKISTAN,
    "Taiwan": TAIWAN,
    "Tanzania": TANZANIA,
    "Thailand": THAILAND,
    "Togo": TOGO,
    "Tonga": TONGA,
    "Trinidad and Tobago": TRINIDAD_AND_TOBAGO,
    "Tunisia": TUNISIA,
    "Turkey": TURKEY,
    "Turkmenistan": TURKMENISTAN,
    "Turks and Caicos Islands": TURKS_AND_CAICOS_ISLANDS,
    "Uganda": UGANDA,
    "Ukraine": UKRAINE,
    "United Arab Emirates": UNITED_ARAB_EMIRATES,
    "United Kingdom": UNITED_KINGDOM,
    "United States": UNITED_STATES,
    "Uruguay": URUGUAY,
    "Uzbekistan": UZBEKISTAN,
    "Vanuatu": VANUATU,
    "Venezuela": VENEZUELA,
    "Vietnam": VIETNAM,
    "Virgin Islands (British)": VIRGIN_ISLANDS_BRITISH,
    "Yemen": YEMEN,
    "Zambia": ZAMBIA,
    "Zimbabwe": ZIMBABWE,
}


# Rate Plan ID constants

RATEPLAN_0GB_USCANMX_LTE_USAGE = 2529001
RATEPLAN_0KB_INTL_SELECT_PLAN = 4313601
RATEPLAN_0KB = 7885702
RATEPLAN_0KB_USCANMX_LTE_USAGE = 2444401
RATEPLAN_0KB_USCANMX_USAGE = 2444301
RATEPLAN_100MB_INTL_PLAN = 1926501
RATEPLAN_100MB_INTL_SELECT = 4313701
RATEPLAN_100MB_PLAN = 1787501
RATEPLAN_10MB_PLAN = 2827002
RATEPLAN_1GB_INTL_SELECT = 4313901
RATEPLAN_1GB_LTE_PLAN = 2398401
RATEPLAN_1GB_US_PLAN = 2827102
RATEPLAN_1MB_PLAN = 2826902
RATEPLAN_20GB_INTL_USAGE_TIER = 7885802
RATEPLAN_30MB_PLAN = 2828902
RATEPLAN_500MB_INTL_SELECT = 4313801
RATEPLAN_76800GB_USCANMX_USAGE = 5213802

rate_plan_id_to_name = {
    RATEPLAN_0GB_USCANMX_LTE_USAGE: "Samsara Networks - 0GB USCANMX LTE Usage",
    RATEPLAN_0KB_INTL_SELECT_PLAN: "Samsara Networks - 0kB Intl Select Plan",
    RATEPLAN_0KB: "Samsara Networks - 0kB Plan",
    RATEPLAN_0KB_USCANMX_LTE_USAGE: "Samsara Networks - 0kB USCANMX LTE Usage",
    RATEPLAN_0KB_USCANMX_USAGE: "Samsara Networks - 0kB USCANMX Usage",
    RATEPLAN_100MB_INTL_PLAN: "Samsara Networks - 100MB Intl Plan",
    RATEPLAN_100MB_INTL_SELECT: "Samsara Networks - 100MB Intl Select",
    RATEPLAN_100MB_PLAN: "Samsara Networks - 100MB Plan",
    RATEPLAN_10MB_PLAN: "Samsara Networks - 10MB Plan",
    RATEPLAN_1GB_INTL_SELECT: "Samsara Networks - 1GB Intl Select",
    RATEPLAN_1GB_LTE_PLAN: "Samsara Networks - 1GB LTE Plan",
    RATEPLAN_1GB_US_PLAN: "Samsara Networks - 1GB US Plan",
    RATEPLAN_1MB_PLAN: "Samsara Networks - 1MB Plan",
    RATEPLAN_20GB_INTL_USAGE_TIER: "Samsara Networks - 20GB Intl Usage Tier",
    RATEPLAN_30MB_PLAN: "Samsara Networks - 30MB Plan",
    RATEPLAN_500MB_INTL_SELECT: "Samsara Networks - 500MB Intl Select",
    RATEPLAN_76800GB_USCANMX_USAGE: "Samsara Networks - 76800GB USCANMX Usage",
}

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Cost Calculation Functions

# COMMAND ----------

from dataclasses import dataclass


@dataclass
class RatePlanTier:
    rate_plan_id: str
    # defined in $.
    minimum_resource_cost: float
    included_mbs: int
    # defined in $ / MiB
    data_overage_internal: float
    # defined in $ / MiB
    data_overage_roaming: float


# RATE PLAN: Samsara Networks - 100MB Intl Plan
#
# Subscriber Charge: 12.00 $ / SIM
#
# From Jasper Website:
# Zone 1: Group A 2015:
#  Tier 1: Data Overage (per KB): 0.0014550781
#
# Zone 2: Other Usage:
#  Tier 1: Data Overage (per KB): 0.0002441406
def getTierForInternationalPlan(totalUsageForRatePlan):
    return RatePlanTier(
        rate_plan_id=RATEPLAN_100MB_INTL_PLAN,
        minimum_resource_cost=12.00,
        included_mbs=100,
        data_overage_internal=0.0014550781 * 1024,
        data_overage_roaming=0.0002441406 * 1024,
    )


# RATE PLAN: Samsara Networks - 20GB Intl Usage Tier
#
# Subscriber Charge: 0.2 $ / SIM
#
# Zone 1: AT&T Intl Select 27
#  Tier 1: 0 - 20,971,520 KB          --> Overage (per KB): 0.0000043945
#  Tier 2: 20,971,520 - 26,214,400 KB --> Overage (per KB): 0.0000041504
#  Tier 3: 26,214,400 - 31,457,280 KB --> Overage (per KB): 0.0000036133
#  Tier 4: 31,457,280 KB and above    --> Overage (per KB): 0.0000034180
#
# Zone 2:
#  All Tiers: Overage (per KB): 0.0002441406
def getTierForEuropeanRatePlan(totalUsageForRatePlan):
    numKB = totalUsageForRatePlan / 1024
    if numKB < 20971520:
        return RatePlanTier(
            rate_plan_id=RATEPLAN_20GB_INTL_USAGE_TIER,
            minimum_resource_cost=0.20,
            included_mbs=0,
            data_overage_internal=0.0000043945 * 1024,
            data_overage_roaming=0.0002441406 * 1024,
        )
    elif numKB >= 20971520 and numKB < 26214400:
        return RatePlanTier(
            rate_plan_id=RATEPLAN_20GB_INTL_USAGE_TIER,
            minimum_resource_cost=0.20,
            included_mbs=0,
            data_overage_internal=0.0000041504 * 1024,
            data_overage_roaming=0.0002441406 * 1024,
        )
    elif numKB >= 26214400 and numKB < 31457280:
        return RatePlanTier(
            rate_plan_id=RATEPLAN_20GB_INTL_USAGE_TIER,
            minimum_resource_cost=0.20,
            included_mbs=0,
            data_overage_internal=0.0000036133 * 1024,
            data_overage_roaming=0.0002441406 * 1024,
        )
    else:
        return RatePlanTier(
            rate_plan_id=RATEPLAN_20GB_INTL_USAGE_TIER,
            minimum_resource_cost=0.20,
            included_mbs=0,
            data_overage_internal=0.0000034180 * 1024,
            data_overage_roaming=0.0002441406 * 1024,
        )


# Rate Plan: Samsara Networks - 76800GB USCANMX Usage
#
# Subscriber Charge: 0.2 $ / SIM
#
# Zone 1: US-Canada-Mexico
#  Tier 1: 0 - 107,374,182,400 KB               --> Overage (per KB): 0.0000061035
#  Tier 2: 107,374,182,400 - 161,061,273,600 KB --> Overage (per KB): 0.0000053711
#  Tier 3: 161,061,273,600 - 209,715,200,000 KB --> Overage (per KB): 0.0000043945
#  Tier 4: 209,715,200,000 - 314,572,800,000 KB --> Overage (per KB): 0.0000035156
#  Tier 5: 314,572,800,000 and above            --> Overage (per KB): 0.0000030762
#
# Zone 2:
#  All Tiers: Overage (per KB): 0.0002441406
def getTierForNorAMRatePlan(totalUsageForRatePlan):
    numKB = totalUsageForRatePlan / 1024
    if numKB < 107374182400:
        return RatePlanTier(
            rate_plan_id=RATEPLAN_76800GB_USCANMX_USAGE,
            minimum_resource_cost=0.2,
            included_mbs=0,
            data_overage_internal=0.0000061035 * 1024,
            data_overage_roaming=0.0002441406 * 1024,
        )
    elif numKB >= 107374182400 and numKB < 161061273600:
        return RatePlanTier(
            rate_plan_id=RATEPLAN_76800GB_USCANMX_USAGE,
            minimum_resource_cost=0.2,
            included_mbs=0,
            data_overage_internal=0.0000053711 * 1024,
            data_overage_roaming=0.0002441406 * 1024,
        )
    elif numKB >= 161061273600 and numKB < 209715200000:
        return RatePlanTier(
            rate_plan_id=RATEPLAN_76800GB_USCANMX_USAGE,
            minimum_resource_cost=0.2,
            included_mbs=0,
            data_overage_internal=0.0000043945 * 1024,
            data_overage_roaming=0.0002441406 * 1024,
        )
    elif numKB >= 209715200000 and numKB < 314572800000:
        return RatePlanTier(
            rate_plan_id=RATEPLAN_76800GB_USCANMX_USAGE,
            minimum_resource_cost=0.2,
            included_mbs=0,
            data_overage_internal=0.0000035156 * 1024,
            data_overage_roaming=0.0002441406 * 1024,
        )
    else:
        return RatePlanTier(
            rate_plan_id=RATEPLAN_76800GB_USCANMX_USAGE,
            minimum_resource_cost=0.2,
            included_mbs=0,
            data_overage_internal=0.0000030762 * 1024,
            data_overage_roaming=0.0002441406 * 1024,
        )


# Rate Plan: Samsara Networks - 0kB Plan
#
# Subscriber Charge: 0.45 $ / SIM
#
# Zone 1: US-Canada-Mexico
#  Tier 1: 0 and up --> Overage (per KB): 0.0000214844
#
# Zone 2:
#  All Tiers: Overage (per KB): 0.0002441406
#
def getTierFor3GRatePlan(totalUsageForRatePlan):
    return RatePlanTier(
        rate_plan_id=RATEPLAN_0KB,
        minimum_resource_cost=0.45,
        included_mbs=0,
        data_overage_internal=0.0000214844 * 1024,
        data_overage_roaming=0.0002441406 * 1024,
    )


# Rate Plan: Samsara Networks - 1MB Plan
#
# Subscriber Charge: 0.8 $ / SIM
#
# Zone 1: US-Canada-Mexico
#  Tier 1: 0 and up --> Overage (per KB): 0.0009765625
#
# Zone 2:
#  All Tiers: Overage (per KB): 0.004882813
#
def getTierFor1MBPlan(totalUsageForRatePlan):
    return RatePlanTier(
        rate_plan_id=RATEPLAN_1MB_PLAN,
        minimum_resource_cost=0.8,
        included_mbs=0,
        data_overage_internal=0.0009765625 * 1024,
        data_overage_roaming=0.004882813 * 1024,
    )


# Rate Plan: Samsara Networks - 0kB Intl Select Plan
#
# Subscriber Charge: 0.4 $ / SIM
#
# Zone 1: AT&T Intl Select 27
#  Tier 1: 0 and up --> Overage (per KB): 0.0004882813
#
# Zone 2:
#  All Tiers: Overage (per KB): 0.0002441406
#
def getTierFor0KBIntlSelectPlan(totalUsageForRatePlan):
    return RatePlanTier(
        rate_plan_id=RATEPLAN_1MB_PLAN,
        minimum_resource_cost=0.4,
        included_mbs=0,
        data_overage_internal=0.0004882813 * 1024,
        data_overage_roaming=0.0002441406 * 1024,
    )


# TODO: we don't have a mapping of what countries are in the country group AT&T Intl Select 27
# so we can't determine what the roaming vs. internal costs are right now.
def calculate_cost_0kb_intl_select(plans_to_usage_mapping, totalUsageForRatePlan):
    return 0


def calculate_cost_3g(plans_to_usage_mapping, totalUsageForRatePlan):
    tier = getTierFor3GRatePlan(totalUsageForRatePlan)
    cost = (
        tier.minimum_resource_cost
        + tier.data_overage_internal * plans_to_usage_mapping[NORAM] / SIZEOFMB
        + tier.data_overage_roaming
        * (
            plans_to_usage_mapping[EUROPE]
            + plans_to_usage_mapping[GROUP_A]
            + plans_to_usage_mapping[OUTSIDE_GROUP_A]
        )
        / SIZEOFMB
    )
    return cost


def calculate_cost_noram(plans_to_usage_mapping, totalUsageForRatePlan):
    tier = getTierForNorAMRatePlan(totalUsageForRatePlan)
    cost = (
        tier.minimum_resource_cost
        + tier.data_overage_internal * plans_to_usage_mapping[NORAM] / SIZEOFMB
        + tier.data_overage_roaming
        * (
            plans_to_usage_mapping[EUROPE]
            + plans_to_usage_mapping[GROUP_A]
            + plans_to_usage_mapping[OUTSIDE_GROUP_A]
        )
        / SIZEOFMB
    )
    return cost


def calculate_cost_europe(plans_to_usage_mapping, totalUsageForRatePlan):
    tier = getTierForEuropeanRatePlan(totalUsageForRatePlan)
    return (
        tier.minimum_resource_cost
        + tier.data_overage_internal * plans_to_usage_mapping[EUROPE] / SIZEOFMB
        + tier.data_overage_roaming
        * (
            plans_to_usage_mapping[NORAM]
            + plans_to_usage_mapping[GROUP_A]
            + plans_to_usage_mapping[OUTSIDE_GROUP_A]
        )
        / SIZEOFMB
    )


# for Samsara Networks - 100MB Intl Plan
def calculate_cost_international(plans_to_usage_mapping, totalUsageForRatePlan):
    tier = getTierForInternationalPlan(totalUsageForRatePlan)

    # TODO: We need to only calculate overages if we go over 100 MB, which should ideally never happen.
    # Perhaps we can do that after pooled calculations are made.

    # Rate is per MB while our data is counted in bytes, so we have to divide by size of MiB.
    # cost = (
    #    tier.minimum_resource_cost
    #   + tier.data_overage_roaming * plans_to_usage_mapping[OUTSIDE_GROUP_A] / SIZEOFMB
    # )
    return tier.minimum_resource_cost


def calculate_cost_1mb(plans_to_usage_mapping, totalUsageForRatePlan):
    tier = getTierFor1MBPlan(totalUsageForRatePlan)
    cost = (
        tier.minimum_resource_cost
        + tier.data_overage_internal * plans_to_usage_mapping[NORAM] / SIZEOFMB
        + tier.data_overage_roaming
        * (
            plans_to_usage_mapping[EUROPE]
            + plans_to_usage_mapping[GROUP_A]
            + plans_to_usage_mapping[OUTSIDE_GROUP_A]
        )
        / SIZEOFMB
    )
    return cost


# calculates the total cost (subscription + data charge) for the given ICCID for the rate
# plan which it is assigned.
def calculate_cost(iccid, rate_plan, plans_to_usage_mapping, totalUsageForRatePlan):
    if rate_plan == RATEPLAN_0KB:
        return calculate_cost_3g(plans_to_usage_mapping, totalUsageForRatePlan)
    elif rate_plan == RATEPLAN_76800GB_USCANMX_USAGE:
        return calculate_cost_noram(plans_to_usage_mapping, totalUsageForRatePlan)
    elif rate_plan == RATEPLAN_20GB_INTL_USAGE_TIER:
        return calculate_cost_europe(plans_to_usage_mapping, totalUsageForRatePlan)
    elif rate_plan == RATEPLAN_100MB_INTL_PLAN:
        return calculate_cost_international(
            plans_to_usage_mapping, totalUsageForRatePlan
        )
    elif rate_plan == RATEPLAN_1MB_PLAN:
        return calculate_cost_1mb(plans_to_usage_mapping, totalUsageForRatePlan)
    elif rate_plan == RATEPLAN_0KB_INTL_SELECT_PLAN:
        return 0
    elif rate_plan == RATEPLAN_0GB_USCANMX_LTE_USAGE:
        return 0
    # No devices should be allocated to other rate plans; algo should have reallocated to one of
    # the above.
    else:
        return 0


# returns the subscription charge (base fee for a device) for a rate plan. The total usage
# can affect this subscription cost depending upon the rate plan.
def calculate_subscription_cost(rate_plan, totalUsageForRatePlan):
    if rate_plan == RATEPLAN_0KB:
        return getTierFor3GRatePlan(totalUsageForRatePlan).minimum_resource_cost
    elif rate_plan == RATEPLAN_76800GB_USCANMX_USAGE:
        return getTierForNorAMRatePlan(totalUsageForRatePlan).minimum_resource_cost
    elif rate_plan == RATEPLAN_20GB_INTL_USAGE_TIER:
        return getTierForEuropeanRatePlan(totalUsageForRatePlan).minimum_resource_cost
    elif rate_plan == RATEPLAN_100MB_INTL_PLAN:
        return getTierForInternationalPlan(totalUsageForRatePlan).minimum_resource_cost
    elif rate_plan == RATEPLAN_1MB_PLAN:
        return getTierFor1MBPlan(totalUsageForRatePlan).minimum_resource_cost
    elif rate_plan == RATEPLAN_0KB_INTL_SELECT_PLAN:
        # TODO
        return 0
    elif rate_plan == RATEPLAN_0GB_USCANMX_LTE_USAGE:
        # TODO
        return 0
    # No devices should be allocated to other rate plans; algo should have reallocated to one of
    # the above.
    else:
        return 0


# COMMAND ----------

# MAGIC %md
# MAGIC # Rate Plan Allocation Algorithm

# COMMAND ----------

from collections import defaultdict, Counter
from dataclasses import dataclass
from builtins import sum as fsum
import json
from math import floor, ceil

"""
The logic below implements an algorithm for optimizing rate plan allocation
for Samsara gateways.

PRD: https://paper.dropbox.com/doc/PRD-ATT-Backend-Cellular-Optimization-System--AzmCgJ5tKrfWTDka0wizJjpaAg-2vFYQhcD5iKTbHYr5ZJ2T
RFC: https://paper.dropbox.com/doc/RFC-Automating-Optimization-of-Cellular-Rate-Plans-for-Samsara-Gateways-052020--A0P8kh1Vc~Y3GTxwytRxaD0KAg-3ESI1E3WHD2AvkQOxhf1h
"""

"""
# CORE LOGIC
IF ICCID IN "New Noram Rate Plan, 3G" AND NOT ROAMING THEN "New Noram Rate Plan, 3G"
ELSEIF ICCID ONLY IN "North America" THEN "New NorAm Rate Plan"
ELSEIF ICCID ONLY IN "Schedule B5" THEN "New Europe Rate Plan"
ELSE
  IF ICCID NOT IN "Group A" THEN "New Europe Rate Plan"
  ELSEIF ICCID PARTIALLY IN "North America + Group A" THEN "International Plan"
  ELSEIF ICCID PARTIALLY IN "Schedule B5 + Group A" THEN
    IF "Roaming Data Usage" > "Cutoff" THEN "International Plan"
    ELSE "New Europe Rate Plan"
  ELSEIF ICCID ONLY IN "Group A" THEN "International Plan"
  ELSE # SHOULD BE NO SIMS IN ELSE CONDITION

# INTERNATIONAL PLAN OVERAGE
IF "Allocated Usage International Plan" < "Total Usage International Plan"
THEN "Reallocate Low Usage ICCIDs" TO "International Plan"
# PREFERABLY REALLOCATE DEVICE PARTIALLY IN "Schedule B5 + GROUP A"
"""


"""
For each rate plan, we need to aggregate total usage for all devices in that rate plan.
Have dict keyed by rate plan ID which shows total usage.

At bottom of function, we add usage to the entry for the new rate plan
"""


@dataclass
class PotentialICCID:
    iccid: str
    ideal_rate_plan: str
    usage: int
    preference: int
    plans_to_usage_mapping: defaultdict(int)


class ICCIDToChangeMetadata:
    def __init__(self, iccid, old_rate_plan, new_rate_plan, plans_to_usage_mapping):
        self.iccid = iccid
        self.old_rate_plan = old_rate_plan
        self.new_rate_plan = new_rate_plan
        self.plans_to_usage_mapping = plans_to_usage_mapping


class JasperOptimization:
    def __init__(self):
        # Dictionary of all iccids that are going to change with respective rate plan {"iccid": ("rate_plan", "rate_plan_name")} i.e {"89011702272018710035" : ("7885802", "RATEPLAN_20GB_INTL_USAGE_TIER")}
        self.iccids_to_change = {}
        # List of all potential iccids to switch over to international plan to increase pool [{"ICCID": "89011702272018710035","Ideal_Rate_Plan":"7885802", "Usage": 12312312, "Preference": 2}]
        self.potential_international_iccids = []
        # Dictionary of rate_plans to sets of iccids from snapshot data (all activated devices) {RATEPLAN_20GB_INTL_USAGE_TIER: {"89011702272018710035", "89011702272018710036"}, RATEPLAN_100MB_INTL_PLAN: {"89011702272018710037"}}
        self.rate_plan_iccid_sets = defaultdict(set)
        self.allocated_usage_international_plan = 0
        self.total_international_plan_usage = 0
        self.info_per_iccid = defaultdict(int)
        self.usage_by_rate_plan = defaultdict(int)

    def populate_dormant_iccids(self, snapshot_data):
        # Map rate plans to set of ICCIDs.
        for k, v in snapshot_data.items():
            if k in rate_plan_id_to_name:
                self.rate_plan_iccid_sets[k] = set(v)

    # Compare if rate plan is different from what we are setting it to. Only update ICCIDS that are different.
    # Updates `iccids_to_change` with the given iccid if the rate plan changes..
    def assign_rate_plan(
        self,
        iccid,
        assigned_rate_plan_id,
        new_rate_plan,
        total_usage,
        plans_to_usage_mapping,
        preference,
    ):
        # We remove from the snapshot set because there has been some data usage for this ICCID
        for _, iccidset in self.rate_plan_iccid_sets.items():
            if iccid in iccidset:
                iccidset.remove(iccid)

        if (
            assigned_rate_plan_id != new_rate_plan
            and new_rate_plan in rate_plan_id_to_name
        ):
            self.iccids_to_change[iccid] = (
                new_rate_plan,
                rate_plan_id_to_name[new_rate_plan],
            )

        # Since on the international plan, we have allotted 100 MB to the pool
        if new_rate_plan == RATEPLAN_100MB_INTL_PLAN:
            self.allocated_usage_international_plan += POOL_INCREASE_PER_DEVICE
            self.total_international_plan_usage += total_usage
        else:
            self.potential_international_iccids.append(
                PotentialICCID(
                    iccid=iccid,
                    ideal_rate_plan=new_rate_plan,
                    usage=total_usage,
                    preference=preference,
                    plans_to_usage_mapping=plans_to_usage_mapping,
                )
            )

        if iccid in self.info_per_iccid:
            # Given that assign_rate_plan has been called previously for this ICCID, and it updates the
            # total usage for the ideal rate plan for this ICCID, we need to negate that change to the
            # total usage for the rate plan.
            self.usage_by_rate_plan[assigned_rate_plan_id] -= total_usage

            # Always keep track of the original rate plan which was assigned.
            assigned_rate_plan_id = self.info_per_iccid[iccid].old_rate_plan

        # Cache computed state about this ICCID for cost computations later on.
        self.info_per_iccid[iccid] = ICCIDToChangeMetadata(
            iccid, assigned_rate_plan_id, new_rate_plan, plans_to_usage_mapping
        )

        # Update total usage for the newly assigned rate plan.
        self.usage_by_rate_plan[new_rate_plan] += total_usage

    # Iterate through dataUsagePerLocale dictionary and group tap_codes to locations
    # Locations: NorAm, Europe, GroupA, OutsideGroupA
    # Outputs dict with the usage in bytes per-location.
    def process_tap_codes(self, row):
        plans_to_usage_mapping = defaultdict(int)
        for tap_code, usage in row.data_usage_per_locale.items():
            try:
                tc = tap_code_to_country[tap_code]
            except KeyError:
                print(
                    "KeyError getting tap code: iccid: ",
                    row.iccid,
                    " tap_code: ",
                    tap_code,
                    " usage: ",
                    usage,
                )
                continue
            country = country_string_to_enum[tc]
            if country_string_to_enum[tc] in north_america_locales:
                plans_to_usage_mapping[NORAM] += usage
            elif country_string_to_enum[tc] in schedule_b5_locales:
                plans_to_usage_mapping[EUROPE] += usage
            elif country_string_to_enum[tc] in group_a_locales:
                plans_to_usage_mapping[GROUP_A] += usage
            else:
                plans_to_usage_mapping[OUTSIDE_GROUP_A] += usage
        return plans_to_usage_mapping

    # Returns the sum of all values for all keys in `plans_to_usage_mapping`.
    def total_usage(self, plans_to_usage_mapping):
        if plans_to_usage_mapping:
            return fsum(plans_to_usage_mapping.values())
        return 0

    # Implements core logic outlined in PRD (listed above).
    # For a given row, which consists of the usage across all tap codes for a given ICCID,
    # determiens which rate plan that device should be allocated purely based off its
    # usage alone.
    def process_iccid(self, row):
        # Convert tap_code->usage to location->usage
        plans_to_usage_mapping = self.process_tap_codes(row)

        # If on the 3G plan...
        if row.assigned_rate_plan_id == RATEPLAN_0KB:
            # Not roaming (only in NorAm) keep as NorAm Plan 3G.
            if NORAM in plans_to_usage_mapping and len(plans_to_usage_mapping) == 1:
                self.assign_rate_plan(
                    row.iccid,
                    row.assigned_rate_plan_id,
                    RATEPLAN_0KB,
                    self.total_usage(plans_to_usage_mapping),
                    plans_to_usage_mapping,
                    3,
                )

            # If roaming move to International (shouldn't be likely since devices on the 3G plan are mostly in NorAm)
            else:
                self.assign_rate_plan(
                    row.iccid,
                    row.assigned_rate_plan_id,
                    RATEPLAN_100MB_INTL_PLAN,
                    self.total_usage(plans_to_usage_mapping),
                    plans_to_usage_mapping,
                    100,
                )

        # Exclusively one location; this means that a given ICCID was exclusively in one of the sets of
        # countries which correspond to a given rate plan, making rate-plan allocation easy.
        elif len(plans_to_usage_mapping) == 1:
            # Only North America.
            if NORAM in plans_to_usage_mapping:
                # Set to NewNorAm Plan
                self.assign_rate_plan(
                    row.iccid,
                    row.assigned_rate_plan_id,
                    RATEPLAN_76800GB_USCANMX_USAGE,
                    self.total_usage(plans_to_usage_mapping),
                    plans_to_usage_mapping,
                    3,
                )

            # Only Europe.
            elif EUROPE in plans_to_usage_mapping:
                # Set to Europe Plan
                self.assign_rate_plan(
                    row.iccid,
                    row.assigned_rate_plan_id,
                    RATEPLAN_20GB_INTL_USAGE_TIER,
                    self.total_usage(plans_to_usage_mapping),
                    plans_to_usage_mapping,
                    3,
                )

            # NOTE: This line is moved from the original spot outlined in the PRD
            # Only in Group A.
            elif GROUP_A in plans_to_usage_mapping:
                # International plan and sum usage
                self.assign_rate_plan(
                    row.iccid,
                    row.assigned_rate_plan_id,
                    RATEPLAN_100MB_INTL_PLAN,
                    self.total_usage(plans_to_usage_mapping),
                    plans_to_usage_mapping,
                    100,
                )

            # Outside countries which are part of our rate plans (roaming).
            elif OUTSIDE_GROUP_A in plans_to_usage_mapping:
                # Set to Europe Plan since roaming rates for the EU are the cheapest
                self.assign_rate_plan(
                    row.iccid,
                    row.assigned_rate_plan_id,
                    RATEPLAN_20GB_INTL_USAGE_TIER,
                    self.total_usage(plans_to_usage_mapping),
                    plans_to_usage_mapping,
                    3,
                )

        # In this section, we deal with devices which moved between countries that span multiple rate plans, e.g.
        # Mexico (part of NORAM) and Guatemala (part of International).
        else:

            # NOTE: This case is not accounted for in PRD logic.
            # Device only in NorAm and outside Group A
            if (
                NORAM in plans_to_usage_mapping
                and OUTSIDE_GROUP_A in plans_to_usage_mapping
                and len(plans_to_usage_mapping) == 2
            ):
                # Set to NorAm
                self.assign_rate_plan(
                    row.iccid,
                    row.assigned_rate_plan_id,
                    RATEPLAN_76800GB_USCANMX_USAGE,
                    self.total_usage(plans_to_usage_mapping),
                    plans_to_usage_mapping,
                    3,
                )

            # NOTE: This case is not accounted for in PRD logic (case shouldn't happen)
            # Device in NorAm and Europe.
            elif NORAM in plans_to_usage_mapping and EUROPE in plans_to_usage_mapping:
                # Determine where usage is higher and change to respective plan
                if plans_to_usage_mapping[NORAM] > plans_to_usage_mapping[EUROPE]:
                    self.assign_rate_plan(
                        row.iccid,
                        row.assigned_rate_plan_id,
                        RATEPLAN_76800GB_USCANMX_USAGE,
                        self.total_usage(plans_to_usage_mapping),
                        plans_to_usage_mapping,
                        3,
                    )
                else:
                    self.assign_rate_plan(
                        row.iccid,
                        row.assigned_rate_plan_id,
                        RATEPLAN_20GB_INTL_USAGE_TIER,
                        self.total_usage(plans_to_usage_mapping),
                        plans_to_usage_mapping,
                        3,
                    )

            elif (
                GROUP_A not in plans_to_usage_mapping
                and len(plans_to_usage_mapping) > 0
            ):
                # Set to Europe Plan since roaming rates are the cheapest in the EU
                self.assign_rate_plan(
                    row.iccid,
                    row.assigned_rate_plan_id,
                    RATEPLAN_20GB_INTL_USAGE_TIER,
                    self.total_usage(plans_to_usage_mapping),
                    plans_to_usage_mapping,
                    3,
                )
            elif NORAM in plans_to_usage_mapping and GROUP_A in plans_to_usage_mapping:
                # Set to International Plan and update total international usage.
                self.assign_rate_plan(
                    row.iccid,
                    row.assigned_rate_plan_id,
                    RATEPLAN_100MB_INTL_PLAN,
                    self.total_usage(plans_to_usage_mapping),
                    plans_to_usage_mapping,
                    100,
                )

            # Assuming that OutsideGroupA -> goes to EU so we compare based on usage what is cheaper
            elif (
                EUROPE in plans_to_usage_mapping
                or OUTSIDE_GROUP_A in plans_to_usage_mapping
            ) and GROUP_A in plans_to_usage_mapping:
                if plans_to_usage_mapping[GROUP_A] > ROAMING_CUTOFF:
                    # Set to International Plan and sum intl usage
                    self.assign_rate_plan(
                        row.iccid,
                        row.assigned_rate_plan_id,
                        RATEPLAN_100MB_INTL_PLAN,
                        self.total_usage(plans_to_usage_mapping),
                        plans_to_usage_mapping,
                        100,
                    )
                else:
                    # Set to Europe Plan
                    self.assign_rate_plan(
                        row.iccid,
                        row.assigned_rate_plan_id,
                        RATEPLAN_20GB_INTL_USAGE_TIER,
                        self.total_usage(plans_to_usage_mapping),
                        plans_to_usage_mapping,
                        2,
                    )
            else:
                print("Unhandled iccid: ", row)
                print("Mapping: ", plans_to_usage_mapping)

    def reallocate_unused_intl_devices(self):
        # Figure out how many multiples of 100 MB we aren't using.
        num_devices_to_reallocate = floor(
            (
                self.allocated_usage_international_plan
                - self.total_international_plan_usage
            )
            / POOL_INCREASE_PER_DEVICE
        )

        # Sort the ICCIDs so that we consistently re-allocate the same ICCIDs across different runs of the algorithm
        # that use the same Subscriber Snapshot.
        sorted_intl_iccids = sorted(self.rate_plan_iccid_sets[RATEPLAN_100MB_INTL_PLAN])

        i = 0
        while (
            i < num_devices_to_reallocate
            and i < len(sorted_intl_iccids)
            and self.allocated_usage_international_plan
            > self.total_international_plan_usage
        ):
            iccidToReallocate = sorted_intl_iccids[i]
            # Use low preference value to make sure this device isn't reallocated later on.
            # Move to NorAm rate plan because the charge per device is much lower ($0.20 vs. $12.00 as of 6/11/20).
            self.assign_rate_plan(
                iccidToReallocate,
                RATEPLAN_100MB_INTL_PLAN,
                RATEPLAN_76800GB_USCANMX_USAGE,
                0,
                {NORAM: 0, GROUP_A: 0, EUROPE: 0, OUTSIDE_GROUP_A: 0},
                100,
            )
            self.allocated_usage_international_plan -= POOL_INCREASE_PER_DEVICE
            i += 1

    def reallocate_iccids_by_preference(self):
        # Add unused NorAm devices to potential_international_iccids dict
        # First, try to reallocate devices which are not using any data in the NorAm rate plan to international plan.
        # These devices aren't using any data, so we will only incur Minimum Recurring Charge (MRC) of international
        # plan. If a device that is reallocated does start using data in NorAm, it will be reallocated by the next run
        # of this algorithm when new session data is given to us by AT&T.
        for iccid in self.rate_plan_iccid_sets[RATEPLAN_76800GB_USCANMX_USAGE]:
            self.potential_international_iccids.append(
                PotentialICCID(
                    iccid=iccid,
                    ideal_rate_plan=RATEPLAN_76800GB_USCANMX_USAGE,
                    usage=0,
                    preference=1,
                    plans_to_usage_mapping={
                        NORAM: 0,
                        GROUP_A: 0,
                        EUROPE: 0,
                        OUTSIDE_GROUP_A: 0,
                    },
                )
            )

        # Throughout the optimization we have been populating potential_international_iccids with various preference
        # values. This preference represents which devices we will move out of their assigned rate plan into the
        # international plan to increase the pool size
        # Preference = 1 -> Unused devices on the NorAm Plan
        # Preference = 2 -> Devices that are set to EU plan because they used lower than the cutoff in Group A
        # Preference = 3 -> Devices that is set to any rate plan that is NOT International
        # Preference = 100 -> Devices that are already set to International (should never reach these)
        # We then sort the list on preference, then data usage and then ICCIDs so that we pick the same ICCIDs between
        # different runs of the same data. We keep moving devices to increase the pool until we are within 100 MB of
        # allocated usage and total international usage
        if (
            self.allocated_usage_international_plan
            < self.total_international_plan_usage
        ):
            sorted_iccids = sorted(
                self.potential_international_iccids,
                key=lambda k: (k.preference, k.usage, k.iccid),
            )
            i = 0
            while (
                i < len(sorted_iccids)
                and self.total_international_plan_usage
                > self.allocated_usage_international_plan
            ):
                potential_iccid = sorted_iccids[i]

                self.assign_rate_plan(
                    potential_iccid.iccid,
                    potential_iccid.ideal_rate_plan,
                    RATEPLAN_100MB_INTL_PLAN,
                    potential_iccid.usage,
                    potential_iccid.plans_to_usage_mapping,
                    100,
                )
                i += 1

    def reallocate_low_usage_intl_plan_devices(self):
        # If an ICCID has all of its usage in Group A 2015 countries, we assign it to the international plan. However,
        # this plan has a fixed fee per 100 MB of usage. So, it actually might be cheaper to assign the ICCID to one
        # of the other rate plans we use even if it means that we'll incur overages with those plans. Perform cost
        # calculations for all plans and choose the cheapest one to assign to this ICCID.
        for _, v in self.info_per_iccid.items():
            # If we have larger than 50 device's worth of 100MB left on international data plan, then we can move devices
            # which have really low usage in the international rate plan to the European plan. This allows for 5000 MB of
            # overages by devices in the international rate plan.
            if (
                self.allocated_usage_international_plan
                < self.total_international_plan_usage + POOL_INCREASE_PER_DEVICE * 50
            ):
                break

            if v.new_rate_plan == RATEPLAN_100MB_INTL_PLAN:
                intlPlanCost = calculate_cost(
                    v.iccid,
                    RATEPLAN_100MB_INTL_PLAN,
                    v.plans_to_usage_mapping,
                    self.usage_by_rate_plan[RATEPLAN_100MB_INTL_PLAN],
                )
                europeanPlanCost = calculate_cost(
                    v.iccid,
                    RATEPLAN_20GB_INTL_USAGE_TIER,
                    v.plans_to_usage_mapping,
                    self.usage_by_rate_plan[RATEPLAN_20GB_INTL_USAGE_TIER],
                )

                noramPlanCost = calculate_cost(
                    v.iccid,
                    RATEPLAN_76800GB_USCANMX_USAGE,
                    v.plans_to_usage_mapping,
                    self.usage_by_rate_plan[RATEPLAN_76800GB_USCANMX_USAGE],
                )

                # Sometimes NorAm and European costs are equivalent; only assign to NorAm if it
                # truly is cheaper than both the European and international plans.
                if (
                    noramPlanCost < intlPlanCost
                    and noramPlanCost < europeanPlanCost - 0.100
                ):
                    # assign to normAmplan
                    self.assign_rate_plan(
                        v.iccid,
                        v.new_rate_plan,
                        RATEPLAN_76800GB_USCANMX_USAGE,
                        self.total_usage(v.plans_to_usage_mapping),
                        v.plans_to_usage_mapping,
                        100,
                    )
                    self.allocated_usage_international_plan -= POOL_INCREASE_PER_DEVICE
                    self.total_international_plan_usage -= self.total_usage(
                        v.plans_to_usage_mapping
                    )
                # Otherwise assign to the European plan since most international usage is in
                # European countries.
                elif europeanPlanCost < intlPlanCost:
                    self.assign_rate_plan(
                        v.iccid,
                        v.new_rate_plan,
                        RATEPLAN_20GB_INTL_USAGE_TIER,
                        self.total_usage(v.plans_to_usage_mapping),
                        v.plans_to_usage_mapping,
                        100,
                    )
                    self.allocated_usage_international_plan -= POOL_INCREASE_PER_DEVICE
                    self.total_international_plan_usage -= self.total_usage(
                        v.plans_to_usage_mapping
                    )

    # Given that we are charged at a rate per device by AT&T for devices in the international plan, we want to have the
    # minimum number of devices in this plan as possible, which is (number of devices * 100 MB - total usage) < 100 MB.
    # This is because usage for devices in the international plan is pooled, meaning that each device allocated to the pool
    # can have its 100 MB used by other devices in the pool. If we have more MB used than allocated, we get penalized. So,
    # we need to reallocate devices from other rate plan assignments if needed based on order of preference.
    #
    # Alternatively, if allocation is way more than allocated international usage, then move devices which are dormant in
    # international plan to the NorAm rate plan which costs much less per device.
    def handle_international_pool_allocation(self):
        # If allocation is more than actual international usage, then move devices which are dormant in international plan
        # to the NorAm rate plan which costs must less per device.
        if (
            self.allocated_usage_international_plan
            > self.total_international_plan_usage
        ):
            self.reallocate_unused_intl_devices()

        # Reallocate ICCIDs if need be so that we don't face hefty overage charges for going over the pooled max usage
        # for the international plan.
        self.reallocate_iccids_by_preference()

        # Reallocate devices which are assigned the international plan, but actually cost less when put in another plan.
        # This is done here vs. when we are first iterating over all devices because we want to calculate cost, which
        # requires knowledge of total usage for all devices assigned a given rate plan.
        self.reallocate_low_usage_intl_plan_devices()

    def account_for_dormant_international_plan_devices(self):
        numDormantIntlDevices = len(self.rate_plan_iccid_sets[RATEPLAN_100MB_INTL_PLAN])
        self.allocated_usage_international_plan += (
            numDormantIntlDevices * POOL_INCREASE_PER_DEVICE
        )

    def print_unused_devices_by_rate_plan(self):
        for k, v in self.rate_plan_iccid_sets.items():
            if k in rate_plan_id_to_name:
                print("\t{0}: {1}".format(rate_plan_id_to_name[k], len(v)))

    def compute_cost_savings(self):
        cost = 0
        # For all devices for which we are going to change rate plans, figure out the amount the old rate plan
        # would have cost that device vs. the new rate plan, and find the difference to reflect the amount of
        # cost savings.
        for _, v in self.info_per_iccid.items():
            if v.old_rate_plan != v.new_rate_plan:
                oldCost = calculate_cost(
                    v.iccid,
                    v.old_rate_plan,
                    v.plans_to_usage_mapping,
                    self.usage_by_rate_plan[v.old_rate_plan],
                )
                newCost = calculate_cost(
                    v.iccid,
                    v.new_rate_plan,
                    v.plans_to_usage_mapping,
                    self.usage_by_rate_plan[v.new_rate_plan],
                )
                cost += oldCost - newCost
        return cost

    def compute_total_cost_per_rate_plan(self):
        cost = 0
        cost_per_rate_plan = defaultdict(int)

        for _, v in self.info_per_iccid.items():
            cost_per_rate_plan[v.new_rate_plan] += calculate_cost(
                v.iccid,
                v.new_rate_plan,
                v.plans_to_usage_mapping,
                self.usage_by_rate_plan[v.new_rate_plan],
            )

        for k, v in self.rate_plan_iccid_sets.items():
            for kk in v:
                cost_per_rate_plan[k] += calculate_subscription_cost(
                    k,
                    self.usage_by_rate_plan[k],
                )

        return cost_per_rate_plan

    def total_number_devices_per_rate_plan(self):
        print("Number of Devices per Rate Plan: ")
        devices_in_rate_plan = defaultdict(int)

        for _, v in self.info_per_iccid.items():
            devices_in_rate_plan[v.new_rate_plan] = (
                devices_in_rate_plan[v.new_rate_plan] + 1
            )

        for k, v in self.rate_plan_iccid_sets.items():
            devices_in_rate_plan[k] = devices_in_rate_plan[k] + len(v)

        for k in devices_in_rate_plan:
            if k in rate_plan_id_to_name:
                print(
                    "\t{0}: {1}".format(
                        rate_plan_id_to_name[k], devices_in_rate_plan[k]
                    )
                )

    def compute_total_subscription_cost_per_rate_plan(self):
        cost = 0
        cost_per_rate_plan = defaultdict(int)

        for _, v in self.info_per_iccid.items():
            cost_per_rate_plan[v.new_rate_plan] += calculate_subscription_cost(
                v.new_rate_plan,
                self.usage_by_rate_plan[v.new_rate_plan],
            )

        for k, v in self.rate_plan_iccid_sets.items():
            for kk in v:
                cost_per_rate_plan[k] += calculate_subscription_cost(
                    k,
                    self.usage_by_rate_plan[k],
                )

        return cost_per_rate_plan

    def print_subscription_cost_per_rate_plan(self):
        print("Subscription Cost Per Rate Plan")
        for k, v in self.compute_total_subscription_cost_per_rate_plan().items():
            if v != 0 and k in rate_plan_id_to_name:
                print("\t{0}: ${1}".format(rate_plan_id_to_name[k], round(v, 2)))

    def print_cost_per_rate_plan(self):
        print("Cost Per Rate Plan:")
        for k, v in self.compute_total_cost_per_rate_plan().items():
            if v != 0 and k in rate_plan_id_to_name:
                print("\t{0}: ${1}".format(rate_plan_id_to_name[k], round(v, 2)))

    def print_usage_per_rate_plan(self):
        print("Total Data Usage in MB Per Rate Plan")
        for k, v in self.usage_by_rate_plan.items():
            if v != 0 and k in rate_plan_id_to_name:
                print(
                    "\t{0}: {1} MB".format(
                        rate_plan_id_to_name[k], round(v / SIZEOFMB, 2)
                    )
                )

    def get_total_cost(self):
        cost = 0
        for _, v in self.compute_total_cost_per_rate_plan().items():
            cost += v
        return cost

    def print_stats_before_reallocation(self):
        print(
            "Total International Usage Before Intl Relloacation in Bytes: "
            + str(self.total_international_plan_usage)
        )
        print(
            "Allocated International Usage Before Intl Relloacation in Bytes: "
            + str(self.allocated_usage_international_plan)
        )
        print()

        print("Unused Devices Before Reallocation")
        self.print_unused_devices_by_rate_plan()
        print()

    def print_stats_after_reallocation(self):
        print("Unused Devices After Reallocation")
        self.print_unused_devices_by_rate_plan()
        print()

        # Now that we have moved all devices to the rate plans which they will be assigned, we can calculate
        # the cost / data usage for all rate plans, for all devices.
        print(
            "Cost Savings By Changing Rate Plans: ${0}".format(
                round(self.compute_cost_savings(), 2)
            )
        )

        print("Total Cost: ${0}".format(round(self.get_total_cost(), 2)))
        print()
        self.print_subscription_cost_per_rate_plan()
        print()

        print()
        self.print_cost_per_rate_plan()
        print()
        self.print_usage_per_rate_plan()

        print()
        self.total_number_devices_per_rate_plan()
        print()

        print(
            "Total International Usage After Intl Reallocation in Bytes: "
            + str(self.total_international_plan_usage)
        )
        print(
            "Allocated International Usage After Intl Reallocation in Bytes: "
            + str(self.allocated_usage_international_plan)
        )
        print()
        print(
            "Total Number of Device Plans to Change: " + str(len(self.iccids_to_change))
        )

        print(
            {
                k[1]: v
                for k, v in Counter(
                    jasper_optimization.iccids_to_change.values()
                ).items()
            }
        )

    def process_data(self, dataFrame):
        for row in dataFrame.collect():
            self.process_iccid(row)

        # We have iterated through all devices which have session-level data. Now we know which
        # devices which are activated but have had no data usage, and to which rate plan they are
        # allocated. We need to include these devices as part of the data allocation for the international
        # rate plan.
        self.account_for_dormant_international_plan_devices()

        self.print_stats_before_reallocation()
        self.handle_international_pool_allocation()
        self.print_stats_after_reallocation()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Query Dataprep Tables

# COMMAND ----------

from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import (
    col,
    map_from_entries,
    collect_list,
    collect_set,
    struct,
    sum,
    to_date,
    max,
)

# Returns 18th of prior month in format yyyy-mm-dd
def get_18th_prior_month(arbitrary_date):
    last_month = arbitrary_date + relativedelta(months=-1)
    return date(last_month.year, last_month.month, 18).strftime("%Y-%m-%d")


# Returns one month from today in yyyy-mm-dd
def get_prior_month(arbitrary_date):
    last_month = arbitrary_date + relativedelta(months=-1)
    return last_month.strftime("%Y-%m-%d")


def query_table(today):
    dd_int = int(today.strftime("%d"))
    end_date = today.strftime("%Y-%m-%d")

    # This is the last week of the billing cycle so we narrow our window
    if dd_int >= 11 and dd_int <= 18:
        start_date = get_18th_prior_month(today)
    else:
        start_date = get_prior_month(today)

    # Filter only the rows that are relevant based on timestamp from current date to one month prior (or 18th of prior month if between 11th and 18th). The dataframe returned has
    # iccid, data_usage_per_locale, assigned_rate_plan_id
    t1 = table("dataprep_cellular.att_daily_usage")
    t1 = (
        t1.filter(
            (col("record_received_date") > start_date)
            & (col("record_received_date") <= end_date)
        )
        .groupBy("iccid", "tap_code")
        .agg(sum("data_usage").alias("data_usage"))
        .groupBy("iccid")
        .agg(
            map_from_entries(collect_list(struct("tap_code", "data_usage"))).alias(
                "data_usage_per_locale"
            )
        )
    )

    # Get set of devices from the most recent subscriber snapshot which are activated (SIM state 6). We are only billed for activated SIMs.
    t2 = table("dataprep_cellular.att_subscriber_snapshot")
    t2 = (
        t2.select(col("iccid"), col("filedate"), col("assigned_rate_plan_id"))
        .filter(col("sim_state") == 6)
        .groupBy("iccid")
        .agg(
            max(struct("filedate", "assigned_rate_plan_id"))[
                "assigned_rate_plan_id"
            ].alias("assigned_rate_plan_id")
        )
    )

    df = t1.join(t2, on="iccid", how="inner")
    return df


def snapshot_data():
    # Get pairing of all ICCIDs --> rate plan and filter based off devices which are Activated (sim_state=6). We are only
    # billed for devices which are in Activated state, so we only need to change rate plans for devices in that state.
    #
    # Possible sim state values are:
    #
    # 6 = Activated
    # 7 = Deactivated
    # 8 = Retired
    # 9 = Inventory
    # 10 = Test Ready
    # 11 = Activation Ready
    t1 = table("dataprep_cellular.att_subscriber_snapshot")
    t1 = t1.select(max(col("filedate")).alias("latest_snapshot")).alias("latest")

    t2 = table("dataprep_cellular.att_subscriber_snapshot").alias("snapshots")

    df = t2.join(t1, col("latest.latest_snapshot") == col("snapshots.filedate"))
    snapshot = (
        df.select(col("iccid"), col("assigned_rate_plan_id"))
        .filter(col("sim_state") == 6)
        .groupBy("assigned_rate_plan_id")
        .agg(collect_set("iccid"))
        .rdd.collectAsMap()
    )
    return snapshot


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Run The Optimization Algorithm

# COMMAND ----------

jasper_optimization = JasperOptimization()
jasper_optimization.populate_dormant_iccids(snapshot_data())
df = query_table(datetime.utcnow().date())
jasper_optimization.process_data(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Run Unit Tests

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    MapType,
    StringType,
    LongType,
)
import unittest

"""
+--------------+-------+--------+---------------+-----------------------+--------------------------------------------------+
|              | NorAm | Europe | International | Outside International |                      Result                      |
+--------------+-------+--------+---------------+-----------------------+--------------------------------------------------+
| Test Case 1  | X     |        |               |                       | NorAm                                            |
| Test Case 2  |       | X      |               |                       | Europe                                           |
| Test Case 3  |       |        | X             |                       | International                                    |
| Test Case 4  |       |        |               | X                     | Europe                                           |
| Test Case 5  | X     | X      |               |                       | NorAm/Europe depending on more usage (not likely)|
| Test Case 6  |       | X      | X             |                       | International if Intl Usage > Cutoff, EU if less |
| Test Case 7  |       |        | X             | X                     | International if Intl Usage > Cutoff, EU if less |
| Test Case 8  | X     |        | X             |                       | International                                    |
| Test Case 9  | X     |        |               | X                     | NorAm                                            |
| Test Case 10 |       | X      |               | X                     | Europe                                           |
| Test Case 11 | X     | X      | X             |                       | NorAm/Europe depending on more usage (not likely)|
| Test Case 12 |       | X      | X             | X                     | Depends on Usage                                 |
| Test Case 13 | X     |        | X             | X                     | International                                    |
| Test Case 14 | X     | X      |               | X                     | NorAm/Europe depending on more usage (not likely)|
| Test Case 15 | X     | X      | X             | X                     | NorAm/Europe depending on more usage (not likely)|
| Test Case 16 |       |        |               |                       | Empty (No Changes)                               |
+--------------+-------+--------+---------------+-----------------------+--------------------------------------------------+
"""


class TestAlgorithmCases(unittest.TestCase):
    def setUp(self):
        self.test_jasper_optimization = JasperOptimization()
        self.schema = StructType(
            [
                StructField("assigned_rate_plan_id", LongType(), True),
                StructField(
                    "data_usage_per_locale",
                    MapType(StringType(), LongType(), True),
                    True,
                ),
                StructField("iccid", StringType(), True),
            ]
        )

    def test_cost_calculations_3g(self):
        ptum = defaultdict(int)
        ptum[NORAM] = 50000 * SIZEOFMB
        ptum[GROUP_A] = 1000 * SIZEOFMB
        cost_tier_1 = round(
            calculate_cost("1", RATEPLAN_0KB, ptum, 76800 * NUMBYTESINGIGABYTE), 2
        )
        self.assertEqual(1350.45, cost_tier_1)

        ptum = defaultdict(int)
        cost_no_usage = round(
            calculate_cost("1", RATEPLAN_0KB, ptum, 100 * NUMBYTESINGIGABYTE), 2
        )
        self.assertEqual(0.45, cost_no_usage)

    def test_cost_calcuations_noram_tier1(self):
        ptum = defaultdict(int)
        ptum[NORAM] = 50000 * SIZEOFMB
        ptum[GROUP_A] = 1000 * SIZEOFMB
        cost_tier_1 = round(
            calculate_cost(
                "1", RATEPLAN_76800GB_USCANMX_USAGE, ptum, 70000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(562.7, cost_tier_1)

        ptum = defaultdict(int)
        cost_no_usage = round(
            calculate_cost(
                "1", RATEPLAN_76800GB_USCANMX_USAGE, ptum, 70000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(0.2, cost_no_usage)

    def test_cost_calcuations_noram_tier2(self):
        ptum = defaultdict(int)
        ptum[NORAM] = 50000 * SIZEOFMB
        ptum[GROUP_A] = 1000 * SIZEOFMB
        cost_tier_2 = round(
            calculate_cost(
                "1", RATEPLAN_76800GB_USCANMX_USAGE, ptum, 100000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(562.7, cost_tier_2)

        ptum = defaultdict(int)
        cost_no_usage = round(
            calculate_cost(
                "1", RATEPLAN_76800GB_USCANMX_USAGE, ptum, 100000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(0.2, cost_no_usage)

    def test_cost_calcuations_noram_tier3(self):
        ptum = defaultdict(int)
        ptum[NORAM] = 50000 * SIZEOFMB
        ptum[GROUP_A] = 1000 * SIZEOFMB
        cost_tier_1 = round(
            calculate_cost(
                "1", RATEPLAN_76800GB_USCANMX_USAGE, ptum, 120000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(525.2, cost_tier_1)

        ptum = defaultdict(int)
        cost_no_usage = round(
            calculate_cost(
                "1", RATEPLAN_76800GB_USCANMX_USAGE, ptum, 120000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(0.2, cost_no_usage)

    def test_cost_calcuations_noram_tier4(self):
        ptum = defaultdict(int)
        ptum[NORAM] = 50000 * SIZEOFMB
        ptum[GROUP_A] = 1000 * SIZEOFMB
        cost_tier_1 = round(
            calculate_cost(
                "1", RATEPLAN_76800GB_USCANMX_USAGE, ptum, 175000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(475.2, cost_tier_1)

        ptum = defaultdict(int)
        cost_no_usage = round(
            calculate_cost(
                "1", RATEPLAN_76800GB_USCANMX_USAGE, ptum, 175000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(0.2, cost_no_usage)

    def test_cost_calcuations_noram_tier5(self):
        ptum = defaultdict(int)
        ptum[NORAM] = 50000 * SIZEOFMB
        ptum[GROUP_A] = 1000 * SIZEOFMB
        cost_tier_1 = round(
            calculate_cost(
                "1", RATEPLAN_76800GB_USCANMX_USAGE, ptum, 250000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(430.2, cost_tier_1)

        ptum = defaultdict(int)
        cost_no_usage = round(
            calculate_cost(
                "1", RATEPLAN_76800GB_USCANMX_USAGE, ptum, 250000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(0.20, cost_no_usage)

    def test_cost_calcuations_noram_tier6(self):
        ptum = defaultdict(int)
        ptum[NORAM] = 50000 * SIZEOFMB
        ptum[GROUP_A] = 1000 * SIZEOFMB
        cost_tier_1 = round(
            calculate_cost(
                "1", RATEPLAN_76800GB_USCANMX_USAGE, ptum, 350000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(407.7, cost_tier_1)

        ptum = defaultdict(int)
        cost_no_usage = round(
            calculate_cost(
                "1", RATEPLAN_76800GB_USCANMX_USAGE, ptum, 350000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(0.20, cost_no_usage)

    def test_cost_calcuations_europe_tier1(self):
        ptum = defaultdict(int)
        ptum[EUROPE] = 50000 * SIZEOFMB
        ptum[GROUP_A] = 1000 * SIZEOFMB
        cost_tier_1 = round(
            calculate_cost(
                "1", RATEPLAN_20GB_INTL_USAGE_TIER, ptum, 15000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(425.2, cost_tier_1)

        ptum = defaultdict(int)
        cost_no_usage = round(
            calculate_cost(
                "1", RATEPLAN_20GB_INTL_USAGE_TIER, ptum, 15000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(0.2, cost_no_usage)

    def test_cost_calcuations_europe_tier2(self):
        ptum = defaultdict(int)
        ptum[EUROPE] = 50000 * SIZEOFMB
        ptum[GROUP_A] = 1000 * SIZEOFMB
        cost_tier_1 = round(
            calculate_cost(
                "1", RATEPLAN_20GB_INTL_USAGE_TIER, ptum, 22000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(425.2, cost_tier_1)

        ptum = defaultdict(int)
        cost_no_usage = round(
            calculate_cost(
                "1", RATEPLAN_20GB_INTL_USAGE_TIER, ptum, 22000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(0.20, cost_no_usage)

    def test_cost_calcuations_europe_tier3(self):
        ptum = defaultdict(int)
        ptum[EUROPE] = 50000 * SIZEOFMB
        ptum[GROUP_A] = 1000 * SIZEOFMB
        cost_tier_1 = round(
            calculate_cost(
                "1", RATEPLAN_20GB_INTL_USAGE_TIER, ptum, 27000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(425.2, cost_tier_1)

        ptum = defaultdict(int)
        cost_no_usage = round(
            calculate_cost(
                "1", RATEPLAN_20GB_INTL_USAGE_TIER, ptum, 27000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(0.20, cost_no_usage)

    def test_cost_calcuations_europe_tier4(self):
        ptum = defaultdict(int)
        ptum[EUROPE] = 50000 * SIZEOFMB
        ptum[GROUP_A] = 1000 * SIZEOFMB
        cost_tier_1 = round(
            calculate_cost(
                "1", RATEPLAN_20GB_INTL_USAGE_TIER, ptum, 70000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(425.2, cost_tier_1)

        ptum = defaultdict(int)
        cost_no_usage = round(
            calculate_cost(
                "1", RATEPLAN_20GB_INTL_USAGE_TIER, ptum, 70000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(0.20, cost_no_usage)

    def test_cost_calculations_international(self):
        ptum = defaultdict(int)
        ptum[GROUP_A] = 50 * SIZEOFMB
        ptum[OUTSIDE_GROUP_A] = 20 * SIZEOFMB
        cost_tier_1 = round(
            calculate_cost(
                "1", RATEPLAN_100MB_INTL_PLAN, ptum, 70000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        # TODO we need to account for overages
        self.assertEqual(12.0, cost_tier_1)

        ptum = defaultdict(int)
        cost_no_usage = round(
            calculate_cost(
                "1", RATEPLAN_100MB_INTL_PLAN, ptum, 70000 * NUMBYTESINGIGABYTE
            ),
            2,
        )
        self.assertEqual(12.00, cost_no_usage)

    def check_per_iccid_tracking(self, iccid, desiredICCIDToChange):

        itcm = self.test_jasper_optimization.info_per_iccid[iccid]

        self.assertEqual(desiredICCIDToChange.iccid, itcm.iccid)

        self.assertEqual(desiredICCIDToChange.old_rate_plan, itcm.old_rate_plan)

        self.assertEqual(desiredICCIDToChange.new_rate_plan, itcm.new_rate_plan)

        self.assertEqual(
            desiredICCIDToChange.plans_to_usage_mapping[NORAM],
            itcm.plans_to_usage_mapping[NORAM],
        )

        self.assertEqual(
            desiredICCIDToChange.plans_to_usage_mapping[EUROPE],
            itcm.plans_to_usage_mapping[EUROPE],
        )

        self.assertEqual(
            desiredICCIDToChange.plans_to_usage_mapping[GROUP_A],
            itcm.plans_to_usage_mapping[GROUP_A],
        )

        self.assertEqual(
            desiredICCIDToChange.plans_to_usage_mapping[OUTSIDE_GROUP_A],
            itcm.plans_to_usage_mapping[OUTSIDE_GROUP_A],
        )

    def check_usage_by_rate_plan(self, rate_plan_map):
        self.assertEqual(
            rate_plan_map[RATEPLAN_76800GB_USCANMX_USAGE],
            self.test_jasper_optimization.usage_by_rate_plan[
                RATEPLAN_76800GB_USCANMX_USAGE
            ],
        )

        self.assertEqual(
            rate_plan_map[RATEPLAN_20GB_INTL_USAGE_TIER],
            self.test_jasper_optimization.usage_by_rate_plan[
                RATEPLAN_20GB_INTL_USAGE_TIER
            ],
        )

        self.assertEqual(
            rate_plan_map[RATEPLAN_100MB_INTL_PLAN],
            self.test_jasper_optimization.usage_by_rate_plan[RATEPLAN_100MB_INTL_PLAN],
        )

    def test_case_1(self):
        rows = []
        # Device only in US not correct plan
        rows.append(
            Row(iccid="1", assigned_rate_plan_id=1, data_usage_per_locale={"USA11": 5})
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_76800GB_USCANMX_USAGE,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 5
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_76800GB_USCANMX_USAGE, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_76800GB_USCANMX_USAGE] = 5
        self.check_usage_by_rate_plan(rpm)

    def test_case_2(self):
        rows = []
        # Device only in Europe (ScheduleB5) not correct plan
        rows.append(
            Row(iccid="1", assigned_rate_plan_id=1, data_usage_per_locale={"AUTMK": 5})
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_20GB_INTL_USAGE_TIER,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )

        ptum = defaultdict(int)
        ptum[EUROPE] = 5
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_20GB_INTL_USAGE_TIER, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_20GB_INTL_USAGE_TIER] = 5
        self.check_usage_by_rate_plan(rpm)

    def test_case_3(self):
        rows = []
        # Device only in International(GroupA) not correct plan
        rows.append(
            Row(iccid="1", assigned_rate_plan_id=1, data_usage_per_locale={"BRAC3": 5})
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_100MB_INTL_PLAN,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )

        ptum = defaultdict(int)
        ptum[GROUP_A] = 5
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_100MB_INTL_PLAN, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_100MB_INTL_PLAN] = 5
        self.check_usage_by_rate_plan(rpm)

    def test_case_4(self):
        rows = []
        # Device only in outside other 3 categories not correct plan
        rows.append(
            Row(iccid="1", assigned_rate_plan_id=1, data_usage_per_locale={"GTMCM": 5})
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_20GB_INTL_USAGE_TIER,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )

        ptum = defaultdict(int)
        ptum[OUTSIDE_GROUP_A] = 5
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_20GB_INTL_USAGE_TIER, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_20GB_INTL_USAGE_TIER] = 5
        self.check_usage_by_rate_plan(rpm)

    def test_case_5(self):
        rows = []
        # Device in US and EU which greater usage in US not correct plan
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"USA11": 10, "AUTMK": 5},
            )
        )
        # Device in US and EU which greater usage in EU not correct plan
        rows.append(
            Row(
                iccid="2",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"USA11": 5, "AUTMK": 10},
            )
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_76800GB_USCANMX_USAGE,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )
        self.assertEqual(
            RATEPLAN_20GB_INTL_USAGE_TIER,
            self.test_jasper_optimization.iccids_to_change["2"][0],
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 10
        ptum[EUROPE] = 5
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_76800GB_USCANMX_USAGE, ptum)
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 5
        ptum[EUROPE] = 10
        self.check_per_iccid_tracking(
            "2", ICCIDToChangeMetadata("2", 1, RATEPLAN_20GB_INTL_USAGE_TIER, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_76800GB_USCANMX_USAGE] = 15
        rpm[RATEPLAN_20GB_INTL_USAGE_TIER] = 15
        self.check_usage_by_rate_plan(rpm)

    def test_case_6(self):
        rows = []
        # Device in EU and GroupA country more than cutoff
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"AUTMK": 5, "BRAC3": 45000000},
            )
        )
        # Device in EU and GroupA country less than cutoff
        rows.append(
            Row(
                iccid="2",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"AUTMK": 5, "BRAC3": 10},
            )
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_100MB_INTL_PLAN,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )
        self.assertEqual(
            RATEPLAN_20GB_INTL_USAGE_TIER,
            self.test_jasper_optimization.iccids_to_change["2"][0],
        )

        ptum = defaultdict(int)
        ptum[EUROPE] = 5
        ptum[GROUP_A] = 45000000
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_100MB_INTL_PLAN, ptum)
        )

        ptum = defaultdict(int)
        ptum[EUROPE] = 5
        ptum[GROUP_A] = 10
        self.check_per_iccid_tracking(
            "2", ICCIDToChangeMetadata("2", 1, RATEPLAN_20GB_INTL_USAGE_TIER, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_100MB_INTL_PLAN] = 45000005
        rpm[RATEPLAN_20GB_INTL_USAGE_TIER] = 15
        self.check_usage_by_rate_plan(rpm)

    def test_case_7(self):
        rows = []
        # Device in GroupA and outside Group A country more than cutoff
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"GTMCM": 5, "BRAC3": 45000000},
            )
        )
        # Device in GroupA and outside Group A country less than cutoff
        rows.append(
            Row(
                iccid="2",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"GTMCM": 5, "BRAC3": 10},
            )
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_100MB_INTL_PLAN,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )
        self.assertEqual(
            RATEPLAN_20GB_INTL_USAGE_TIER,
            self.test_jasper_optimization.iccids_to_change["2"][0],
        )

        ptum = defaultdict(int)
        ptum[OUTSIDE_GROUP_A] = 5
        ptum[GROUP_A] = 45000000
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_100MB_INTL_PLAN, ptum)
        )

        ptum = defaultdict(int)
        ptum[OUTSIDE_GROUP_A] = 5
        ptum[GROUP_A] = 10
        self.check_per_iccid_tracking(
            "2", ICCIDToChangeMetadata("2", 1, RATEPLAN_20GB_INTL_USAGE_TIER, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_100MB_INTL_PLAN] = 45000005
        rpm[RATEPLAN_20GB_INTL_USAGE_TIER] = 15
        self.check_usage_by_rate_plan(rpm)

    def test_case_8(self):
        rows = []
        # Device in NorAm and International(GroupA)
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"USA11": 5, "BRAC3": 10},
            )
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_100MB_INTL_PLAN,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 5
        ptum[GROUP_A] = 10
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_100MB_INTL_PLAN, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_100MB_INTL_PLAN] = 15
        self.check_usage_by_rate_plan(rpm)

    def test_case_9(self):
        rows = []
        # Device in NorAm and Outside GroupA
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"USA11": 5, "GTMCM": 10},
            )
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_76800GB_USCANMX_USAGE,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 5
        ptum[OUTSIDE_GROUP_A] = 10
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_76800GB_USCANMX_USAGE, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_76800GB_USCANMX_USAGE] = 15
        self.check_usage_by_rate_plan(rpm)

    def test_case_10(self):
        rows = []
        # Device in EU and outside Group A
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"AUTMK": 5, "GTMCM": 10},
            )
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_20GB_INTL_USAGE_TIER,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )

        ptum = defaultdict(int)
        ptum[EUROPE] = 5
        ptum[OUTSIDE_GROUP_A] = 10
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_20GB_INTL_USAGE_TIER, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_20GB_INTL_USAGE_TIER] = 15
        self.check_usage_by_rate_plan(rpm)

    def test_case_11(self):
        # Device in Noram, EU, International
        rows = []
        # Device in US, EU, International with greater usage in US not correct plan
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"USA11": 10, "AUTMK": 5, "BRAC3": 10},
            )
        )
        # Device in US, EU, International with greater usage in EU not correct plan
        rows.append(
            Row(
                iccid="2",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"USA11": 5, "AUTMK": 10, "BRAC3": 10},
            )
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_76800GB_USCANMX_USAGE,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )
        self.assertEqual(
            RATEPLAN_20GB_INTL_USAGE_TIER,
            self.test_jasper_optimization.iccids_to_change["2"][0],
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 10
        ptum[EUROPE] = 5
        ptum[GROUP_A] = 10
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_76800GB_USCANMX_USAGE, ptum)
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 5
        ptum[EUROPE] = 10
        ptum[GROUP_A] = 10
        self.check_per_iccid_tracking(
            "2", ICCIDToChangeMetadata("2", 1, RATEPLAN_20GB_INTL_USAGE_TIER, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_76800GB_USCANMX_USAGE] = 25
        rpm[RATEPLAN_20GB_INTL_USAGE_TIER] = 25
        self.check_usage_by_rate_plan(rpm)

    def test_case_12(self):
        rows = []
        # Device in EU, GroupA, Outside GroupA country more than cutoff
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"AUTMK": 5, "BRAC3": 45000000, "GTMCM": 10},
            )
        )
        # Device in EU, GroupA, Outside GroupA country less than cutoff
        rows.append(
            Row(
                iccid="2",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"AUTMK": 5, "BRAC3": 10, "GTMCM": 10},
            )
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_100MB_INTL_PLAN,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )
        self.assertEqual(
            RATEPLAN_20GB_INTL_USAGE_TIER,
            self.test_jasper_optimization.iccids_to_change["2"][0],
        )

        ptum = defaultdict(int)
        ptum[EUROPE] = 5
        ptum[GROUP_A] = 45000000
        ptum[OUTSIDE_GROUP_A] = 10
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_100MB_INTL_PLAN, ptum)
        )

        ptum = defaultdict(int)
        ptum[EUROPE] = 5
        ptum[GROUP_A] = 10
        ptum[OUTSIDE_GROUP_A] = 10
        self.check_per_iccid_tracking(
            "2", ICCIDToChangeMetadata("2", 1, RATEPLAN_20GB_INTL_USAGE_TIER, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_100MB_INTL_PLAN] = 45000015
        rpm[RATEPLAN_20GB_INTL_USAGE_TIER] = 25
        self.check_usage_by_rate_plan(rpm)

    def test_case_13(self):
        rows = []
        # Device in NorAm, International(GroupA), Outside International
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"USA11": 5, "BRAC3": 10, "GTMCM": 10},
            )
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_100MB_INTL_PLAN,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 5
        ptum[GROUP_A] = 10
        ptum[OUTSIDE_GROUP_A] = 10
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_100MB_INTL_PLAN, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_100MB_INTL_PLAN] = 25
        self.check_usage_by_rate_plan(rpm)

    def test_case_14(self):
        # Device in NorAm, EU and Outside Group A
        rows = []
        # Device in US, EU, Outside Group A with greater usage in US not correct plan
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"USA11": 10, "AUTMK": 5, "GTMCM": 10},
            )
        )
        # Device in US, EU, Outside Group A with greater usage in EU not correct plan
        rows.append(
            Row(
                iccid="2",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"USA11": 5, "AUTMK": 10, "GTMCM": 10},
            )
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_76800GB_USCANMX_USAGE,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )
        self.assertEqual(
            RATEPLAN_20GB_INTL_USAGE_TIER,
            self.test_jasper_optimization.iccids_to_change["2"][0],
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 10
        ptum[EUROPE] = 5
        ptum[OUTSIDE_GROUP_A] = 10
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_76800GB_USCANMX_USAGE, ptum)
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 5
        ptum[EUROPE] = 10
        ptum[OUTSIDE_GROUP_A] = 10
        self.check_per_iccid_tracking(
            "2", ICCIDToChangeMetadata("2", 1, RATEPLAN_20GB_INTL_USAGE_TIER, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_76800GB_USCANMX_USAGE] = 25
        rpm[RATEPLAN_20GB_INTL_USAGE_TIER] = 25
        self.check_usage_by_rate_plan(rpm)

    def test_case_15(self):
        # Device in NorAm, EU, GroupA and Outside Group A
        rows = []
        # Device in NorAm, EU, GroupA and Outside Group A with greater usage in US not correct plan
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={
                    "USA11": 10,
                    "AUTMK": 5,
                    "BRAC3": 10,
                    "GTMCM": 10,
                },
            )
        )
        # Device in NorAm, EU, GroupA and Outside Group A with greater usage in EU not correct plan
        rows.append(
            Row(
                iccid="2",
                assigned_rate_plan_id=1,
                data_usage_per_locale={
                    "USA11": 5,
                    "AUTMK": 10,
                    "BRAC3": 10,
                    "GTMCM": 10,
                },
            )
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            RATEPLAN_76800GB_USCANMX_USAGE,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )
        self.assertEqual(
            RATEPLAN_20GB_INTL_USAGE_TIER,
            self.test_jasper_optimization.iccids_to_change["2"][0],
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 10
        ptum[EUROPE] = 5
        ptum[GROUP_A] = 10
        ptum[OUTSIDE_GROUP_A] = 10
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_76800GB_USCANMX_USAGE, ptum)
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 5
        ptum[EUROPE] = 10
        ptum[GROUP_A] = 10
        ptum[OUTSIDE_GROUP_A] = 10
        self.check_per_iccid_tracking(
            "2", ICCIDToChangeMetadata("2", 1, RATEPLAN_20GB_INTL_USAGE_TIER, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_76800GB_USCANMX_USAGE] = 35
        rpm[RATEPLAN_20GB_INTL_USAGE_TIER] = 35
        self.check_usage_by_rate_plan(rpm)

    def test_case_16(self):
        self.test_jasper_optimization.process_data(
            spark.createDataFrame([], self.schema)
        )
        self.assertEqual(len(self.test_jasper_optimization.iccids_to_change), 0)
        self.assertEqual(len(self.test_jasper_optimization.info_per_iccid), 0)

    def test_3G(self):
        rows = []
        # Device on 3G and not roaming United States
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=RATEPLAN_0KB,
                data_usage_per_locale={"USA11": 5},
            )
        )
        # Device on 3G and roaming United States
        rows.append(
            Row(
                iccid="2",
                assigned_rate_plan_id=RATEPLAN_0KB,
                data_usage_per_locale={"USA11": 5, "GTMCM": 10},
            )
        )
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertNotIn("1", self.test_jasper_optimization.iccids_to_change)
        self.assertEqual(
            RATEPLAN_100MB_INTL_PLAN,
            self.test_jasper_optimization.iccids_to_change["2"][0],
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 5
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", RATEPLAN_0KB, RATEPLAN_0KB, ptum)
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 5
        ptum[OUTSIDE_GROUP_A] = 10
        self.check_per_iccid_tracking(
            "2",
            ICCIDToChangeMetadata("2", RATEPLAN_0KB, RATEPLAN_100MB_INTL_PLAN, ptum),
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_0KB] = 5
        rpm[RATEPLAN_100MB_INTL_PLAN] = 15
        self.check_usage_by_rate_plan(rpm)

    def test_process_tap_code(self):
        # Device in NorAm, EU, GroupA and Outside Group A
        row = Row(
            iccid="1",
            assigned_rate_plan_id=1,
            data_usage_per_locale={"USA11": 10, "AUTMK": 10, "BRAC3": 10, "GTMCM": 10},
        )
        mapping = self.test_jasper_optimization.process_tap_codes(row)
        self.assertEqual(10, mapping[NORAM])
        self.assertEqual(10, mapping[EUROPE])
        self.assertEqual(10, mapping[GROUP_A])
        self.assertEqual(10, mapping[OUTSIDE_GROUP_A])

        # Device with invalid tap_code
        row = Row(iccid="2", assigned_rate_plan_id=1, data_usage_per_locale={"156": 10})
        mapping = self.test_jasper_optimization.process_tap_codes(row)
        self.assertEqual(0, len(mapping))

        # Device with invalid tap_code and correct tap_code
        row = Row(
            iccid="3",
            assigned_rate_plan_id=1,
            data_usage_per_locale={"156": 10, "USA11": 10},
        )
        mapping = self.test_jasper_optimization.process_tap_codes(row)
        self.assertEqual(10, mapping[NORAM])
        self.assertEqual(1, len(mapping))

    def test_international_states_plan_switches(self):
        rows = []
        # Device 1 uses 190 MB and pool only allocated 100 MB, Should take device 2 and switch to international

        # Device in GroupA country more than cutoff so should be international
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"BRAC3": 190 * SIZEOFMB},
            )
        )
        # Device in EU and GroupA country less than cutoff -> Should go to EU Normally
        rows.append(
            Row(
                iccid="2",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"AUTMK": 5, "BRAC3": 10},
            )
        )
        # Device in USA should still be NorAm
        rows.append(
            Row(
                iccid="3",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"USA11": 90 * SIZEOFMB},
            )
        )

        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))
        self.assertEqual(
            190 * SIZEOFMB + 15,
            self.test_jasper_optimization.total_international_plan_usage,
        )
        self.assertEqual(
            200 * SIZEOFMB,
            self.test_jasper_optimization.allocated_usage_international_plan,
        )
        self.assertEqual(
            RATEPLAN_100MB_INTL_PLAN,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )
        self.assertEqual(
            RATEPLAN_100MB_INTL_PLAN,
            self.test_jasper_optimization.iccids_to_change["2"][0],
        )
        self.assertEqual(
            RATEPLAN_76800GB_USCANMX_USAGE,
            self.test_jasper_optimization.iccids_to_change["3"][0],
        )

        ptum = defaultdict(int)
        ptum[GROUP_A] = 190 * SIZEOFMB
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_100MB_INTL_PLAN, ptum)
        )

        ptum = defaultdict(int)
        ptum[EUROPE] = 5
        ptum[GROUP_A] = 10
        self.check_per_iccid_tracking(
            "2", ICCIDToChangeMetadata("2", 1, RATEPLAN_100MB_INTL_PLAN, ptum)
        )

        ptum = defaultdict(int)
        ptum[NORAM] = 90 * SIZEOFMB
        self.check_per_iccid_tracking(
            "3", ICCIDToChangeMetadata("3", 1, RATEPLAN_76800GB_USCANMX_USAGE, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_100MB_INTL_PLAN] = 190 * SIZEOFMB + 15
        rpm[RATEPLAN_76800GB_USCANMX_USAGE] = 90 * SIZEOFMB
        self.check_usage_by_rate_plan(rpm)

    def test_international_dormant_reallocation(self):
        # Only have international dormant device, assert that there is no allocated data usage
        self.test_jasper_optimization.rate_plan_iccid_sets[
            RATEPLAN_100MB_INTL_PLAN
        ].add("1")
        self.test_jasper_optimization.process_data(
            spark.createDataFrame([], self.schema)
        )
        self.assertEqual(
            0, self.test_jasper_optimization.allocated_usage_international_plan
        )

        ptum = defaultdict(int)
        self.check_per_iccid_tracking(
            "1",
            ICCIDToChangeMetadata(
                "1", RATEPLAN_100MB_INTL_PLAN, RATEPLAN_76800GB_USCANMX_USAGE, ptum
            ),
        )

        rpm = defaultdict(int)
        self.check_usage_by_rate_plan(rpm)

    def test_international_dormant_reallocation_to_noram(self):
        rows = []
        # Allocates device to International Plan with 50 MB
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"BRAC3": 50 * SIZEOFMB},
            )
        )
        # Dormant international plan
        self.test_jasper_optimization.rate_plan_iccid_sets[
            RATEPLAN_100MB_INTL_PLAN
        ].add("2")
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))

        # We have 200 MB allocated for two devices on international but one is not using
        # any data and moving dormant device would still have a large enough pool based on current
        # usage so cheaper to move it to noram plan
        self.assertEqual(
            RATEPLAN_100MB_INTL_PLAN,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )
        self.assertEqual(
            RATEPLAN_76800GB_USCANMX_USAGE,
            self.test_jasper_optimization.iccids_to_change["2"][0],
        )

        ptum = defaultdict(int)
        ptum[GROUP_A] = 50 * SIZEOFMB
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_100MB_INTL_PLAN, ptum)
        )

        ptum = defaultdict(int)
        self.check_per_iccid_tracking(
            "2",
            ICCIDToChangeMetadata(
                "2", RATEPLAN_100MB_INTL_PLAN, RATEPLAN_76800GB_USCANMX_USAGE, ptum
            ),
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_100MB_INTL_PLAN] = 50 * SIZEOFMB
        self.check_usage_by_rate_plan(rpm)

    def test_international_dormant_reallocation_no_change(self):
        rows = []
        # Allocates device to International Plan with 50 MB
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"BRAC3": 150 * SIZEOFMB},
            )
        )
        # Dormant international plan
        self.test_jasper_optimization.rate_plan_iccid_sets[
            RATEPLAN_100MB_INTL_PLAN
        ].add("2")
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))

        # We have 200 MB allocated for two devices on international and one is not using
        # any data but moving dormant device would not have a large enough pool based on current
        # usage so don't change it
        self.assertEqual(
            RATEPLAN_100MB_INTL_PLAN,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )
        self.assertNotIn("2", self.test_jasper_optimization.iccids_to_change)

        ptum = defaultdict(int)
        ptum[GROUP_A] = 150 * SIZEOFMB
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_100MB_INTL_PLAN, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_100MB_INTL_PLAN] = 150 * SIZEOFMB
        self.check_usage_by_rate_plan(rpm)

    def test_usage_more_than_allocation(self):
        rows = []
        # Allocates device to International Plan with 50 MB
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"BRAC3": 150 * SIZEOFMB},
            )
        )
        # Allocates device to Europe Plan with 5 MB
        rows.append(
            Row(
                iccid="3",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"AUTMK": 5 * SIZEOFMB},
            )
        )
        # Dormant NorAm plan device
        self.test_jasper_optimization.rate_plan_iccid_sets[
            RATEPLAN_76800GB_USCANMX_USAGE
        ].add("2")
        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))

        # We have 100 MB allocated for one device on international. We have a dormant
        # noram device and another device that should go to EU. We preference moving
        # a dormant Noram device to the international plan to increase the pool over the
        # EU device.
        self.assertEqual(
            RATEPLAN_100MB_INTL_PLAN,
            self.test_jasper_optimization.iccids_to_change["1"][0],
        )
        self.assertEqual(
            RATEPLAN_100MB_INTL_PLAN,
            self.test_jasper_optimization.iccids_to_change["2"][0],
        )
        self.assertEqual(
            RATEPLAN_20GB_INTL_USAGE_TIER,
            self.test_jasper_optimization.iccids_to_change["3"][0],
        )

        ptum = defaultdict(int)
        ptum[GROUP_A] = 150 * SIZEOFMB
        self.check_per_iccid_tracking(
            "1", ICCIDToChangeMetadata("1", 1, RATEPLAN_100MB_INTL_PLAN, ptum)
        )

        ptum = defaultdict(int)
        self.check_per_iccid_tracking(
            "2",
            ICCIDToChangeMetadata(
                "2", RATEPLAN_76800GB_USCANMX_USAGE, RATEPLAN_100MB_INTL_PLAN, ptum
            ),
        )

        ptum = defaultdict(int)
        ptum[EUROPE] = 5 * SIZEOFMB
        self.check_per_iccid_tracking(
            "3", ICCIDToChangeMetadata("3", 1, RATEPLAN_20GB_INTL_USAGE_TIER, ptum)
        )

        rpm = defaultdict(int)
        rpm[RATEPLAN_100MB_INTL_PLAN] = 150 * SIZEOFMB
        rpm[RATEPLAN_20GB_INTL_USAGE_TIER] = 5 * SIZEOFMB
        self.check_usage_by_rate_plan(rpm)

    def test_calculate_costs(self):
        rows = []

        # NorAm plan
        rows.append(
            Row(
                iccid="1",
                assigned_rate_plan_id=1,
                data_usage_per_locale={"USA11": 50000 * SIZEOFMB},
            )
        )
        # European plan
        rows.append(
            Row(
                iccid="2",
                assigned_rate_plan_id=RATEPLAN_76800GB_USCANMX_USAGE,
                data_usage_per_locale={"AUTMK": 50000 * SIZEOFMB},
            )
        )
        # International plan
        rows.append(
            Row(
                iccid="3",
                assigned_rate_plan_id=RATEPLAN_76800GB_USCANMX_USAGE,
                data_usage_per_locale={"BRAC3": 100 * SIZEOFMB},
            )
        )

        self.test_jasper_optimization.process_data(spark.createDataFrame(rows))

        costs_per_rate_plan = (
            self.test_jasper_optimization.compute_total_cost_per_rate_plan()
        )

        self.assertEqual(
            175.2, round(costs_per_rate_plan[RATEPLAN_20GB_INTL_USAGE_TIER], 2)
        )

        self.assertEqual(
            312.7, round(costs_per_rate_plan[RATEPLAN_76800GB_USCANMX_USAGE], 2)
        )

        self.assertEqual(12.0, round(costs_per_rate_plan[RATEPLAN_100MB_INTL_PLAN], 2))

        cost_savings = self.test_jasper_optimization.compute_cost_savings()

        self.assertEqual(12025.497917440001, cost_savings)


suite = unittest.TestLoader().loadTestsFromTestCase(TestAlgorithmCases)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Upload Results to S3

# COMMAND ----------

from datetime import datetime
import time
import boto3
from json import JSONEncoder


class JasperEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, JasperOptimization):
            o.rate_plan_iccid_sets = {
                k: list(v) for k, v in o.rate_plan_iccid_sets.items()
            }
        return o.__dict__


def upload_to_s3(opto):

    data = [
        {"iccid": k, "rate_plan": v[0], "rate_plan_name": v[1]}
        for k, v in opto.iccids_to_change.items()
    ]
    s3 = get_s3_resource("samsara-jasper-rate-plan-optimization-readwrite")
    bucket = "samsara-jasper-rate-plan-optimization"

    # Update the file `most_recent.json` which is read downstream by the service
    # which will update the rate plans in Jasper.
    obj = s3.Object(bucket, "most_recent.json")
    obj.put(Body=json.dumps(data), ACL="bucket-owner-full-control")

    # Write another copy of the file which will never be overwritten for historical purposes.
    millis = int(round(time.time() * 1000))
    year_month = datetime.utcnow().date().strftime("%Y-%m")

    obj2 = s3.Object(bucket, "month={0}/run={1}/output.json".format(year_month, millis))
    obj2.put(Body=json.dumps(data), ACL="bucket-owner-full-control")

    # Write the entire jasper optimization structure for historical purposes as well.
    obj3 = s3.Object(
        bucket, "month={0}/run={1}/optimization_dump.json".format(year_month, millis)
    )
    obj3.put(
        Body=json.dumps(opto, indent=4, cls=JasperEncoder),
        ACL="bucket-owner-full-control",
    )


upload_to_s3(jasper_optimization)
