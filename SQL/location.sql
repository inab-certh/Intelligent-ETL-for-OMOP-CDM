
-- BEGIN STEP: 0
-- Section 0 start

CREATE VIEW location_enriched_view AS
SELECT 
  p.city,
  p.state,
  p.zip,
  sm.state_abbreviation
FROM 
  synthea_source.patients p
  LEFT JOIN omop.state_map sm ON p.state = sm.state;

-- Section 0 end

-- END STEP: 0

-- BEGIN STEP: 1
-- Section 1 start

INSERT INTO omop.location (
  location_id,
  city,
  state,
  zip,
  location_source_value,
  address_1,
  address_2,
  county
)
SELECT 
  MD5(city || state_abbreviation || zip) AS location_id,
  city,
  state_abbreviation AS state,
  zip,
  zip AS location_source_value,
  CAST(NULL AS VARCHAR) AS address_1,
  CAST(NULL AS VARCHAR) AS address_2,
  CAST(NULL AS VARCHAR) AS county
FROM 
  location_enriched_view;

-- Section 1 end

-- END STEP: 1

-- BEGIN STEP: 0
-- Section 0 start

CREATE VIEW location_enriched_view AS
SELECT 
    p.city,
    p.state,
    p.zip,
    sm.state_abbreviation
FROM 
    synthea_source.patients p
LEFT JOIN 
    omop.state_map sm ON p.state = sm.state;

-- Section 0 end
-- END STEP: 0

