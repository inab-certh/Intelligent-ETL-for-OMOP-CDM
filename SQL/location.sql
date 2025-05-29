-- BEGIN STEP: 0
CREATE OR REPLACE VIEW omop.location_enriched_view AS
SELECT 
  p.city,
  p.state,
  p.zip,
  sm.state_abbreviation
FROM 
  synthea.patients p
  LEFT JOIN omop.states_map sm ON p.state = sm.state;
-- END STEP: 0

-- BEGIN STEP: 1
INSERT INTO omop.location (
  location_id,
  city,
  state,
  location_source_value,
  address_1,
  address_2,
  county,
  zip
)
SELECT 
  MD5(p.city || p.state_abbreviation || p.zip)::uuid,
  p.city,
  p.state_abbreviation,
  p.zip,
  NULL::VARCHAR,
  NULL::VARCHAR,
  NULL::VARCHAR,
  p.zip
FROM 
  omop.location_enriched_view;
-- END STEP: 1

