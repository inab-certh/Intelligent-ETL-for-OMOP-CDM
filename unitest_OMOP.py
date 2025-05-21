import psycopg2

## Generic tests

def check_primary_key_uniqueness(conn, table, pk_column):
    query = f"SELECT COUNT(*) - COUNT(DISTINCT {pk_column}) FROM {table};"
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()[0]
    if result == 0:
        return f"Test completed successfully."
    else:
        return f"Test failed: Column '{pk_column}' in table '{table}' contains {result} duplicate values."

def check_foreign_key_existence(conn, child_table, fk_column, parent_table, parent_pk_column):
    query = f"""
    SELECT COUNT(*) FROM {child_table} c
    LEFT JOIN {parent_table} p ON c.{fk_column} = p.{parent_pk_column}
    WHERE p.{parent_pk_column} IS NULL;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()[0]
    if result == 0:
        return f"Test completed successfully."
    else:
        return f"Test failed: Found {result} orphan records in '{child_table}.{fk_column}' not present in '{parent_table}.{parent_pk_column}'."

def check_logical_date_order(conn, table, start_date_column, end_date_column):
    query = f"""
    SELECT COUNT(*) FROM {table}
    WHERE {start_date_column} > {end_date_column};
    """
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()[0]
    if result == 0:
        return f"Test completed successfully."
    else:
        return f"Test failed: Found {result} records in '{table}' where '{start_date_column}' is after '{end_date_column}'."


def check_concept_domain_consistency(conn, table, concept_column, expected_domain):
    query = f"""
    SELECT COUNT(*) FROM {table} t
    JOIN omop.concept c ON t.{concept_column} = c.concept_id
    WHERE c.domain_id <> %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (expected_domain,))
        result = cur.fetchone()[0]
    if result == 0:
        return f"Test completed successfully."
    else:
        return (
            f"Test failed: Found {result} concept IDs in column '{concept_column}' of table '{table}' "
            f"that do not belong to the expected domain '{expected_domain}'."
        )
    
def check_location_duplicates(conn):
    query = """
    SELECT address_1, city, state, zip, COUNT(*) AS n 
    FROM omop.location 
    GROUP BY address_1, city, state, zip 
    HAVING COUNT(*) > 1;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        duplicates = cur.fetchall()

    if len(duplicates) == 0:
        return "Test completed successfully."
    else:
        return (
            f"Test failed: Found {len(duplicates)} duplicate entries in the 'location' table "
            f"with the same combination of address_1, city, state, and zip."
        )

def check_care_site_location_fk(conn):
    query = """
    SELECT COUNT(*) 
    FROM omop.care_site cs 
    LEFT JOIN omop.location l ON cs.location_id = l.location_id 
    WHERE cs.location_id IS NOT NULL AND l.location_id IS NULL;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()[0]

    if result == 0:
        return "Test completed successfully."
    else:
        return (
            f"Test failed: Found {result} records in 'care_site' with 'location_id' "
            f"that does not exist in the 'location' table."
        )

def check_provider_specialty_domain(conn):
    query = """
    SELECT COUNT(*) 
    FROM omop.provider p 
    JOIN omop.concept c ON p.specialty_concept_id = c.concept_id 
    WHERE c.domain_id NOT IN ('Provider', 'Specialty');
    """
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()[0]

    if result == 0:
        return "Test completed successfully."
    else:
        return (
            f"Test failed: Found {result} specialty_concept_id values in 'provider' "
            f"that do not belong to the 'Provider' or 'Specialty' concept domains."
        )


def check_birth_year_plausibility(conn):
    query = """
    SELECT COUNT(*) 
    FROM omop.person 
    WHERE year_of_birth < 1900 
       OR year_of_birth > EXTRACT(YEAR FROM CURRENT_DATE);
    """
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()[0]

    if result == 0:
        return "Test completed successfully."
    else:
        return (
            f"Test failed: Found {result} records in 'person' with a year_of_birth before 1900 "
            f"or after the current year."
        )

def check_visit_within_observation_period(conn):
    query = """
    SELECT COUNT(*) 
    FROM omop.visit_occurrence v
    JOIN omop.observation_period o ON v.person_id = o.person_id
    WHERE v.visit_start_date < o.observation_period_start_date
       OR v.visit_end_date > o.observation_period_end_date;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()[0]

    if result == 0:
        return "Test completed successfully."
    else:
        return (
            f"Test failed: Found {result} visits in 'visit_occurrence' that fall outside the person's observation period."
        )

def check_drug_exposure_day_supply_consistency(conn):
    query = """
    SELECT COUNT(*) 
    FROM omop.drug_exposure 
    WHERE day_supply IS NOT NULL 
      AND drug_exposure_end_date <> drug_exposure_start_date + (day_supply - 1);
    """
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()[0]

    if result == 0:
        return "Test completed successfully."
    else:
        return (
            f"Test failed: Found {result} drug exposure records where 'end_date' does not match 'start_date + day_supply - 1'."
        )

def check_measurement_unit_domain(conn):
    query = """
    SELECT COUNT(*) 
    FROM omop.measurement m
    JOIN omop.concept u ON m.unit_concept_id = u.concept_id
    WHERE m.value_as_number IS NOT NULL AND u.domain_id <> 'Unit';
    """
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()[0]

    if result == 0:
        return "Test completed successfully."
    else:
        return (
            f"Test failed: Found {result} measurement records where 'value_as_number' is set "
            f"but 'unit_concept_id' is not in the 'Unit' domain."
        )

def check_observation_type_not_null(conn):
    query = """
    SELECT COUNT(*) 
    FROM omop.observation 
    WHERE observation_type_concept_id IS NULL;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()[0]

    if result == 0:
        return "Test completed successfully."
    else:
        return (
            f"Test failed: Found {result} records in 'observation' with NULL 'observation_type_concept_id'."
        )



check_concept_domain_consistency(conn, "person", "gender_concept_id", "Gender")
check_logical_date_order(conn, "observation_period", "observation_period_start_date", "observation_period_end_date")
check_concept_domain_consistency(conn, "condition_occurrence", "condition_concept_id", "Condition")
check_concept_domain_consistency(conn, "procedure_occurrence", "procedure_concept_id", "Procedure")
check_concept_domain_consistency(conn, "device_exposure", "device_concept_id", "Device")
check_concept_domain_consistency(conn, "drug_exposure", "drug_concept_id", "Drug")
check_logical_date_order(conn, "payer_plan_period", "payer_plan_period_start_date", "payer_plan_period_end_date")