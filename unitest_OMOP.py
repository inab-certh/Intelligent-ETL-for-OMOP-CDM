import psycopg2


def unittest_location_table(user="admin", password="adminpassword", host="localhost", port="5432", database="synthea"):
    try:
        # Connexion à la base de données
        conn = psycopg2.connect(
            dbname=database,
            user=user,
            password=password,
            host=host,
            port=port
        )
        
        # Test 1: Vérifier l'unicité de la clé primaire location_id
        pk_result = check_primary_key_uniqueness(conn, "omop.location", "location_id")
        
        if "failed" in pk_result:
            return f"Primary key test failed: {pk_result}. Fix duplicate location_id values before proceeding."
        
        # Test 2: Vérifier l'absence de doublons complets
        duplicate_query = """
        SELECT COUNT(*) - COUNT(DISTINCT (city, state, location_source_value, address_1, address_2, county, zip))
        FROM omop.location;
        """
        
        with conn.cursor() as cur:
            cur.execute(duplicate_query)
            duplicates = cur.fetchone()[0]
            
            if duplicates > 0:
                return f"Duplicate rows test failed: Found {duplicates} complete duplicate rows in omop.location. Remove duplicate entries to ensure data integrity."
        
        return "All tests passed: Primary key is unique and no duplicate rows found in omop.location table."
        
    except psycopg2.Error as e:
        return f"Database connection error: {e}. Check database credentials and table existence."
        
    except Exception as e:
        return f"Unexpected error during testing: {e}. Verify table structure and permissions."
        
    finally:
        if 'conn' in locals():
            conn.close()




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



