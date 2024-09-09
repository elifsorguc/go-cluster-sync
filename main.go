package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Command-line flags for DB connection details
	sourceDBHost := flag.String("sourceHost", "", "IP address of the source database server")
	destDBHost := flag.String("destHost", "", "IP address of the destination database server")
	sourceDBName := flag.String("sourceDB", "", "Name of the source database")
	destDBName := flag.String("destDB", "", "Name of the destination database")
	sourceTableName := flag.String("sourceTable", "", "Name of the source table")
	destTableName := flag.String("destTable", "", "Name of the destination table")
	dbUser := flag.String("dbUser", "root", "Database user")
	dbPassword := flag.String("dbPassword", "password", "Database password")

	flag.Parse()

	// Source and Destination connection strings
	sourceDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s", *dbUser, *dbPassword, *sourceDBHost, *sourceDBName)
	destDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s", *dbUser, *dbPassword, *destDBHost, *destDBName)

	// Connect to source database
	srcDB, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		log.Fatalf("Error connecting to source database: %v", err)
	}
	defer srcDB.Close()

	// Connect to destination database
	dstDB, err := sql.Open("mysql", destDSN)
	if err != nil {
		log.Fatalf("Error connecting to destination database: %v", err)
	}
	defer dstDB.Close()

	// Check if the destination table exists, and create it if not
	err = createTableIfNotExists(srcDB, dstDB, *sourceTableName, *destTableName)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}

	// Perform data migration
	migrateData(srcDB, dstDB, *sourceTableName, *destTableName)
}

// createTableIfNotExists dynamically copies table schema from source to destination
func createTableIfNotExists(srcDB, destDB *sql.DB, sourceTableName, destTableName string) error {
	// Check if table exists in the destination
	var tableName string
	checkQuery := fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '%s'", destTableName)
	err := destDB.QueryRow(checkQuery).Scan(&tableName)

	if err == sql.ErrNoRows {
		// If the table doesn't exist, retrieve the source table's structure
		tableDef, err := getTableDefinition(srcDB, sourceTableName)
		if err != nil {
			return fmt.Errorf("failed to get table definition: %v", err)
		}

		// Create the table in the destination
		createTableSQL := fmt.Sprintf("CREATE TABLE %s (%s)", destTableName, tableDef)
		_, err = destDB.Exec(createTableSQL)
		if err != nil {
			return fmt.Errorf("failed to create table: %v", err)
		}
		fmt.Printf("Table '%s' created successfully\n", destTableName)
		return nil
	} else if err != nil {
		return fmt.Errorf("error checking table existence: %v", err)
	}

	// Table already exists
	fmt.Printf("Table '%s' already exists\n", destTableName)
	return nil
}


// getTableDefinition retrieves the table definition from the source DB using DESCRIBE
func getTableDefinition(db *sql.DB, tableName string) (string, error) {
	query := fmt.Sprintf("DESCRIBE %s", tableName)

	rows, err := db.Query(query)
	if err != nil {
		return "", fmt.Errorf("failed to query table definition: %v", err)
	}
	defer rows.Close()

	var columns []string
	var primaryKeyColumns []string

	for rows.Next() {
		var field, fieldType, null, key, extra string
		var defaultValue sql.NullString // This allows us to handle NULL default values

		err := rows.Scan(&field, &fieldType, &null, &key, &defaultValue, &extra)
		if err != nil {
			return "", fmt.Errorf("failed to scan table definition: %v", err)
		}

		// Handle created_at and updated_at columns separately
		if field == "created_at" || field == "updated_at" {
			// Handle timestamps specially to avoid MySQL syntax issues
			columnDef := fmt.Sprintf("`%s` %s", field, fieldType)
			if field == "created_at" {
				columnDef += " DEFAULT CURRENT_TIMESTAMP"
			} else if field == "updated_at" {
				columnDef += " DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
			}
			columns = append(columns, columnDef)
			continue
		}

		// Build column definition
		columnDef := fmt.Sprintf("`%s` %s", field, fieldType)

		// Handle nullability
		if null == "NO" {
			columnDef += " NOT NULL"
		} else {
			columnDef += " NULL"
		}

		// Handle default values if present and valid
		if defaultValue.Valid {
			columnDef += fmt.Sprintf(" DEFAULT '%s'", defaultValue.String)
		}

		// Handle extra information (e.g., auto_increment)
		if extra != "" {
			columnDef += " " + extra
		}

		// Check if this column is part of the primary key
		if key == "PRI" {
			primaryKeyColumns = append(primaryKeyColumns, fmt.Sprintf("`%s`", field))
		}

		columns = append(columns, columnDef)
	}

	// Join column definitions with commas
	tableDef := strings.Join(columns, ", ")

	// Add primary key definition if primary key columns exist
	if len(primaryKeyColumns) > 0 {
		primaryKeyDef := fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(primaryKeyColumns, ", "))
		tableDef += primaryKeyDef
	}

	return tableDef, nil
}



// migrateData copies data from source table to destination table
func migrateData(srcDB, dstDB *sql.DB, sourceTable, destTable string) {
    // Log the start of data migration
    fmt.Printf("Starting data migration from '%s' to '%s'\n", sourceTable, destTable)

    // Prepare data extraction from source table
    query := fmt.Sprintf("SELECT * FROM %s", sourceTable)
    rows, err := srcDB.Query(query)
    if err != nil {
        log.Fatalf("Error fetching data from source table: %v", err)
    }
    defer rows.Close()
    fmt.Println("Data fetched from source table successfully.")

    // Dynamically determine the number of columns
    cols, err := rows.Columns()
    if err != nil {
        log.Fatalf("Error fetching column information: %v", err)
    }
    fmt.Printf("Columns in source table: %v\n", cols)

    // Prepare insert statement for the destination table
    insertStmt := fmt.Sprintf("INSERT INTO %s VALUES (%s)", destTable, strings.Repeat("?,", len(cols)-1)+"?")
    fmt.Printf("Insert Statement: %s\n", insertStmt)
    stmt, err := dstDB.Prepare(insertStmt)
    if err != nil {
        log.Fatalf("Error preparing insert statement: %v", err)
    }
    defer stmt.Close()
    fmt.Println("Insert statement prepared successfully.")

    // Iterate over rows from the source table
    rowCount := 0
    for rows.Next() {
        // Dynamically create a slice of interfaces to hold the values
        values := make([]interface{}, len(cols))
        valuePointers := make([]interface{}, len(cols))
        for i := range values {
            valuePointers[i] = &values[i]
        }

        // Scan the row into the values slice
        err := rows.Scan(valuePointers...)
        if err != nil {
            log.Fatalf("Error scanning row: %v", err)
        }

        // Convert []byte to string where necessary
        for i, val := range values {
            if b, ok := val.([]byte); ok {
                values[i] = string(b) // Convert []byte to string
            }
        }

        // Print the row data for debugging purposes
        rowData := make([]string, len(cols))
        for i, col := range cols {
            rowData[i] = fmt.Sprintf("%s: %v", col, values[i])
        }
        fmt.Printf("Row %d: %v\n", rowCount+1, strings.Join(rowData, ", "))

        // Execute the insert statement
        _, err = stmt.Exec(values...)
        if err != nil {
            log.Printf("Error inserting row %d: %v\n", rowCount+1, err)
            continue
        }

        rowCount++
        fmt.Printf("Successfully inserted row %d\n", rowCount)
    }

    if err = rows.Err(); err != nil {
        log.Fatalf("Error iterating over rows: %v", err)
    }

    fmt.Printf("Data migration completed successfully. Total rows migrated: %d\n", rowCount)
}
