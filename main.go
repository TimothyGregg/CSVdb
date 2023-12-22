package main

/*
Need to account for column name length (https://www.postgresql.org/docs/7.0/syntax525.htm)
*/

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

// For comparison of worst-case types
type pg_type int

const (
	pg_text pg_type = iota
	pg_timestamp
	pg_float
	pg_integer
)

const (
	SEPARATOR = ','
)

type docker_db_helper struct {
	DATABASE_CONTAINER_NAME string
	POSTGRES_USER           string
	POSTGRES_PASSWORD       string
	POSTGRES_DB             string
	PORT                    string
}

func (helper *docker_db_helper) Conn() (*sql.DB, error) {
	// We use localhost here because we're executing from the host, not within the docker network
	// fmt.Printf("postgresql://%s:%s@%s:%s/%s?sslmode=disable\n", helper.POSTGRES_USER, helper.POSTGRES_PASSWORD, "localhost", helper.PORT, helper.POSTGRES_DB)
	connStr := fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=disable", helper.POSTGRES_USER, helper.POSTGRES_PASSWORD, "localhost", helper.PORT, helper.POSTGRES_DB)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	return db, nil
}

type csv_file_for_pg struct {
	name       string
	headers    []string
	types      []pg_type
	rows       []map[string]string
	euro_dates bool
	skipped    int
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	var helper = docker_db_helper{
		DATABASE_CONTAINER_NAME: os.Getenv("DATABASE_CONTAINER_NAME"),
		POSTGRES_USER:           os.Getenv("POSTGRES_USER"),
		POSTGRES_PASSWORD:       os.Getenv("POSTGRES_PASSWORD"),
		POSTGRES_DB:             os.Getenv("POSTGRES_DB"),
		PORT:                    os.Getenv("LOCAL_DATABASE_PORT"),
	}

	// Create csvs dir if not exists
	bool, err := exists("./csvs")
	if err != nil {
		log.Fatal(err)
	}
	if !bool {
		if err := os.Mkdir("./csvs", os.ModePerm); err != nil {
			log.Fatal(err)
		}
		fmt.Println("Please place csvs in ./csvs directory for loading. Current separator is char [" + fmt.Sprint(int(SEPARATOR)) + "]")
		return
	}
	// Import the CSVs
	err = filepath.Walk("./csvs",
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if path != "./csvs" && info.IsDir() {
				return filepath.SkipDir
			}
			fmt.Println("PATH: " + path)
			_, _, bool := strings.Cut(path, ".csv")
			if !info.IsDir() && info.Name() != ".DS_Store" && bool {
				fmt.Println("./" + path)
				PG_Import(helper, "./"+path)
			}
			return nil
		})
	if err != nil {
		log.Println(err)
	}
}

func PG_Import(helper docker_db_helper, path string) {
	// DB Connection
	db, err := helper.Conn()
	if err != nil {
		log.Fatal(err)
	}

	thingy := make(chan string)
	ongoing := make(chan bool)
	go Gimme_Dat(path, thingy, ongoing)
	var n = 0
	for <-ongoing {
		qs := <-thingy
		_, err := db.Exec(qs)
		if err != nil {
			log.Fatal("Exec error: " + err.Error() + ` [` + qs + `]`)
		}
		n++
		if (n-2)%1000 == 0 && n != 2 {
			fmt.Println(fmt.Sprint(n-2) + " records added")
		}
	}
	fmt.Println(fmt.Sprint(n-2) + " total record(s) added")
	fmt.Println("~" + <-thingy + " record(s) skipped")
}

func Gimme_Dat(filename string, output chan string, signal chan bool) {
	content := get_csv_for_pg(filename)

	// Drop any existing table with the same name
	signal <- true
	output <- `DROP TABLE IF EXISTS "` + content.name + `";`

	// Create the table
	out := `CREATE TABLE IF NOT EXISTS "` + content.name + `"("` + content.name + `_uid" serial PRIMARY KEY, my_created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),`
	for n, header := range content.headers {
		out += header + ` ` + fmt.Sprint(content.types[n]) + `,`
	}
	out = out[:len(out)-1] + `);`
	signal <- true
	output <- out

	// Add the Data
	for _, row := range content.rows {
		front := `INSERT INTO "` + content.name + `" (`
		back := ` Values (`
		for n, header := range content.headers {
			if row[header] == "" {
				continue
			}
			front += header + ","
			if content.types[n] != pg_integer && content.types[n] != pg_float {
				row[header] = strings.ReplaceAll(row[header], `'`, "`")
				back += `'` + row[header] + `',`
			} else {
				back += row[header] + `,`
			}
		}
		signal <- true
		out := strings.ToValidUTF8(front[:len(front)-1]+`)`+back[:len(back)-1]+`);`, "~")
		// fmt.Println(out)
		output <- out
	}

	signal <- false
	output <- fmt.Sprint(content.skipped)
}

func (pgt pg_type) String() string {
	switch pgt {
	case pg_text:
		return "text"
	case pg_timestamp:
		return "timestamp"
	case pg_float:
		return "float8"
	case pg_integer:
		return "integer"
	}
	return "WHAT"
}

// Take a CSV file and convert it to the csv_file_for_pg type
func get_csv_for_pg(filename string) csv_file_for_pg {
	// Open the file to read
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("Error while reading the file", err)
	}
	defer file.Close()

	// Write a file with no fucking NULs (MAYBE)

	// Parse the file into a csv_file
	reader := csv.NewReader(file)
	reader.Comma = SEPARATOR
	reader.LazyQuotes = true
	var content csv_file_for_pg
	content.skipped = 0
	content.euro_dates = false // Assume american date string unless proven otherwise
	content.rows = make([]map[string]string, 0)

	// Get table name
	table_name_in_arr := strings.Split(filename, "/")
	content.name, _ = strings.CutSuffix(table_name_in_arr[len(table_name_in_arr)-1], ".csv")
	content.name = strings.ToLower((regexp.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(strings.TrimSpace(content.name), "_")))

	// Get headers
	var record []string
	next_row(reader, &record)
	for _, val := range record {
		if val != "" {
			val = strings.Replace(val, "\x00", "", -1)
			// fmt.Println(val)
			val = strings.ToLower((regexp.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(strings.TrimSpace(val), "_")))
			content.headers = append(content.headers, val)
		}
	}
	// fmt.Println(content.headers)

	// Set up the worst-case type array
	content.types = make([]pg_type, len(content.headers))
	for i := 0; i < len(content.headers); i++ {
		content.types[i] = pg_integer
	}
	regex_int, _ := regexp.Compile(`^[0-9]*$`)
	regex_float, _ := regexp.Compile(`^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$`)
	regex_timestamp, _ := regexp.Compile(`([0-9]+(/[0-9]+)+)`)

	// Create skips dir if not exists
	bool, err := exists("./skips")
	if err != nil {
		log.Fatal(err)
	}
	if !bool {
		if err := os.Mkdir("./skips", os.ModePerm); err != nil {
			log.Fatal(err)
		}
	}

	// Open file for errored lines
	skipped_file, err := os.OpenFile("./skips/"+fmt.Sprint(time.Now().UnixMicro())+"_skipped", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal("Error while opening the skipped_file", err)
	}
	defer skipped_file.Close()
	_, err = skipped_file.WriteString("\"" + strings.Join(content.headers, "\",\"") + "\"\n")
	if err != nil {
		log.Fatal(err)
	}

	// Get all records
	err = next_row(reader, &record)
	for i := 0; err != io.EOF; i++ {
		if err != nil {
			// Note: encodings/csv supports multi-line fields, so this can skip two lines if the reader was unable to fund a suitable line terminus outside of a field
			fmt.Println("Error in next_row: " + err.Error())
			_, err = skipped_file.WriteString("\"" + strings.Join(record, "\",\"") + "\"\n")
			if err != nil {
				log.Fatal(err)
			}
			content.skipped++
			err = next_row(reader, &record)
			i--
			continue
		}
		content.rows = append(content.rows, make(map[string]string))
		for n, header := range content.headers {
			// Add the value to the row
			content.rows[i][header] = record[n]

			// Check worst-case type
			// Integer only
			if regex_int.MatchString(record[n]) {
				if len(record[n]) < 9 {
					content.types[n] = min(content.types[n], pg_integer)
				} else {
					content.types[n] = pg_text
				}
				// Float only
			} else if regex_float.MatchString(record[n]) {
				content.types[n] = min(content.types[n], pg_float)
				// Timestamp only
			} else if regex_timestamp.MatchString(record[n]) {
				content.types[n] = min(content.types[n], pg_timestamp)
				// Check to see if there's proof we're using european timestamps
				i := strings.Index(record[n], "/")
				month_num, _ := strconv.Atoi(record[n][:i])
				if month_num > 12 {
					content.euro_dates = true
				}
				// Anything else is text
			} else {
				content.types[n] = pg_text
			}
		}
		err = next_row(reader, &record)
	}

	// Reorganize the date string to US format
	if content.euro_dates {
		for x, row := range content.rows {
			for n, header := range content.headers {
				// Modify dates
				if content.types[n] == pg_timestamp {
					date_str := row[header]
					if date_str == "" {
						content.rows[x][header] = ""
						continue
					}
					// First Separator
					i := strings.Index(date_str, "/")
					day := date_str[:i]
					// Second Separator
					i2 := strings.Index(date_str[i+1:], "/")
					month := date_str[i+1 : i+1+i2]
					// Combine
					content.rows[x][header] = month + "/" + day + date_str[i+1+i2:]
				}
			}
		}
	}
	return content
}

// Reach the next row of a CSV file
func next_row(reader *csv.Reader, target *[]string) error {
	var err error
	*target, err = reader.Read()
	return err
}

// Check folder existence
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
