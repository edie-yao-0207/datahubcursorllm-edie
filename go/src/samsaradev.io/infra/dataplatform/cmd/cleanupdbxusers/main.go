package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects"
	"samsaradev.io/team/databricksusers"
)

/**
 * This script is used to clean up the databricksusers.go file in the databricksusers package.
 * To run this script you'll need to:
 * 1. Ask help-biztech for an export of all users with okta access to databricks.
 * 2. Save the file as "okta.csv" in the same directory as this script.
 * 3. Run `go run main.go` in the `cmd/cleanupdbxusers` directory.
 *    - This will rewrite the databricksusers.go file, removing anyone without okta access.
 *
 * After running it, please double check who got removed. A few things to watch out for:
 * - did any machine users accidentally get removed? e.g. safety-airflow user. these users don't have okta
 * - users without okta could also have their accounts made somewhat recently and not have gotten around
 *   yet to asking biztech for the okta tile. Double check deletions of recent users.
 */
func main() {
	hierarchy, err := dataplatformprojects.MakeGroupHierarchy()
	if err != nil {
		panic(err)
	}

	// Construct some helpful maps.
	allUsers := make(map[string]struct{})
	for _, user := range hierarchy.AllUsers {
		allUsers[user.Email] = struct{}{}
	}

	nonDev := make(map[string]struct{})
	for _, member := range databricksusers.DatabricksUserRegistry {
		nonDev[member.Email] = struct{}{}
	}

	// Figure out who should have access (based on the csv)
	oktaMap, err := readOktaCsv("okta.csv")
	if err != nil {
		panic(err)
	}

	var lines []string
	lines = append(lines, "package databricksusers")
	lines = append(lines, "")
	lines = append(lines, "import (")
	lines = append(lines, "\t\"samsaradev.io/team/teamnames\"")
	lines = append(lines, ")")
	lines = append(lines, "")
	lines = append(lines, "type DatabricksUser struct {")
	lines = append(lines, "\tName  string")
	lines = append(lines, "\tEmail string")
	lines = append(lines, "\tTeams []string")
	lines = append(lines, "}")
	lines = append(lines, "")
	lines = append(lines, "var DatabricksUserRegistry = []DatabricksUser{")

	fmt.Printf("Non-Rnd users we're removing from the databricks users list:\n")
	for _, user := range databricksusers.DatabricksUserRegistry {
		if _, ok := oktaMap[user.Email]; !ok {
			fmt.Printf("\t %s\n", user.Email)
		} else {
			lines = append(lines, "\t{")
			lines = append(lines, fmt.Sprintf("\t\tName:  \"%s\",", user.Name))
			lines = append(lines, fmt.Sprintf("\t\tEmail: \"%s\",", user.Email))
			lines = append(lines, "\t\tTeams: []string{")
			for _, team := range user.Teams {
				lines = append(lines, fmt.Sprintf("\t\t\tteamnames.%s,", team))
			}
			lines = append(lines, "\t\t},")
			lines = append(lines, "\t},")
		}
	}
	lines = append(lines, "}")
	lines = append(lines, "")

	// Write over the existing file.
	path := filepath.Join(filepathhelpers.BackendRoot, "go/src/samsaradev.io/team/databricksusers/databricksusers.go")
	total := strings.Join(lines, "\n")
	err = os.WriteFile(path, []byte(total), 0644)
	if err != nil {
		panic(err)
	}

	fmt.Printf("\n\nRnd users without okta access. Not a bad thing but just for information:\n")
	for _, user := range hierarchy.AllUsers {
		if _, ok := nonDev[user.Email]; !ok {
			if _, ok := oktaMap[user.Email]; !ok {
				fmt.Printf("\t %s\n", user.Email)
			}
		}
	}

	fmt.Printf("\n\nOkta users without databricks access. Not a problem, but still odd.\n")
	for email, user := range oktaMap {
		if _, ok := allUsers[email]; !ok {
			fmt.Printf("\t %s, %s\n", user.email, user.oktagroup)
		}
	}
}

type oktaUser struct {
	email     string
	oktagroup string
}

func readOktaCsv(filename string) (map[string]oktaUser, error) {
	// Open the CSV file
	file, err := os.Open(filename)
	if err != nil {
		return nil, oops.Wrapf(err, "error opening file")
	}
	defer file.Close()

	// Read all records from the CSV file
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, oops.Wrapf(err, "error reading csv")
	}

	users := make(map[string]oktaUser)

	// Iterate over the records (skipping the header)
	for i, record := range records {
		if i == 0 {
			// Skip the header row
			continue
		}

		users[record[0]] = oktaUser{
			email:     record[0],
			oktagroup: record[1],
		}
	}

	return users, nil
}
