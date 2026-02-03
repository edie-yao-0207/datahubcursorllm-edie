package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/helpers/ni/filepathhelpers"
	"samsaradev.io/infra/config"
)

/**
 * This script can help you put together the necessary commands to run commands directly on MySQL
 * dbs, and also run the mysqlbinlog utility. It can be a bit cumbersome to do so, so this script
 * can hopefully serve as a guide, and i'm committing it to the repo in case someone else finds
 * value in it.
 *
 * Note that you can only run `mysql` and `mysqlbinlog` commands from devices4, so you'll need permissions
 * to that.
 */
func main() {
	// Read the production config file, so that we can get access to all the database names.
	prodFile := filepathhelpers.BackendRoot + "/config/src/prod/config.json"
	contents, err := os.ReadFile(prodFile)
	if err != nil {
		log.Fatalf("failed to read %s, %v\n", prodFile, err)
	}

	var prodConfig *config.AppConfig
	if err = json.Unmarshal(contents, &prodConfig); err != nil {
		log.Fatalf("failed to unmarshal, %v\n", err)
	}

	fmt.Printf("WARNING: This script will print out database passwords on the console. Please type yes to confirm: ")
	var response string
	_, err = fmt.Scanln(&response)
	if err != nil {
		log.Fatal(err)
	}
	if response != "yes" {
		log.Fatalf("please confirm by typing yes")
	}

	//runCmdsForDatabase("workflows", prodConfig.WorkflowsDatabaseShardHosts)
	//runCmdsForDatabase("mdm", prodConfig.MdmDatabaseShardHosts)
	runCmdsForDatabase("trips", prodConfig.TripsDatabaseShardHosts)
}

// Iterate over all the shards and print out the password, mysql connection command, and then mysqlbinlog command.
func runCmdsForDatabase(dbName string, dbhosts map[string]string) {
	passwordKey := dbName + "_database_password"
	pwdcommand := exec.Command("go", "run", "infra/security/cmd/secretshelper/main.go", "-region", "us-west-2", "-task", "read", "-key", passwordKey)
	pwdcommand.Dir = filepath.Join(filepathhelpers.BackendRoot, "go/src/samsaradev.io")
	pwdcommand.Env = append(os.Environ(), "AWS_DEFAULT_PROFILE=readadmin")
	output, err := pwdcommand.CombinedOutput()
	if err != nil {
		log.Fatalln(oops.Wrapf(err, "failed to fetch pwd: %s", string(output)))
	}

	password := strings.TrimSpace(string(output))

	for _, host := range dbhosts {
		// Unfortunately, we cannot simply call mysql with the -p<password> flag as that is insecure;
		// so you'll need to copy in the password yourself.
		fmt.Printf("db password from appsecrets: %s\n", password)
		fmt.Printf(`mysql -h "%s" --ssl-ca=/etc/ssl/certs/ca-certificates.crt --ssl-mode=VERIFY_IDENTITY -P 3306 -u prod_admin -e "call mysql.rds_show_configuration; show binary logs;" --password`+"\n", host)
		fmt.Printf("binary log filename: ")
		var response string
		_, err = fmt.Scanln(&response)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf(`mysqlbinlog -R -h "%s" -u prod_admin %s -p | head`+"\n", host, response)
		_, _ = fmt.Scanln(&response)
	}

}
