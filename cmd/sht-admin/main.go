package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"sht/internal/admin"
)

var dbPath = getenv("SHT_DB_PATH", admin.DefaultDBPath)

func getenv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func usage() string {
	return strings.TrimSpace(`
usage:
  sht-admin user create <name>
  sht-admin user list
  sht-admin user disable <name>
  sht-admin service create <name>
  sht-admin key add <user> <name> <public-key-file>
  sht-admin key list [user]
  sht-admin key disable <key-id>
  sht-admin authorized-keys sync --output <path>
`)
}

func userUsage() string {
	return strings.TrimSpace(`
usage:
  sht-admin user create <name>
  sht-admin user list
  sht-admin user disable <name>
`)
}

func keyUsage() string {
	return strings.TrimSpace(`
usage:
  sht-admin key add <user> <name> <public-key-file>
  sht-admin key list [user]
  sht-admin key disable <key-id>
`)
}

func serviceUsage() string {
	return strings.TrimSpace(`
usage:
  sht-admin service create <name>
`)
}

func authorizedKeysUsage() string {
	return "usage: sht-admin authorized-keys sync --output <path>"
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func parseAuthorizedKeysSyncArgs(args []string) string {
	var output string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "--help", "help":
			fmt.Println(authorizedKeysUsage())
			os.Exit(0)
		case "--output":
			if i+1 >= len(args) {
				die("%s", authorizedKeysUsage())
			}
			output = args[i+1]
			i++
		default:
			die("unknown authorized-keys sync option: %s\n%s", args[i], authorizedKeysUsage())
		}
	}
	if output == "" {
		die("%s", authorizedKeysUsage())
	}
	return output
}

func userCommand(args []string) error {
	db, err := admin.OpenDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Println(userUsage())
		if len(args) == 0 {
			return fmt.Errorf("missing user command")
		}
		return nil
	}
	switch args[0] {
	case "create":
		if len(args) != 2 {
			return fmt.Errorf("usage: sht-admin user create <name>")
		}
		u, err := admin.CreateUser(db, args[1])
		if err != nil {
			return err
		}
		fmt.Printf("%d %s enabled\n", u.ID, u.Name)
		return nil
	case "list":
		if len(args) != 1 {
			return fmt.Errorf("usage: sht-admin user list")
		}
		users, err := admin.ListUsers(db)
		if err != nil {
			return err
		}
		for _, u := range users {
			state := "disabled"
			if u.Enabled {
				state = "enabled"
			}
			fmt.Printf("%d %s %s %s %s\n", u.ID, u.Name, state, u.Kind, u.IdentityProvider)
		}
		return nil
	case "disable":
		if len(args) != 2 {
			return fmt.Errorf("usage: sht-admin user disable <name>")
		}
		if err := admin.DisableUser(db, args[1]); err != nil {
			return err
		}
		fmt.Printf("%s disabled\n", args[1])
		return nil
	default:
		return fmt.Errorf("unknown user command: %s\n%s", args[0], userUsage())
	}
}

func serviceCommand(args []string) error {
	db, err := admin.OpenDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Println(serviceUsage())
		if len(args) == 0 {
			return fmt.Errorf("missing service command")
		}
		return nil
	}
	switch args[0] {
	case "create":
		if len(args) != 2 {
			return fmt.Errorf("usage: sht-admin service create <name>")
		}
		u, err := admin.CreateServiceUser(db, args[1])
		if err != nil {
			return err
		}
		fmt.Printf("%d %s enabled service system\n", u.ID, u.Name)
		return nil
	default:
		return fmt.Errorf("unknown service command: %s\n%s", args[0], serviceUsage())
	}
}

func keyCommand(args []string) error {
	db, err := admin.OpenDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Println(keyUsage())
		if len(args) == 0 {
			return fmt.Errorf("missing key command")
		}
		return nil
	}
	switch args[0] {
	case "add":
		if len(args) != 4 {
			return fmt.Errorf("usage: sht-admin key add <user> <name> <public-key-file>")
		}
		key, err := admin.AddKeyFromFile(db, args[1], args[2], args[3])
		if err != nil {
			return err
		}
		fmt.Printf("%d %s %s enabled\n", key.ID, key.UserName, key.Name)
		return nil
	case "list":
		if len(args) > 2 {
			return fmt.Errorf("usage: sht-admin key list [user]")
		}
		userName := ""
		if len(args) == 2 {
			userName = args[1]
		}
		keys, err := admin.ListKeys(db, userName)
		if err != nil {
			return err
		}
		for _, key := range keys {
			state := "disabled"
			if key.Enabled {
				state = "enabled"
			}
			fmt.Printf("%d %s %s %s %s\n", key.ID, key.UserName, key.Name, state, key.PublicKey)
		}
		return nil
	case "disable":
		if len(args) != 2 {
			return fmt.Errorf("usage: sht-admin key disable <key-id>")
		}
		keyID, err := admin.DisableKey(db, args[1])
		if err != nil {
			return err
		}
		fmt.Printf("%d disabled\n", keyID)
		return nil
	default:
		return fmt.Errorf("unknown key command: %s\n%s", args[0], keyUsage())
	}
}

func syncAuthorizedKeys(output string) error {
	db, err := admin.OpenDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	return admin.SyncAuthorizedKeys(db, output)
}

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		die("%s", usage())
	}
	if args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Println(usage())
		return
	}

	var err error
	switch {
	case len(args) >= 2 && args[0] == "authorized-keys" && args[1] == "sync":
		output := parseAuthorizedKeysSyncArgs(args[2:])
		err = syncAuthorizedKeys(output)
		if err != nil && errors.Is(err, os.ErrNotExist) {
			die("authorized-keys sync failed: %v", err)
		}
	case args[0] == "user":
		err = userCommand(args[1:])
	case args[0] == "key":
		err = keyCommand(args[1:])
	case args[0] == "service":
		err = serviceCommand(args[1:])
	case args[0] == "authorized-keys":
		err = fmt.Errorf("unknown authorized-keys command\n%s", authorizedKeysUsage())
	default:
		err = fmt.Errorf("%s", usage())
	}
	if err != nil {
		die("%v", err)
	}
}
