package gateway

import (
	"fmt"
	"strings"
)

// Category groups commands for centralized gateway policy decisions.
type Category string

const (
	Read        Category = "READ"
	Write       Category = "WRITE"
	Transaction Category = "TRANSACTION"
	Blocking    Category = "BLOCKING"
	Auth        Category = "AUTH"
	Admin       Category = "ADMIN"
)

type commandRule struct {
	category Category
	allowed  bool
}

var commandRules = map[string]commandRule{
	"PING": {Read, true}, "ECHO": {Read, true}, "TYPE": {Read, true},
	"GET": {Read, true}, "INCR": {Write, true}, "SET": {Write, true},
	"LPUSH": {Write, true}, "RPUSH": {Write, true}, "LPOP": {Write, true},
	"LRANGE": {Read, true}, "LLEN": {Read, true}, "BLPOP": {Blocking, true},
	"ZADD": {Write, true}, "ZRANK": {Read, true}, "ZRANGE": {Read, true},
	"ZCARD": {Read, true}, "ZSCORE": {Read, true}, "ZREM": {Write, true},
	"XADD": {Write, true}, "XRANGE": {Read, true}, "XREAD": {Blocking, true},
	"MULTI": {Transaction, true}, "EXEC": {Transaction, true}, "DISCARD": {Transaction, true},
	// Redis credentials and ACL administration remain internal to the gateway.
	"AUTH": {Auth, false}, "ACL": {Admin, false},
}

// PolicyError is a stable public-policy failure, without internal details.
type PolicyError struct {
	Category Category
	Message  string
}

func (e *PolicyError) Error() string { return e.Message }

func Categorize(command string) (Category, bool) {
	rule, ok := commandRules[strings.ToUpper(command)]
	if !ok {
		return "", false
	}
	return rule.category, true
}

func CheckPolicy(args []string, hasSession bool) error {
	if len(args) == 0 {
		return &PolicyError{Message: "command must not be empty"}
	}
	rule, ok := commandRules[strings.ToUpper(args[0])]
	if !ok || !rule.allowed {
		return &PolicyError{Category: rule.category, Message: fmt.Sprintf("command %q is not available through the gateway", strings.ToUpper(args[0]))}
	}
	if rule.category == Transaction && !hasSession {
		return &PolicyError{Category: Transaction, Message: "transaction commands require X-Redis-Session"}
	}
	return nil
}

func SupportedCommands() []string {
	commands := make([]string, 0, len(commandRules))
	for command, rule := range commandRules {
		if rule.allowed {
			commands = append(commands, command)
		}
	}
	sortStrings(commands)
	return commands
}

func sortStrings(items []string) {
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j] < items[j-1]; j-- {
			items[j], items[j-1] = items[j-1], items[j]
		}
	}
}
