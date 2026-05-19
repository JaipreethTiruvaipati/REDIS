package auth

import (
	"crypto/sha256"
	"fmt"
)

// User represents a Redis ACL user with access control properties.
type User struct {
	Username  string
	NoPass    bool
	Passwords []string
	Flags     []string // e.g. "on", "allkeys", "allcommands"
}

// defaultUser is the built-in Redis user all connections start as.
var defaultUser = &User{
	Username:  "default",
	NoPass:    true,
	Passwords: []string{},
	Flags:     []string{"nopass"}, // no flags for now — later stages add "nopass" etc.
}

// DefaultUser returns the built-in default user.
func DefaultUser() *User {
	return defaultUser
}

// userRegistry holds all known users.
// In later stages, users can be added/modified via ACL SETUSER.
var userRegistry = map[string]*User{
	"default": defaultUser,
}

// GetUser looks up a user by username.
// Returns the user and true if found, nil and false otherwise.
func GetUser(username string) (*User, bool) {
	u, ok := userRegistry[username]
	return u, ok
}

// SetPassword adds a SHA-256 hashed password to the user
// and removes the nopass flag (as per Redis ACL behavior).
func (u *User) SetPassword(password string) {
	// Hash the password using SHA-256, stored as lowercase hex
	hash := sha256.Sum256([]byte(password))
	hashStr := fmt.Sprintf("%x", hash)
	u.Passwords = append(u.Passwords, hashStr)
	// Setting a real password removes the nopass flag
	u.NoPass = false
	filtered := []string{}
	for _, f := range u.Flags {
		if f != "nopass" {
			filtered = append(filtered, f)
		}
	}
	u.Flags = filtered
}
