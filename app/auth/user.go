package auth

import (
	"crypto/sha256"
	"fmt"
	"sync"
)

// User represents a Redis ACL user with access control properties.
type User struct {
	mu        sync.RWMutex
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
	u.mu.Lock()
	defer u.mu.Unlock()
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

// SetNoPass restores passwordless authentication and removes stored password
// hashes. It is primarily useful for explicit development configuration.
func (u *User) SetNoPass() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.NoPass = true
	u.Passwords = nil
	for _, flag := range u.Flags {
		if flag == "nopass" {
			return
		}
	}
	u.Flags = append(u.Flags, "nopass")
}

// Authenticate checks if the given password matches any stored password hash.
// If the user has nopass set, any password is accepted.
func (u *User) Authenticate(password string) bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if u.NoPass {
		return true
	}
	hash := sha256.Sum256([]byte(password))
	hashStr := fmt.Sprintf("%x", hash)
	for _, stored := range u.Passwords {
		if stored == hashStr {
			return true
		}
	}
	return false
}

// IsNoPass reports whether this user accepts any password.
func (u *User) IsNoPass() bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.NoPass
}

// Snapshot returns a race-free copy of user metadata for ACL responses.
func (u *User) Snapshot() (flags, passwords []string) {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return append([]string(nil), u.Flags...), append([]string(nil), u.Passwords...)
}
