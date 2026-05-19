package auth

// User represents a Redis ACL user with access control properties.
// Designed to be extended in later stages for passwords, flags, and permissions.
type User struct {
	Username  string
	NoPass    bool
	Passwords []string
}

// DefaultUser returns the built-in Redis default user.
// All new connections are automatically authenticated as this user.
// In later stages, this will be configurable.
func DefaultUser() *User {
	return &User{
		Username:  "default",
		NoPass:    true,
		Passwords: []string{},
	}
}
