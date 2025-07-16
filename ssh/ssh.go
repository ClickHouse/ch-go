package ssh

import (
	"fmt"
	"os"

	"golang.org/x/crypto/ssh"
)

// LoadPrivateKeyFromFile loads an SSH private key from a file and returns a signer.
// Supports passphrase-protected keys for RSA, ECDSA, and Ed25519 key types.
func LoadPrivateKeyFromFile(filename, passphrase string) (ssh.Signer, error) {
	if filename == "" {
		return nil, fmt.Errorf("filename cannot be empty")
	}

	if _, err := os.Stat(filename); err != nil {
		return nil, fmt.Errorf("failed to access file: %w", err)
	}

	data, err := os.ReadFile(filename) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("failed to read SSH key file: %w", err)
	}

	if passphrase != "" {
		return ssh.ParsePrivateKeyWithPassphrase(data, []byte(passphrase))
	}
	return ssh.ParsePrivateKey(data)
}
