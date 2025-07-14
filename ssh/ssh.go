package ssh

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"strings"

	"golang.org/x/crypto/ssh"
)

// SSHKey represents an SSH private/public key pair for authentication.
type SSHKey struct {
	signer    ssh.Signer
	publicKey ssh.PublicKey
}

// LoadPrivateKeyFromFile loads an SSH private key from a file.
// Supports passphrase-protected keys for RSA, ECDSA, and Ed25519 key types.
func LoadPrivateKeyFromFile(filename, passphrase string) (*SSHKey, error) {
	if filename == "" {
		return nil, fmt.Errorf("filename cannot be empty")
	}

	if strings.Contains(filename, "..") {
		return nil, fmt.Errorf("filename contains invalid characters")
	}

	if _, err := os.Stat(filename); err != nil {
		return nil, fmt.Errorf("failed to access file: %w", err)
	}

	data, err := os.ReadFile(filename) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("failed to read SSH key file: %w", err)
	}

	var signer ssh.Signer
	if passphrase != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase(data, []byte(passphrase))
	} else {
		signer, err = ssh.ParsePrivateKey(data)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to parse SSH key: %w", err)
	}

	return &SSHKey{
		signer:    signer,
		publicKey: signer.PublicKey(),
	}, nil
}

// SignString signs the input string with the SSH private key and returns a base64-encoded signature.
func (k *SSHKey) SignString(input string) (string, error) {
	signature, err := k.signer.Sign(rand.Reader, []byte(input))
	if err != nil {
		return "", fmt.Errorf("failed to sign data: %w", err)
	}

	return base64.StdEncoding.EncodeToString(signature.Blob), nil
}

// GetPublicKeyBase64 returns the SSH public key in base64-encoded format.
func (k *SSHKey) GetPublicKeyBase64() (string, error) {
	keyBytes := ssh.MarshalAuthorizedKey(k.publicKey)
	if len(keyBytes) == 0 {
		return "", fmt.Errorf("failed to marshal public key")
	}

	// Remove trailing newline
	keyStr := strings.TrimSpace(string(keyBytes))
	return keyStr, nil
}

// GetKeyType returns the SSH key type (e.g., "ssh-ed25519", "ecdsa-sha2-nistp256", "ssh-rsa").
func (k *SSHKey) GetKeyType() string {
	return k.publicKey.Type()
}

// IsEmpty returns true if the SSH key is not properly initialized.
func (k *SSHKey) IsEmpty() bool {
	return k.signer == nil || k.publicKey == nil
}
