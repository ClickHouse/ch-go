package ssh

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSSHKeyLoading(t *testing.T) {
	t.Run("ValidKey", func(t *testing.T) {
		// Create a temporary RSA key for testing
		privateKey, err := generateTestRSAKey()
		require.NoError(t, err)

		tmpFile, err := os.CreateTemp("", "test_key*")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		err = os.WriteFile(tmpFile.Name(), privateKey, 0600)
		require.NoError(t, err)

		signer, err := LoadPrivateKeyFromFile(tmpFile.Name(), "")
		require.NoError(t, err)
		require.NotNil(t, signer)
	})

	t.Run("MissingFile", func(t *testing.T) {
		_, err := LoadPrivateKeyFromFile("/nonexistent", "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to access file")
	})

	t.Run("InvalidKey", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "invalid_key*")
		require.NoError(t, err)
		defer os.Remove(tmpFile.Name())

		err = os.WriteFile(tmpFile.Name(), []byte("not a key"), 0600)
		require.NoError(t, err)

		_, err = LoadPrivateKeyFromFile(tmpFile.Name(), "")
		require.Error(t, err)
		require.Contains(t, err.Error(), "no key found")
	})
}

func generateTestRSAKey() ([]byte, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	privateKeyPEM := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	}

	return pem.EncodeToMemory(privateKeyPEM), nil
}
