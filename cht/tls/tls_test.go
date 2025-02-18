/*

This optional test is for checking mutual TLS connectivity.  It is normally disabled but can be enabled
by setting the environment variable CH_GO_TLS_TESTS=True.  You should also add server1.clickhouse.test
as an alias to localhost in /etc/hosts, or otherwise ensure that server1.clickhouse.test points to
your test clickhouse server.


Configure your clickhouse server configuration using the certificates in this directory

<openSSL>
    <server>
        <certificateFile>clickhouse_test_server.crt</certificateFile>
        <privateKeyFile>clickhouse_test_server.key</privateKeyFile>
        <verificationMode>strict</verificationMode>
        <caConfig>clickhouse_test_ca.crt</caConfig>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3,tlsv1</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
</openSSL>

Sample xml user for clickhouse server configuration (within the <users> element in users.xml)
<cert_user>
    <ssl_certificates>
        <common_name>cert_user</common_name>
    </ssl_certificates>
    <profile>default</profile>
</cert_user>

*/

package tls_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
)

func TestMutualTLS(t *testing.T) {
	if run, _ := strconv.ParseBool(os.Getenv("CH_GO_TLS_TESTS")); !run {
		t.Skip("Not configured to run TLS tests")
	}

	_, b, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller ok false")
	}
	testDir := filepath.Join(filepath.Dir(b), ".")

	certTxt, err := os.ReadFile(testDir + "/clickhouse_test_client.crt")
	require.NoError(t, err)

	certKey, err := os.ReadFile(testDir + "/clickhouse_test_client.key")
	require.NoError(t, err)

	cert, err := tls.X509KeyPair(certTxt, certKey)
	require.NoError(t, err)

	rootCA, err := os.ReadFile(testDir + "/clickhouse_test_ca.crt")
	require.NoError(t, err)

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rootCA)

	tlsCfg := tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	opts := ch.Options{
		User:     "cert_user",
		Password: "",
		Address:  "server1.clickhouse.test:9440",
		TLS:      &tlsCfg,
	}

	conn, err := ch.Dial(context.Background(), opts)
	require.NoError(t, err)
	require.NoError(t, conn.Ping(context.Background()))

	_ = conn.Close()

	pool, err := chpool.Dial(context.Background(), chpool.Options{
		ClientOptions: opts,
		MaxConns:      2,
	})

	require.NoError(t, err)
	require.NoError(t, pool.Ping(context.Background()))

	pool.Close()
}
