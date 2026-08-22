// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package conformance

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aero-arc/aero-arc-conformance/internal/config"
	"google.golang.org/grpc/credentials"
)

func TestAssignmentServerCredentialsRequireTrustedClientCertificate(t *testing.T) {
	caCertificate, caKey := createTestCertificateAuthority(t)
	serverCertificate := createTestLeafCertificate(t, caCertificate, caKey, 2, "conformance", x509.ExtKeyUsageServerAuth)
	clientCertificate := createTestLeafCertificate(t, caCertificate, caKey, 3, "api", x509.ExtKeyUsageClientAuth)
	directory := t.TempDir()
	serverCertificateFile, serverKeyFile := writeTestCertificate(t, directory, "server", serverCertificate)
	clientCAFile := filepath.Join(directory, "client-ca.crt")
	if err := os.WriteFile(clientCAFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertificate.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	serverCredentials, err := newAssignmentServerCredentials(config.GRPCTLS{CertificateFile: serverCertificateFile, PrivateKeyFile: serverKeyFile, ClientCAFile: clientCAFile})
	if err != nil {
		t.Fatal(err)
	}
	rootCAs := x509.NewCertPool()
	rootCAs.AddCert(caCertificate)
	t.Run("trusted API certificate", func(t *testing.T) {
		if clientErr, serverErr := testTLSHandshake(serverCredentials, &tls.Config{Certificates: []tls.Certificate{clientCertificate}, RootCAs: rootCAs, ServerName: "conformance", MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}}); clientErr != nil || serverErr != nil {
			t.Fatalf("client error=%v server error=%v", clientErr, serverErr)
		}
	})
	t.Run("missing client certificate", func(t *testing.T) {
		_, serverErr := testTLSHandshake(serverCredentials, &tls.Config{RootCAs: rootCAs, ServerName: "conformance", MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}})
		if serverErr == nil {
			t.Fatal("server accepted an unauthenticated client")
		}
	})
}

func createTestCertificateAuthority(t *testing.T) (*x509.Certificate, ed25519.PrivateKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test API client CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	encoded, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return certificate, privateKey
}

func createTestLeafCertificate(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey, serial int64, name string, usage x509.ExtKeyUsage) tls.Certificate {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
	}
	encoded, err := x509.CreateCertificate(rand.Reader, template, ca, publicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{encoded}, PrivateKey: privateKey}
}

func writeTestCertificate(t *testing.T, directory, name string, certificate tls.Certificate) (string, string) {
	t.Helper()
	certificateFile := filepath.Join(directory, name+".crt")
	privateKeyFile := filepath.Join(directory, name+".key")
	if err := os.WriteFile(certificateFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	privateKey, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(privateKeyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKey}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certificateFile, privateKeyFile
}

func testTLSHandshake(serverCredentials credentials.TransportCredentials, clientConfig *tls.Config) (error, error) {
	serverConnection, clientConnection := net.Pipe()
	serverErrors := make(chan error, 1)
	go func() {
		_, _, err := serverCredentials.ServerHandshake(serverConnection)
		serverErrors <- err
	}()
	client := tls.Client(clientConnection, clientConfig)
	clientErr := client.Handshake()
	_ = clientConnection.Close()
	return clientErr, <-serverErrors
}
