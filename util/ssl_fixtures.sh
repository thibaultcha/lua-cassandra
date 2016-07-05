#!/usr/bin/env bash

set -e
set -x

PASS="cassandra"

pushd spec/fixtures/ssl
  rm -f *

  ## server keystore
  # generate keystore
  keytool -genkey \
          -keyalg RSA \
          -validity 365 \
          -alias cassandra \
          -storepass $PASS \
          -keypass $PASS \
          -dname 'CN=Thibault Charbonnier, O=Mashape, L=San Francisco, ST=CA, C=US' \
          -keystore keystore.jks

  # export cert
  keytool -export \
          -alias cassandra \
          -file cassandra.crt \
          -keystore keystore.jks \
          -storepass $PASS \

  # convert to PEM
  openssl x509 \
          -inform der \
          -in cassandra.crt \
          -out cassandra.pem

  ## clients truststore
  # generate truststore (keys the node accepts when receiving messages) with pwd cassandra
  keytool -genkeypair \
          -keyalg RSA \
          -alias client \
          -validity 365 \
          -storepass $PASS \
          -keypass $PASS \
          -dname 'CN=Thibault Charbonnier, O=Mashape, L=San Francisco, ST=CA, C=US' \
          -keystore truststore.jks

  # export the private and public parts
  keytool -importkeystore \
          -srckeystore truststore.jks \
          -srcstorepass $PASS \
          -destkeystore client.p12 \
          -deststorepass $PASS \
          -deststoretype PKCS12

  # convert certificate to PEM
  openssl pkcs12 \
          -in client.p12 \
          -passin pass:$PASS \
          -nokeys \
          -out client_cert.pem

  # convert key to PEM
  openssl pkcs12 \
          -in client.p12 \
          -passin pass:$PASS \
          -nodes \
          -nocerts \
          -out client_key.pem
popd
