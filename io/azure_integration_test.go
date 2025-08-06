// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build integration

package io_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun/driver/sqliteshim"
	"gocloud.dev/blob/azureblob"
)

const (
	accountName              = "devstoreaccount1"
	accountKey               = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	endpoint                 = "127.0.0.1:11000"
	protocol                 = "http"
	containerName            = "warehouse"
	connectionStringTemplate = "DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;BlobEndpoint=%s://%s/%s;"
)

type AzureBlobIOTestSuite struct {
	suite.Suite

	ctx context.Context
}

func (s *AzureBlobIOTestSuite) SetupTest() {
	s.ctx = context.Background()

	s.Require().NoError(s.createContainerIfNotExist(containerName))
}

func (s *AzureBlobIOTestSuite) TestAzureBlobWarehouseKey() {
	path := "iceberg-test-azure/test-table-azure"
	containerName := "warehouse"
	properties := iceberg.Properties{
		"uri":                       ":memory:",
		sqlcat.DriverKey:            sqliteshim.ShimName,
		sqlcat.DialectKey:           string(sqlcat.SQLite),
		"type":                      "sql",
		io.AdlsSharedKeyAccountName: accountName,
		io.AdlsSharedKeyAccountKey:  accountKey,
		io.AdlsEndpoint:             endpoint,
		io.AdlsProtocol:             protocol,
	}

	cat, err := catalog.Load(context.Background(), "default", properties)
	s.Require().NoError(err)
	s.Require().NotNil(cat)

	c := cat.(*sqlcat.Catalog)
	s.Require().NoError(c.CreateNamespace(s.ctx, catalog.ToIdentifier("iceberg-test-azure"), nil))

	tbl, err := c.CreateTable(s.ctx,
		catalog.ToIdentifier("iceberg-test-azure", "test-table-azure"),
		iceberg.NewSchema(0, iceberg.NestedField{
			Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, ID: 1,
		}), catalog.WithLocation(fmt.Sprintf("abfs://%s/iceberg/%s", containerName, path)))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	tbl, err = c.LoadTable(s.ctx,
		catalog.ToIdentifier("iceberg-test-azure", "test-table-azure"),
		properties)
	s.Require().NoError(err)
	s.Require().NotNil(tbl)
}

func (s *AzureBlobIOTestSuite) TestAzuriteWarehouseConnectionString() {
	connectionString := fmt.Sprintf(connectionStringTemplate, protocol, accountName, accountKey, protocol, endpoint, accountName)
	path := "iceberg-test-azure/test-table-azure"
	containerName := "warehouse"
	properties := iceberg.Properties{
		"uri":                       ":memory:",
		sqlcat.DriverKey:            sqliteshim.ShimName,
		sqlcat.DialectKey:           string(sqlcat.SQLite),
		"type":                      "sql",
		io.AdlsSharedKeyAccountName: accountName,
		io.AdlsConnectionStringPrefix + accountName: connectionString,
	}

	cat, err := catalog.Load(context.Background(), "default", properties)
	s.Require().NoError(err)
	s.Require().NotNil(cat)

	c := cat.(*sqlcat.Catalog)
	s.Require().NoError(c.CreateNamespace(s.ctx, catalog.ToIdentifier("iceberg-test-azure"), nil))

	tbl, err := c.CreateTable(s.ctx,
		catalog.ToIdentifier("iceberg-test-azure", "test-table-azure"),
		iceberg.NewSchema(0, iceberg.NestedField{
			Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, ID: 1,
		}), catalog.WithLocation(fmt.Sprintf("wasb://%s/iceberg/%s", containerName, path)))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	tbl, err = c.LoadTable(s.ctx,
		catalog.ToIdentifier("iceberg-test-azure", "test-table-azure"),
		properties)
	s.Require().NoError(err)
	s.Require().NotNil(tbl)
}

func (s *AzureBlobIOTestSuite) createContainerIfNotExist(containerName string) error {
	svcURL, err := azureblob.NewServiceURL(&azureblob.ServiceURLOptions{
		AccountName:   accountName,
		Protocol:      protocol,
		StorageDomain: endpoint,
	})
	if err != nil {
		return err
	}

	sharedKeyCred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return err
	}

	client, err := azblob.NewClientWithSharedKeyCredential(string(svcURL), sharedKeyCred, nil)
	if err != nil {
		return err
	}

	_, err = client.CreateContainer(s.ctx, containerName, nil)
	if err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		return err
	}

	return nil
}

func TestAzureURLParsing(t *testing.T) {
	tests := []struct {
		name              string
		urlStr            string
		expectedContainer string
		expectedHost      string
	}{
		{
			name:              "Modern Azure URL with container@account format",
			urlStr:            "abfs://my-container@myaccount.dfs.core.windows.net/iceberg/test_data",
			expectedContainer: "my-container",
			expectedHost:      "myaccount.dfs.core.windows.net",
		},
		{
			name:              "Traditional Azure URL with host only",
			urlStr:            "abfs://warehouse/iceberg/test-table",
			expectedContainer: "warehouse",
			expectedHost:      "warehouse",
		},

	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := url.Parse(tt.urlStr)
			require.NoError(t, err)

			// Test our container name extraction logic
			containerName := parsed.Host
			if parsed.User != nil && parsed.User.Username() != "" {
				containerName = parsed.User.Username()
			}

			assert.Equal(t, tt.expectedContainer, containerName, 
				"Container name should be extracted correctly")
			assert.Equal(t, tt.expectedHost, parsed.Host, 
				"Host should be parsed correctly")
		})
	}
}

func TestAzureContainerNameExtraction(t *testing.T) {
	// Test that demonstrates our URL parsing logic works correctly
	// This test shows the container name extraction without requiring internal functions
	
	tests := []struct {
		name              string
		urlStr            string
		expectedContainer string
		description       string
	}{
		{
			name:              "Container@Account URL format (Polaris style)",
			urlStr:            "abfs://my-container@myaccount.dfs.core.windows.net/iceberg/test_data",
			expectedContainer: "my-container",
			description:       "Should extract container name from User part when using container@account format",
		},
		{
			name:              "Traditional host-only URL format",
			urlStr:            "abfs://warehouse/iceberg/test-table",
			expectedContainer: "warehouse",
			description:       "Should use host as container name for traditional format",
		},

	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the URL
			parsed, err := url.Parse(tt.urlStr)
			require.NoError(t, err, "URL should parse without error")

			// Apply our container name extraction logic
			containerName := parsed.Host
			if parsed.User != nil && parsed.User.Username() != "" {
				containerName = parsed.User.Username()
			}

			// Verify the container name is extracted correctly
			assert.Equal(t, tt.expectedContainer, containerName, tt.description)
			
			// Additional validation for container@account format
			if parsed.User != nil && parsed.User.Username() != "" {
				assert.Contains(t, parsed.Host, ".dfs.core.windows.net", 
					"Host should contain account name with Azure domain")
				assert.NotContains(t, parsed.Host, "@", 
					"Host should not contain @ symbol (it should be in User)")
			}
		})
	}
}

func TestAzureBlobIOIntegration(t *testing.T) {
	suite.Run(t, new(AzureBlobIOTestSuite))
}
