// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use anyhow::anyhow;
use futures::stream::BoxStream;
use futures::{pin_mut, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::catalog::{Schema, OFFSET_COLUMN_NAME};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::DatumRef;
use serde_derive::{Deserialize, Serialize};
use tokio_postgres::types::PgLsn;
use tokio_postgres::NoTls;

use crate::error::ConnectorError;
use crate::parser::postgres_row_to_datums;
use crate::source::cdc::external::{
    CdcOffset, ConnectorResult, DebeziumOffset, ExternalTableConfig, ExternalTableReader,
    SchemaTableName,
};

#[derive(Debug, Clone, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct PostgresOffset {
    pub txid: i64,
    // Internally, an LSN is a 64-bit integer, representing a byte position in the write-ahead log stream.
    // It is printed as two hexadecimal numbers of up to 8 digits each, separated by a slash; for example, 16/B374D848
    pub lsn: u64,
}

impl PostgresOffset {
    pub fn parse_debezium_offset(offset: &str) -> ConnectorResult<Self> {
        let dbz_offset: DebeziumOffset = serde_json::from_str(offset).map_err(|e| {
            ConnectorError::Internal(anyhow!("invalid upstream offset: {}, error: {}", offset, e))
        })?;

        Ok(Self {
            txid: dbz_offset
                .source_offset
                .txid
                .ok_or_else(|| anyhow!("invalid postgres txid"))?,
            lsn: dbz_offset
                .source_offset
                .lsn
                .ok_or_else(|| anyhow!("invalid postgres lsn"))?,
        })
    }
}

#[derive(Debug)]
pub struct PostgresExternalTableReader {
    config: ExternalTableConfig,
    rw_schema: Schema,
    field_names: String,

    client: tokio::sync::Mutex<tokio_postgres::Client>,
}

impl ExternalTableReader for PostgresExternalTableReader {
    fn get_normalized_table_name(&self, table_name: &SchemaTableName) -> String {
        format!(
            "\"{}\".\"{}\"",
            table_name.schema_name, table_name.table_name
        )
    }

    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let mut client = self.client.lock().await;
        // start a transaction to read current lsn and txid
        let trxn = client.transaction().await?;
        let row = {
            let rs = trxn.query("SELECT pg_current_wal_lsn()", &[]).await?;
            rs.into_iter()
                .exactly_one()
                .map_err(|e| anyhow!("fail to get current lsn: {}", e))?
        };

        let mut pg_offset = PostgresOffset::default();
        let pg_lsn = row.get::<_, PgLsn>(0);
        tracing::debug!("current lsn: {}", pg_lsn);
        pg_offset.lsn = pg_lsn.into();

        let row = {
            let rs = trxn.query("SELECT txid_current()", &[]).await?;
            rs.into_iter()
                .exactly_one()
                .map_err(|e| anyhow!("fail to get current txid: {}", e))?
        };

        let txid: i64 = row.get::<_, i64>(0);
        pg_offset.txid = txid;

        // commit the transaction
        trxn.commit().await?;

        Ok(CdcOffset::Postgres(pg_offset))
    }

    fn parse_cdc_offset(&self, offset: &str) -> ConnectorResult<CdcOffset> {
        Ok(CdcOffset::Postgres(PostgresOffset::parse_debezium_offset(
            offset,
        )?))
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.snapshot_read_inner(table_name, start_pk, primary_keys)
    }
}

impl PostgresExternalTableReader {
    pub async fn new(
        properties: HashMap<String, String>,
        rw_schema: Schema,
    ) -> ConnectorResult<Self> {
        tracing::debug!(?rw_schema, "create postgres external table reader");

        let config = serde_json::from_value::<ExternalTableConfig>(
            serde_json::to_value(properties).unwrap(),
        )
        .map_err(|e| {
            ConnectorError::Config(anyhow!(
                "fail to extract postgres connector properties: {}",
                e
            ))
        })?;

        let database_url = format!(
            "postgresql://{}:{}@{}:{}/{}",
            config.username, config.password, config.host, config.port, config.database
        );

        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("connection error: {}", e);
            }
        });

        let field_names = rw_schema
            .fields
            .iter()
            .filter(|f| f.name != OFFSET_COLUMN_NAME)
            .map(|f| format!("\"{}\"", f.name.as_str()))
            .join(",");

        Ok(Self {
            config,
            rw_schema,
            field_names,
            client: tokio::sync::Mutex::new(client),
        })
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk_row: Option<OwnedRow>,
        primary_keys: Vec<String>,
    ) {
        let order_key = primary_keys.iter().join(",");
        let sql = if start_pk_row.is_none() {
            format!(
                "SELECT {} FROM {} ORDER BY {}",
                self.field_names,
                self.get_normalized_table_name(&table_name),
                order_key
            )
        } else {
            let filter_expr = Self::filter_expression(&primary_keys);
            format!(
                "SELECT {} FROM {} WHERE {} ORDER BY {}",
                self.field_names,
                self.get_normalized_table_name(&table_name),
                filter_expr,
                order_key
            )
        };

        let client = self.client.lock().await;
        client.execute("set time zone '+00:00'", &[]).await?;

        let params: Vec<DatumRef<'_>> = match start_pk_row {
            Some(ref pk_row) => pk_row.iter().collect_vec(),
            None => Vec::new(),
        };

        let stream = client.query_raw(&sql, &params).await?;
        let row_stream = stream.map(|row| {
            let row = row?;
            let datums = postgres_row_to_datums(row, &self.rw_schema)?;
            Ok::<_, ConnectorError>(OwnedRow::new(datums))
        });

        pin_mut!(row_stream);
        #[for_await]
        for row in row_stream {
            let row = row?;
            yield row;
        }
    }

    // row filter expression: (v1, v2, v3) > ($1, $2, $3)
    fn filter_expression(columns: &[String]) -> String {
        let mut col_expr = String::new();
        let mut arg_expr = String::new();
        for (i, column) in columns.iter().enumerate() {
            if i > 0 {
                col_expr.push_str(", ");
                arg_expr.push_str(", ");
            }
            col_expr.push_str(column);
            arg_expr.push_str(format!("${}", i + 1).as_str());
        }
        format!("({}) > ({})", col_expr, arg_expr)
    }
}

#[cfg(test)]
mod tests {
    use futures::pin_mut;
    use futures_async_stream::for_await;
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};

    use crate::source::cdc::external::postgres::PostgresExternalTableReader;
    use crate::source::cdc::external::{ExternalTableReader, SchemaTableName};

    #[test]
    fn test_mysql_binlog_offset() {
        let cols = vec!["v1".to_string(), "v2".to_string(), "v3".to_string()];
        let expr = PostgresExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(v1, v2, v3) > ($1, $2, $3)");
    }

    // manual test
    #[ignore]
    #[tokio::test]
    async fn test_pg_table_reader() {
        let columns = vec![
            ColumnDesc::named("v1", ColumnId::new(1), DataType::Int32),
            ColumnDesc::named("v2", ColumnId::new(2), DataType::Varchar),
            ColumnDesc::named("v3", ColumnId::new(3), DataType::Decimal),
            ColumnDesc::named("v4", ColumnId::new(4), DataType::Date),
        ];
        let rw_schema = Schema {
            fields: columns.iter().map(Field::from).collect(),
        };

        let props = convert_args!(hashmap!(
                "hostname" => "localhost",
                "port" => "8432",
                "username" => "myuser",
                "password" => "123456",
                "database.name" => "mydb",
                "schema.name" => "public",
                "table.name" => "t1"));
        let reader = PostgresExternalTableReader::new(props, rw_schema)
            .await
            .unwrap();

        let offset = reader.current_cdc_offset().await.unwrap();
        println!("CdcOffset: {:?}", offset);

        let start_pk = OwnedRow::new(vec![Some(ScalarImpl::from(3)), Some(ScalarImpl::from("c"))]);
        let stream = reader.snapshot_read(
            SchemaTableName {
                schema_name: "public".to_string(),
                table_name: "t1".to_string(),
            },
            Some(start_pk),
            vec!["v1".to_string(), "v2".to_string()],
        );

        pin_mut!(stream);
        #[for_await]
        for row in stream {
            println!("OwnedRow: {:?}", row);
        }
    }
}
